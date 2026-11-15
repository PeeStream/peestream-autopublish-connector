"""HTTP receiver: signature check, dedupe, dispatch to actions.

Order of operations for every POST (contract v1.1, section 4):

1. read ``Content-Length`` bounded by ``max_body_bytes`` -> ``413`` if exceeded
2. verify ``X-PeeStream-Signature`` over the RAW body -> ``401`` if invalid
3. parse JSON -> ``400`` if unparsable, not an object, or missing ``event_id``
4. ``event_id`` already seen or ``occurred_at`` older than TTL -> ``200`` ignored
5. unknown ``event`` value -> ``200`` ignored (forward compatibility)
6. claim the event, run actions, persist outcome

``GET /healthz`` returns 200 for liveness checks and never touches secrets.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional, Tuple

from . import actions, signature
from .config import ConnectorConfig
from .store import DEDUPE, FAILED, PARTIAL, PROCESSED, EventStore

log = logging.getLogger("peestream_connector")

KNOWN_EVENTS = ("media.ready", "site.test")


def parse_occurred_at(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_expired(occurred_at: Optional[datetime], ttl_hours: int,
               now: Optional[datetime] = None) -> bool:
    if occurred_at is None:
        return False
    now = now or datetime.now(timezone.utc)
    return occurred_at < now - timedelta(hours=ttl_hours)


class Connector:
    """Everything the HTTP layer needs, independent of the socket."""

    def __init__(self, config: ConnectorConfig, store: EventStore):
        self.config = config
        self.store = store

    def process(self, raw_body: bytes, signature_header: Optional[str],
                remote_addr: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
        store = self.store
        if not signature.verify(self.config.signature.secret, raw_body, signature_header):
            store.journal("rejected", remote_addr=remote_addr, http_status=401,
                          reason="invalid_signature",
                          detail={"body_bytes": len(raw_body)})
            return 401, {"status": "error", "error": "invalid signature"}

        try:
            event = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            store.journal("rejected", remote_addr=remote_addr, http_status=400,
                          reason="invalid_json")
            return 400, {"status": "error", "error": "invalid json"}
        if not isinstance(event, dict):
            store.journal("rejected", remote_addr=remote_addr, http_status=400,
                          reason="payload_not_object")
            return 400, {"status": "error", "error": "payload must be a JSON object"}

        event_id = str(event.get("event_id") or "").strip()
        event_name = str(event.get("event") or "").strip()
        if not event_id:
            store.journal("rejected", remote_addr=remote_addr, http_status=400,
                          reason="missing_event_id", event=event_name)
            return 400, {"status": "error", "error": "missing event_id"}

        # Dedupe wins over the age window: a repeat of an event already handled is
        # always answered 200 without work, even if it has since aged out.
        existing = store.get_event(event_id)
        if existing is not None:
            store.journal("dedupe", event_id=event_id, event=event_name,
                          remote_addr=remote_addr, http_status=200,
                          reason="already_seen")
            return 200, {"status": "ok", "event_id": event_id, "duplicate": True}

        occurred = parse_occurred_at(event.get("occurred_at"))
        if is_expired(occurred, self.config.signature.ttl_hours):
            store.journal("expired", event_id=event_id, event=event_name,
                          remote_addr=remote_addr, http_status=200,
                          reason="occurred_at older than %sh" % self.config.signature.ttl_hours)
            return 200, {"status": "ok", "event_id": event_id, "ignored": "expired"}

        if event_name not in KNOWN_EVENTS:
            store.journal("unknown_event", event_id=event_id, event=event_name,
                          remote_addr=remote_addr, http_status=200,
                          reason="unknown event ignored")
            return 200, {"status": "ok", "event_id": event_id, "ignored": "unknown_event"}

        key = actions.canonical_key(event)
        if not store.claim_event(event, key):
            store.journal(DEDUPE, event_id=event_id, event=event_name,
                          remote_addr=remote_addr, http_status=200,
                          reason="claim_lost_race")
            return 200, {"status": "ok", "event_id": event_id, "duplicate": True}

        try:
            results = actions.run_all(self.config, raw_body, event, key)
        except Exception as exc:  # action module is defensive, but never lose the event
            log.exception("action dispatch crashed for %s", event_id)
            store.finish_event(event_id, FAILED, {"error": str(exc)[:200]})
            store.journal(FAILED, event_id=event_id, event=event_name,
                          remote_addr=remote_addr, http_status=500,
                          reason="dispatch crashed: %s" % str(exc)[:160])
            return 500, {"status": "error", "event_id": event_id, "error": "dispatch failed"}

        failed = [r for r in results if not r.get("ok")]
        if not failed:
            status = PROCESSED
            http_status = 200
        elif len(failed) == len(results):
            status = FAILED
            http_status = 500
        else:
            status = PARTIAL
            http_status = 500

        store.finish_event(event_id, status, {"canonical_key": key, "results": results})
        store.journal(status, event_id=event_id, event=event_name, remote_addr=remote_addr,
                      http_status=http_status, reason=_status_reason(status),
                      detail={"canonical_key": key, "results": results})
        return http_status, {"status": "ok" if http_status == 200 else "error",
                             "event_id": event_id, "outcome": status,
                             "actions": [{"action": r["action"], "ok": r["ok"]}
                                         for r in results]}

    def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "site_id": self.config.site_id,
            "dedupe_ttl_hours": self.config.signature.ttl_hours,
            "counts": self.store.counts(),
        }


class ConnectorServer(ThreadingHTTPServer):
    """``ThreadingHTTPServer`` that releases the worker thread's SQLite handle.

    The default ``process_request_thread`` drops the thread without a chance to
    close thread-local resources; overriding it keeps the connection lifetime
    tied to the request lifetime.
    """

    def __init__(self, address, handler, store):
        super().__init__(address, handler)
        self.store = store

    def process_request_thread(self, request, client_address):  # type: ignore[override]
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.store.release_thread()


def _status_reason(status: str) -> str:
    return {"processed": "all actions ok", "partial": "some actions failed",
            "failed": "all actions failed"}.get(status, status)


def make_handler(connector: Connector) -> Callable[..., BaseHTTPRequestHandler]:
    max_body = connector.config.max_body_bytes
    header_name = connector.config.signature.header
    store = connector.store

    class Handler(BaseHTTPRequestHandler):
        server_version = "PeeStreamConnector/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:  # keep stdlib noise out
            log.debug("%s - %s", self.address_string(), fmt % args)

        # ------------------------------------------------------------ routes
        def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
            if self.path.split("?")[0] in ("/healthz", "/health", "/"):
                self._json(200, connector.health())
                return
            self._json(404, {"status": "error", "error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path.split("?")[0] not in ("/", "/webhook", "/peestream/webhook"):
                self._json(404, {"status": "error", "error": "not found"})
                return
            start = time.time()
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                store.journal("rejected", remote_addr=self.client_address[0],
                              http_status=400, reason="bad_content_length")
                self._json(400, {"status": "error", "error": "bad content-length"})
                return
            if length <= 0:
                store.journal("rejected", remote_addr=self.client_address[0],
                              http_status=400, reason="empty_body")
                self._json(400, {"status": "error", "error": "empty body"})
                return
            if length > max_body:
                store.journal("rejected", remote_addr=self.client_address[0],
                              http_status=413, reason="body_too_large",
                              detail={"content_length": length, "limit": max_body})
                self._json(413, {"status": "error", "error": "body too large"})
                return
            raw_body = self.rfile.read(length)
            status, payload = connector.process(
                raw_body, self.headers.get(header_name), self.client_address[0])
            payload["elapsed_ms"] = int((time.time() - start) * 1000)
            self._json(status, payload)

        # ------------------------------------------------------------ helpers
        def _json(self, status: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

    return Handler


def make_server(connector: Connector, host: Optional[str] = None,
                port: Optional[int] = None) -> ConnectorServer:
    host = connector.config.host if host is None else host
    port = connector.config.port if port is None else port
    server = ConnectorServer((host, port), make_handler(connector), connector.store)
    server.daemon_threads = True
    return server
