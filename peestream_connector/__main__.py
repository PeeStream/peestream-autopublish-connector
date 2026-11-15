"""Entry point: ``python -m peestream_connector serve|check|simulate``.

``serve``    run the webhook receiver (foreground; systemd runs it directly)
``check``    load the configuration and report what would be enabled, then exit
``simulate`` POST a signed sample event to a running instance (local smoke test)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Dict, List, Optional

from . import signature
from .config import ConfigError, ConnectorConfig
from .receiver import Connector, make_server
from .store import EventStore


def _setup_logging(level: str, log_path: Optional[str]) -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_path:
        directory = os.path.dirname(os.path.abspath(log_path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )


def _cmd_check(config: ConnectorConfig) -> int:
    print(json.dumps(config.sanitized(), indent=2, ensure_ascii=False))
    store = EventStore(config.db_path)
    print(json.dumps({"store": config.db_path, "counts": store.counts()}, indent=2))
    store.close()
    return 0


def _cmd_serve(config: ConnectorConfig) -> int:
    store = EventStore(config.db_path)
    connector = Connector(config, store)
    server = make_server(connector)
    log = logging.getLogger("peestream_connector")
    log.info("listening on %s:%d site_id=%s", config.host, config.port, config.site_id)

    stop = threading.Event()

    def _stop(signum: int, _frame: Any) -> None:
        log.info("signal %s - shutting down", signum)
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    last_purge = 0.0
    try:
        while not stop.is_set():
            # ``serve_forever`` blocks without a way to run the hourly purge, so the
            # main thread polls with a short socket timeout instead.
            server.timeout = 1.0
            server.handle_request()
            now = time.time()
            if now - last_purge > 3600:
                removed = store.purge_expired(config.signature.ttl_hours)
                if removed:
                    log.info("dedupe purge removed %d rows", removed)
                last_purge = now
    finally:
        server.server_close()
        store.close()
        log.info("stopped")
    return 0


def _cmd_simulate(config: ConnectorConfig, event_name: str, url: str) -> int:
    event: Dict[str, Any] = {
        "version": 1,
        "event_id": "evt_sim_" + uuid.uuid4().hex[:16],
        "event": event_name,
        "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "site_id": config.site_id,
        "kind": "movie",
        "tmdb_id": 550,
        "imdb_id": "tt0137523",
        "season": None,
        "episode": None,
        "title": "Connector Smoke Test",
        "folder_path": "Smoke/Tests",
        "file_code": "SmokeTest001",
        "player_url": "https://peeplayer.com/e/SmokeTest001",
        "watch_url": "https://peeplayer.com/v/SmokeTest001",
        "download_url": None,
        "poster_url": "https://example.invalid/poster.jpg",
        "duration_s": 60,
    }
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-PeeStream-Signature": signature.compute(config.signature.secret, body),
            "X-PeeStream-Event-Id": event["event_id"],
            "X-PeeStream-Timestamp": str(int(time.time())),
            "User-Agent": "PeeStream-Connector-Simulator/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            print("HTTP %s %s" % (response.getcode(), response.read().decode("utf-8")))
            return 0 if 200 <= response.getcode() < 300 else 1
    except urllib.error.HTTPError as exc:
        print("HTTP %s %s" % (exc.code, exc.read().decode("utf-8", errors="replace")))
        return 1
    except urllib.error.URLError as exc:
        print("transport error: %s" % exc)
        return 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="peestream-connector", description=__doc__)
    parser.add_argument("--config", default=os.environ.get("PEESTREAM_CONNECTOR_CONFIG",
                                                           "config.json"))
    parser.add_argument("--log-level", default=os.environ.get("PEESTREAM_LOG_LEVEL", "INFO"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="run the webhook receiver")
    sub.add_parser("check", help="validate configuration and print a safe summary")
    simulate = sub.add_parser("simulate", help="send a signed sample event")
    simulate.add_argument("--event", default="site.test", choices=("site.test", "media.ready"))
    simulate.add_argument("--url", default=None)

    args = parser.parse_args(argv)
    try:
        config = ConnectorConfig.load(args.config)
    except ConfigError as exc:
        sys.stderr.write("configuration error: %s\n" % exc)
        return 2

    _setup_logging(args.log_level, config.log_path)
    if args.command == "check":
        return _cmd_check(config)
    if args.command == "serve":
        return _cmd_serve(config)
    url = args.url or "http://%s:%d/" % (
        "127.0.0.1" if config.host in ("0.0.0.0", "::") else config.host, config.port)
    return _cmd_simulate(config, args.event, url)


if __name__ == "__main__":
    sys.exit(main())
