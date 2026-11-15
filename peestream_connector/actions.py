"""Execution mechanisms for a verified event: shell hook, webhook forward, JSON sink.

Every action returns a small result dict ``{"action": ..., "ok": bool, "info": ...}``
and never raises for expected failures - the HTTP layer decides the status code
from the collected results. Secrets are never copied into results or logs.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import threading
import urllib.error
import urllib.request
from contextlib import closing
from typing import Any, Dict, List, Optional

from . import signature
from .config import ConnectorConfig


def canonical_key(event: Dict[str, Any]) -> Optional[str]:
    """Content identity used by the JSON sink and its SQLite index.

    Movie:    ``movie:tmdb:<id>`` (or ``movie:imdb:<id>`` when TMDb is missing)
    Episode:  ``episode:tmdb:<series>:s<S>e<E>`` - season 0 is kept as 0.
    Tests:    ``test:<site_id>``; other events fall back to their event id.
    """
    event_name = str(event.get("event") or "")
    if event_name == "site.test":
        return "test:%s" % (event.get("site_id") or "unknown")
    kind = str(event.get("kind") or "")
    tmdb_id = event.get("tmdb_id")
    imdb_id = event.get("imdb_id")
    if kind == "movie":
        if tmdb_id not in (None, ""):
            return "movie:tmdb:%s" % tmdb_id
        if imdb_id:
            return "movie:imdb:%s" % imdb_id
    if kind == "episode":
        season = event.get("season")
        episode = event.get("episode")
        if tmdb_id not in (None, "") and season is not None and episode is not None:
            return "episode:tmdb:%s:s%se%s" % (tmdb_id, int(season), int(episode))
    return "event:%s" % (event.get("event_id") or "unknown")


def run_shell(config: ConnectorConfig, raw_body: bytes, event: Dict[str, Any]) -> Dict[str, Any]:
    shell = config.shell
    if not shell.enabled:
        return {"action": "shell", "ok": True, "info": "disabled"}
    argument = raw_body.decode("utf-8", errors="replace")
    argv = list(shell.command)
    payload = None
    if shell.mode == "argument":
        argv.append(argument)
    else:
        payload = raw_body
    env = os.environ.copy()
    env.update(shell.pass_env)
    env.update({
        "PEESTREAM_EVENT": str(event.get("event") or ""),
        "PEESTREAM_EVENT_ID": str(event.get("event_id") or ""),
        "PEESTREAM_SITE_ID": str(event.get("site_id") or ""),
    })
    try:
        completed = subprocess.run(
            argv, input=payload, env=env, capture_output=True,
            timeout=shell.timeout_seconds, check=False,
        )
    except subprocess.TimeoutExpired:
        return {"action": "shell", "ok": False,
                "info": "timeout after %ss" % shell.timeout_seconds}
    except OSError as exc:
        return {"action": "shell", "ok": False, "info": "spawn failed: %s" % exc}
    if completed.returncode != 0:
        return {"action": "shell", "ok": False,
                "info": "exit %d: %s" % (completed.returncode,
                                         _tail(completed.stderr))}
    return {"action": "shell", "ok": True, "info": "exit 0",
            "stdout": _tail(completed.stdout)}


def run_webhook(config: ConnectorConfig, raw_body: bytes,
                event: Dict[str, Any]) -> Dict[str, Any]:
    hook = config.webhook
    if not hook.enabled:
        return {"action": "webhook", "ok": True, "info": "disabled"}
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "PeeStream-Connector/1.0",
        "X-PeeStream-Event-Id": str(event.get("event_id") or ""),
    }
    if hook.resign_internal:
        headers["X-PeeStream-Signature"] = signature.compute(hook.internal_secret, raw_body)
        headers["X-PeeStream-Resigned"] = "connector"
    headers.update(hook.extra_headers)
    request = urllib.request.Request(hook.url, data=raw_body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=hook.timeout_seconds) as response:
            status = response.getcode()
    except urllib.error.HTTPError as exc:
        return {"action": "webhook", "ok": False, "info": "HTTP %s" % exc.code}
    except (urllib.error.URLError, OSError) as exc:
        return {"action": "webhook", "ok": False, "info": "transport error: %s" % exc}
    ok = 200 <= status < 300
    return {"action": "webhook", "ok": ok, "info": "HTTP %s" % status}


_sink_lock = threading.Lock()


def run_json_sink(config: ConnectorConfig, raw_body: bytes, event: Dict[str, Any],
                  key: Optional[str]) -> Dict[str, Any]:
    sink = config.json_sink
    if not sink.enabled:
        return {"action": "json_sink", "ok": True, "info": "disabled"}
    directory = os.path.abspath(sink.directory)
    try:
        os.makedirs(directory, exist_ok=True)
        record = {"event": event, "received_at": _now(), "canonical_key": key,
                  "raw_body_sha256": _raw_sha256(raw_body)}
        path = os.path.join(directory, _safe_filename(key or "unknown") + ".json")
        payload = json.dumps(record, ensure_ascii=False, indent=2).encode("utf-8")
        _atomic_write(path, payload)
        if sink.index_sqlite:
            _index_sqlite(os.path.abspath(sink.sqlite_path), key, event, payload)
    except OSError as exc:
        return {"action": "json_sink", "ok": False, "info": "write failed: %s" % exc}
    return {"action": "json_sink", "ok": True, "info": "written"}


def run_all(config: ConnectorConfig, raw_body: bytes, event: Dict[str, Any],
            key: Optional[str]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    if config.shell.enabled:
        results.append(run_shell(config, raw_body, event))
    if config.webhook.enabled:
        results.append(run_webhook(config, raw_body, event))
    if config.json_sink.enabled:
        results.append(run_json_sink(config, raw_body, event, key))
    return results


# ---------------------------------------------------------------- helpers

def _safe_filename(key: str) -> str:
    kept = [c if (c.isalnum() or c in "-._") else "_" for c in key]
    return "".join(kept)[:150] or "unknown"


def _atomic_write(path: str, payload: bytes) -> None:
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _index_sqlite(path: str, key: Optional[str], event: Dict[str, Any],
                  payload: bytes) -> None:
    import sqlite3
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with _sink_lock:
        # ``closing`` guarantees the handle is released even when an index write
        # raises; a leaked connection keeps the WAL file pinned.
        with closing(sqlite3.connect(path, timeout=30)) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS items("
                "canonical_key TEXT PRIMARY KEY, kind TEXT, tmdb_id INTEGER, imdb_id TEXT, "
                "season INTEGER, episode INTEGER, title TEXT, event_id TEXT, "
                "updated_at TEXT, body_json TEXT)"
            )
            connection.execute(
                "INSERT OR REPLACE INTO items(canonical_key,kind,tmdb_id,imdb_id,season,"
                "episode,title,event_id,updated_at,body_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (key, event.get("kind"), _int_or_none(event.get("tmdb_id")),
                 event.get("imdb_id"), _int_or_none(event.get("season")),
                 _int_or_none(event.get("episode")), event.get("title"),
                 event.get("event_id"), _now(), payload.decode("utf-8")),
            )
            connection.commit()


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _raw_sha256(raw_body: bytes) -> str:
    import hashlib
    return hashlib.sha256(raw_body).hexdigest()


def _tail(data: Optional[bytes], limit: int = 300) -> str:
    if not data:
        return ""
    text = data.decode("utf-8", errors="replace").strip()
    return text[-limit:]


def _now() -> str:
    import time
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def describe_shell_command(command: List[str]) -> str:
    """Human readable form for logs - never contains a secret by construction."""
    return " ".join(shlex.quote(part) for part in command)
