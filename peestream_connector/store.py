"""Local SQLite registry: dedupe ledger (``event_id``, 72 h TTL) plus an audit journal.

Two tables, both created with ``CREATE TABLE IF NOT EXISTS`` only - the connector
must be safe to start against an existing database:

* ``events``  - one row per processed ``event_id`` with the outcome of each action
                in ``result_json``. ``INSERT OR IGNORE`` of the event row *before*
                actions run is what makes processing atomic: a repeated delivery
                finds the row and is answered with ``200`` without side effects.
* ``journal`` - append-only trace of every request, signed or not, rejected or
                accepted, so an operator can answer "what arrived and what did we
                do with it" from the database alone.

The database is used from several worker threads of the HTTP server, therefore a
per-thread connection is opened lazily (``threading.local``) instead of sharing
one connection across threads.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, Optional, Set

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
    event_id      TEXT PRIMARY KEY,
    event         TEXT NOT NULL,
    occurred_at   TEXT,
    site_id       TEXT,
    kind          TEXT,
    canonical_key TEXT,
    received_at   TEXT NOT NULL,
    status        TEXT NOT NULL,
    result_json   TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_received ON events(received_at);
CREATE INDEX IF NOT EXISTS ix_events_canonical ON events(canonical_key);

CREATE TABLE IF NOT EXISTS journal(
    id          INTEGER PRIMARY KEY,
    at          TEXT NOT NULL,
    event_id    TEXT,
    event       TEXT,
    remote_addr TEXT,
    outcome     TEXT NOT NULL,
    http_status INTEGER,
    reason      TEXT,
    detail_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_journal_at ON journal(at);
"""

DEDUPE = "dedupe"
PROCESSED = "processed"
PARTIAL = "partial"
FAILED = "failed"


def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class EventStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        directory = os.path.dirname(os.path.abspath(db_path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._local = threading.local()
        # Re-entrant on purpose: ``journal``/``claim_event`` hold the lock and call
        # ``_conn()``, which registers the new connection under the same lock. A
        # plain ``Lock`` deadlocked every first DB write on a worker thread.
        self._lock = threading.RLock()
        # Every connection opened in a worker thread is tracked so ``close()`` can
        # release all of them. Server threads end on their own, and a connection
        # left to the garbage collector keeps the WAL file pinned.
        self._conns: Set[sqlite3.Connection] = set()
        connection = self._conn()
        connection.executescript(SCHEMA)
        connection.commit()

    def _conn(self) -> sqlite3.Connection:
        connection = getattr(self._local, "conn", None)
        if connection is None:
            connection = sqlite3.connect(self.db_path, timeout=30)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA busy_timeout=30000")
            connection.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = connection
            with self._lock:
                self._conns.add(connection)
        return connection

    def release_thread(self) -> None:
        """Close the calling thread's connection (called when a worker thread ends).

        HTTP worker threads are created per request by ``ThreadingHTTPServer``; the
        thread-local connection must be closed explicitly or it is only released
        when the interpreter reclaims the thread, which on a busy daemon leaves
        descriptors and WAL file handles open.
        """
        connection = getattr(self._local, "conn", None)
        if connection is None:
            return
        self._local.conn = None
        with self._lock:
            self._conns.discard(connection)
        try:
            connection.close()
        except sqlite3.Error:
            pass

    # ---------------------------------------------------------------- events

    def claim_event(self, event: Dict[str, Any], canonical_key: Optional[str]) -> bool:
        """Insert the event id. ``True`` = this delivery owns the event (first sight).

        The insert is ``INSERT OR IGNORE`` on the primary key, so concurrent
        duplicate deliveries cannot both win - SQLite serialises the insert and
        exactly one caller observes one row.
        """
        with self._lock:
            cursor = self._conn().execute(
                "INSERT OR IGNORE INTO events(event_id,event,occurred_at,site_id,kind,"
                "canonical_key,received_at,status,result_json) VALUES(?,?,?,?,?,?,?,?,NULL)",
                (event.get("event_id"), str(event.get("event") or ""),
                 event.get("occurred_at"), event.get("site_id"), event.get("kind"),
                 canonical_key, utc_now_iso(), PROCESSED),
            )
            self._conn().commit()
            return cursor.rowcount == 1

    def finish_event(self, event_id: str, status: str, result: Dict[str, Any]) -> None:
        with self._lock:
            self._conn().execute(
                "UPDATE events SET status=?, result_json=? WHERE event_id=?",
                (status, json.dumps(result, ensure_ascii=False), event_id),
            )
            self._conn().commit()

    def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn().execute(
            "SELECT event_id,event,status,received_at,result_json FROM events WHERE event_id=?",
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        return {"event_id": row[0], "event": row[1], "status": row[2],
                "received_at": row[3],
                "result": json.loads(row[4]) if row[4] else None}

    def purge_expired(self, ttl_hours: int) -> int:
        """Drop dedupe rows older than the TTL.

        The 72 h window is a floor required by the contract (backend retries last
        ~32.6 h); keeping rows longer is harmless, dropping them earlier is not.
        Cutoff is computed in UTC to match ``received_at``.
        """
        cutoff = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                               time.gmtime(time.time() - ttl_hours * 3600))
        with self._lock:
            cursor = self._conn().execute("DELETE FROM events WHERE received_at < ?", (cutoff,))
            self._conn().commit()
            return cursor.rowcount

    # ---------------------------------------------------------------- journal

    def journal(self, outcome: str, event_id: Optional[str] = None, event: Optional[str] = None,
                remote_addr: Optional[str] = None, http_status: Optional[int] = None,
                reason: Optional[str] = None, detail: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._conn().execute(
                "INSERT INTO journal(at,event_id,event,remote_addr,outcome,http_status,reason,"
                "detail_json) VALUES(?,?,?,?,?,?,?,?)",
                (utc_now_iso(), event_id, event, remote_addr, outcome, http_status, reason,
                 json.dumps(detail, ensure_ascii=False) if detail else None),
            )
            self._conn().commit()

    def counts(self) -> Dict[str, int]:
        events = self._conn().execute("SELECT COUNT(*) FROM events").fetchone()[0]
        journal = self._conn().execute("SELECT COUNT(*) FROM journal").fetchone()[0]
        return {"events": events, "journal": journal}

    def close(self) -> None:
        with self._lock:
            connections = list(self._conns)
            self._conns.clear()
        for connection in connections:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        self._local.conn = None
