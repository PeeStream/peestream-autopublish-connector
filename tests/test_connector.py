#!/usr/bin/env python3
"""Mock test suite for the PeeStream AutoPublish connector.

Run: ``python3 -m unittest discover -s tests -v`` (stdlib only, no third-party deps).

Every test generates its own webhook secret and signs the raw body with the same
HMAC-SHA256 algorithm the backend uses, so no real credential ever appears here.
"""

import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peestream_connector import signature  # noqa: E402
from peestream_connector.actions import canonical_key  # noqa: E402
from peestream_connector.config import ConfigError, ConnectorConfig  # noqa: E402
from peestream_connector.receiver import Connector, is_expired, make_server, parse_occurred_at  # noqa: E402
from peestream_connector.store import EventStore  # noqa: E402

SECRET = "test_secret_" + "a" * 40  # synthetic, generated per process


def sample_event(event_id=None, event="media.ready", **overrides):
    body = {
        "version": 1,
        "event_id": event_id or "evt_test_%d" % time.time_ns(),
        "event": event,
        "occurred_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "site_id": "site_test001",
        "kind": "movie",
        "tmdb_id": 550,
        "imdb_id": "tt0137523",
        "season": None,
        "episode": None,
        "title": "Test Movie",
        "folder_path": "Filmovi/2026",
        "file_code": "AbC123xYz789",
        "player_url": "https://peeplayer.com/e/AbC123xYz789",
        "watch_url": "https://peeplayer.com/v/AbC123xYz789",
        "download_url": None,
        "poster_url": "https://example.invalid/p.jpg",
        "duration_s": 5400,
    }
    body.update(overrides)
    return body


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class SignatureTests(unittest.TestCase):
    def test_valid_signature_accepted(self):
        raw = b'{"event_id":"evt_1"}'
        header = signature.compute(SECRET, raw)
        self.assertTrue(header.startswith("sha256="))
        self.assertTrue(signature.verify(SECRET, raw, header))

    def test_wrong_secret_rejected(self):
        raw = b'{"event_id":"evt_1"}'
        header = signature.compute("other_" + "b" * 40, raw)
        self.assertFalse(signature.verify(SECRET, raw, header))

    def test_tampered_body_rejected(self):
        raw = b'{"event_id":"evt_1","title":"A"}'
        header = signature.compute(SECRET, raw)
        self.assertFalse(signature.verify(SECRET, raw.replace(b'"A"', b'"B"'), header))

    def test_malformed_headers_rejected_without_exception(self):
        raw = b"{}"
        good = signature.compute(SECRET, raw)
        for bad in (None, "", "sha256=", "sha1=" + good[7:], "sha256=zz" * 8,
                    good.upper(), "sha256=" + good[7:-1]):
            self.assertFalse(signature.verify(SECRET, raw, bad), bad)

    def test_uppercase_hex_accepted(self):
        raw = b'{"x":1}'
        header = signature.compute(SECRET, raw)
        self.assertTrue(signature.verify(SECRET, raw, header.upper().replace("SHA256=", "sha256=")))

    def test_multibyte_body_signed_as_bytes(self):
        raw = '{"title":"Šđčćž – film"}'.encode("utf-8")
        self.assertTrue(signature.verify(SECRET, raw, signature.compute(SECRET, raw)))


class CanonicalKeyTests(unittest.TestCase):
    def test_movie_by_tmdb(self):
        self.assertEqual(canonical_key(sample_event()), "movie:tmdb:550")

    def test_movie_by_imdb_fallback(self):
        self.assertEqual(canonical_key(sample_event(tmdb_id=None)), "movie:imdb:tt0137523")

    def test_episode_season_zero_preserved(self):
        event = sample_event(kind="episode", season=0, episode=3)
        self.assertEqual(canonical_key(event), "episode:tmdb:550:s0e3")

    def test_episode_without_coordinates_falls_back(self):
        event = sample_event(kind="episode", season=None, episode=None)
        self.assertTrue(canonical_key(event).startswith("event:"))

    def test_test_event_key(self):
        self.assertEqual(canonical_key(sample_event(event="site.test")), "test:site_test001")


class ParsingTests(unittest.TestCase):
    def test_parse_z_and_offset(self):
        self.assertEqual(parse_occurred_at("2026-11-13T20:00:00Z"),
                         datetime(2026, 11, 13, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(parse_occurred_at("2026-11-13T20:00:00+00:00"),
                         datetime(2026, 11, 13, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(parse_occurred_at("2026-11-13T22:00:00+02:00"),
                         datetime(2026, 11, 13, 20, 0, tzinfo=timezone.utc))

    def test_parse_invalid(self):
        for bad in (None, "", "yesterday", 1234567890):
            self.assertIsNone(parse_occurred_at(bad))

    def test_expiry_window(self):
        now = datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc)
        self.assertTrue(is_expired(now - timedelta(hours=73), 72, now))
        self.assertFalse(is_expired(now - timedelta(hours=71), 72, now))
        self.assertFalse(is_expired(None, 72, now))


class ConfigTests(unittest.TestCase):
    base = {
        "site_id": "site_test001",
        "server": {"host": "127.0.0.1", "port": 8855, "webhook_secret": SECRET},
        "actions": {"json_sink": {"enabled": True, "directory": "data/items"}},
    }

    def test_minimal_config_ok(self):
        config = ConnectorConfig.from_dict(self.base, env={})
        self.assertEqual(config.port, 8855)
        self.assertEqual(config.signature.ttl_hours, 72)
        self.assertTrue(config.json_sink.enabled)

    def test_env_secret_wins_over_file(self):
        env_secret = "env_" + "c" * 40
        config = ConnectorConfig.from_dict(self.base, env={"PEESTREAM_WEBHOOK_SECRET": env_secret})
        self.assertEqual(config.signature.secret, env_secret)

    def test_bad_site_id_rejected(self):
        bad = json.loads(json.dumps(self.base))
        bad["site_id"] = "../../etc"
        with self.assertRaises(ConfigError):
            ConnectorConfig.from_dict(bad, env={})

    def test_short_secret_rejected(self):
        bad = json.loads(json.dumps(self.base))
        bad["server"]["webhook_secret"] = "short"
        with self.assertRaises(ConfigError):
            ConnectorConfig.from_dict(bad, env={})

    def test_no_action_rejected(self):
        bad = json.loads(json.dumps(self.base))
        bad["actions"] = {"json_sink": {"enabled": False}}
        with self.assertRaises(ConfigError):
            ConnectorConfig.from_dict(bad, env={})

    def test_shell_command_required(self):
        bad = json.loads(json.dumps(self.base))
        bad["actions"]["shell"] = {"enabled": True, "command": []}
        with self.assertRaises(ConfigError):
            ConnectorConfig.from_dict(bad, env={})

    def test_sanitized_has_no_secret(self):
        config = ConnectorConfig.from_dict(self.base, env={})
        dumped = json.dumps(config.sanitized())
        self.assertNotIn(SECRET, dumped)
        self.assertNotIn("webhook_secret", dumped)

    def test_invalid_port(self):
        bad = json.loads(json.dumps(self.base))
        bad["server"]["port"] = 70000
        with self.assertRaises(ConfigError):
            ConnectorConfig.from_dict(bad, env={})

    def test_shipped_placeholder_secret_rejected(self):
        bad = json.loads(json.dumps(self.base))
        bad["server"]["webhook_secret"] = "REPLACE_ME_WITH_THE_SECRET_FROM_SITES_REGISTER_XX"
        with self.assertRaises(ConfigError):
            ConnectorConfig.from_dict(bad, env={})

    def test_example_config_does_not_start(self):
        example = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                               "config.json.example")
        with open(example, encoding="utf-8") as handle:
            data = json.load(handle)
        with self.assertRaises(ConfigError):
            ConnectorConfig.from_dict(data, env={})


class ConnectorProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="psconn-")
        self.sink_dir = os.path.join(self.tmp, "items")
        self.config = ConnectorConfig.from_dict({
            "site_id": "site_test001",
            "server": {"host": "127.0.0.1", "port": 1, "webhook_secret": SECRET},
            "actions": {"json_sink": {"enabled": True, "directory": self.sink_dir,
                                      "sqlite_path": os.path.join(self.tmp, "catalog.sqlite3")}},
        }, env={})
        self.store = EventStore(os.path.join(self.tmp, "connector.sqlite3"))
        self.connector = Connector(self.config, self.store)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _send(self, event, secret=SECRET, sign=True):
        raw = json.dumps(event, ensure_ascii=False).encode("utf-8")
        header = signature.compute(secret, raw) if sign else None
        return self.connector.process(raw, header, "127.0.0.1")

    def test_valid_event_processed_and_sunk(self):
        event = sample_event()
        status, payload = self._send(event)
        self.assertEqual(status, 200)
        self.assertEqual(payload["outcome"], "processed")
        self.assertTrue(os.path.exists(os.path.join(self.sink_dir, "movie_tmdb_550.json")))

    def test_bad_signature_is_401_and_no_side_effect(self):
        status, _ = self._send(sample_event(), secret="x" * 40)
        self.assertEqual(status, 401)
        self.assertFalse(os.path.exists(self.sink_dir))

    def test_twenty_repeats_single_side_effect(self):
        event = sample_event(event_id="evt_repeat")
        statuses = [self._send(event)[0] for _ in range(20)]
        self.assertEqual(statuses, [200] * 20)
        stored = self.store.get_event("evt_repeat")
        self.assertIsNotNone(stored)
        self.assertEqual(stored["status"], "processed")
        files = os.listdir(self.sink_dir)
        self.assertEqual(files, ["movie_tmdb_550.json"])
        self.assertEqual(self.store.counts()["events"], 1)

    def test_unknown_event_acknowledged_without_processing(self):
        status, payload = self._send(sample_event(event="media.updated"))
        self.assertEqual(status, 200)
        self.assertEqual(payload["ignored"], "unknown_event")
        self.assertEqual(self.store.counts()["events"], 0)

    def test_expired_event_acknowledged_without_processing(self):
        old = (datetime.now(timezone.utc) - timedelta(hours=80)).strftime("%Y-%m-%dT%H:%M:%SZ")
        status, payload = self._send(sample_event(occurred_at=old))
        self.assertEqual(status, 200)
        self.assertEqual(payload["ignored"], "expired")
        self.assertEqual(self.store.counts()["events"], 0)

    def test_duplicate_still_deduped_after_ttl_purge(self):
        event = sample_event(event_id="evt_ttl")
        self.assertEqual(self._send(event)[0], 200)
        # Rows are kept far longer than the contract window; purge must not remove
        # a freshly processed event, and a repeat must still be a no-op.
        self.assertEqual(self.store.purge_expired(72), 0)
        status, payload = self._send(event)
        self.assertEqual(status, 200)
        self.assertTrue(payload["duplicate"])
        self.assertEqual(self.store.get_event("evt_ttl")["status"], "processed")

    def test_invalid_json_and_missing_id(self):
        raw = b"not json"
        self.assertEqual(self.connector.process(raw, signature.compute(SECRET, raw))[0], 400)
        status, _ = self._send({"event": "media.ready"})
        self.assertEqual(status, 400)

    def test_json_array_payload_rejected(self):
        raw = b"[1,2,3]"
        self.assertEqual(self.connector.process(raw, signature.compute(SECRET, raw))[0], 400)

    def test_episode_season_zero_written(self):
        event = sample_event(kind="episode", season=0, episode=7, event_id="evt_s0")
        self.assertEqual(self._send(event)[0], 200)
        self.assertTrue(os.path.exists(os.path.join(self.sink_dir, "episode_tmdb_550_s0e7.json")))

    def test_site_test_event_processed(self):
        status, payload = self._send(sample_event(event="site.test"))
        self.assertEqual(status, 200)
        self.assertEqual(payload["outcome"], "processed")


class ShellActionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="psconn-shell-")
        self.out = os.path.join(self.tmp, "hook.jsonl")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, mode):
        script = os.path.join(self.tmp, "hook.py")
        with open(script, "w", encoding="utf-8") as handle:
            handle.write(
                "import sys, json\n"
                "data = sys.stdin.read() if %r == 'stdin' else sys.argv[1]\n"
                "with open(%r, 'a') as f: f.write(json.dumps({'argc': len(data), 'event': json.loads(data)['event_id']}) + '\\n')\n"
                % (mode, self.out)
            )
        config = ConnectorConfig.from_dict({
            "site_id": "site_test001",
            "server": {"host": "127.0.0.1", "port": 1, "webhook_secret": SECRET},
            "actions": {"shell": {"enabled": True, "mode": mode,
                                  "command": [sys.executable, script]}},
        }, env={})
        store = EventStore(os.path.join(self.tmp, "c.sqlite3"))
        try:
            event = sample_event(event_id="evt_shell_%s" % mode)
            raw = json.dumps(event).encode()
            status, payload = Connector(config, store).process(raw, signature.compute(SECRET, raw))
            self.assertEqual(status, 200)
            self.assertEqual(payload["outcome"], "processed")
            with open(self.out, encoding="utf-8") as handle:
                lines = [json.loads(line) for line in handle if line.strip()]
            self.assertEqual(len(lines), 1)
            self.assertGreater(lines[0]["argc"], 0)
            self.assertEqual(lines[0]["event"], event["event_id"])
        finally:
            store.close()

    def test_shell_stdin_mode(self):
        self._run("stdin")

    def test_shell_argument_mode(self):
        self._run("argument")

    def test_failing_hook_yields_500_and_event_marked_failed(self):
        config = ConnectorConfig.from_dict({
            "site_id": "site_test001",
            "server": {"host": "127.0.0.1", "port": 1, "webhook_secret": SECRET},
            "actions": {"shell": {"enabled": True,
                                  "command": [sys.executable, "-c", "raise SystemExit(3)"]}},
        }, env={})
        store = EventStore(os.path.join(self.tmp, "c.sqlite3"))
        try:
            raw = json.dumps(sample_event(event_id="evt_fail")).encode()
            status, _ = Connector(config, store).process(raw, signature.compute(SECRET, raw))
            self.assertEqual(status, 500)
            self.assertEqual(store.get_event("evt_fail")["status"], "failed")
        finally:
            store.close()


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="psconn-http-")
        self.config = ConnectorConfig.from_dict({
            "site_id": "site_test001",
            "server": {"host": "127.0.0.1", "port": 1, "webhook_secret": SECRET,
                       "max_body_bytes": 4096},
            "actions": {"json_sink": {"enabled": True,
                                      "directory": os.path.join(self.tmp, "items"),
                                      # Without this the sink index defaulted to
                                      # ``data/catalog.sqlite3`` relative to the CWD
                                      # and tests polluted the source tree.
                                      "sqlite_path": os.path.join(self.tmp, "catalog.sqlite3")}},
        }, env={})
        self.store = EventStore(os.path.join(self.tmp, "c.sqlite3"))
        self.server = make_server(Connector(self.config, self.store), "127.0.0.1", free_port())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _post(self, body, header=None):
        request = urllib.request.Request(self.base + "/webhook", data=body, method="POST",
                                         headers={"Content-Type": "application/json"})
        if header:
            request.add_header("X-PeeStream-Signature", header)
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.getcode(), json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"{}")

    def test_health(self):
        with urllib.request.urlopen(self.base + "/healthz", timeout=10) as response:
            payload = json.loads(response.read())
        self.assertEqual(response.getcode(), 200)
        self.assertEqual(payload["site_id"], "site_test001")
        self.assertNotIn("secret", json.dumps(payload))

    def test_signed_post_over_http(self):
        raw = json.dumps(sample_event(event_id="evt_http")).encode()
        status, payload = self._post(raw, signature.compute(SECRET, raw))
        self.assertEqual(status, 200)
        self.assertEqual(payload["outcome"], "processed")

    def test_unsigned_post_is_401(self):
        status, payload = self._post(json.dumps(sample_event()).encode())
        self.assertEqual(status, 401)

    def test_oversized_body_is_413(self):
        status, _ = self._post(b"x" * 5000)
        self.assertEqual(status, 413)

    def test_unknown_path_404(self):
        request = urllib.request.Request(self.base + "/nope", data=b"{}", method="POST")
        try:
            urllib.request.urlopen(request, timeout=10)
            self.fail("expected 404")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
