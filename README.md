# PeeStream AutoPublish Connector (standalone)

Universal AutoPublish receiver for sites that are **not** WordPress or DLE:
Joomla, KVS, custom CMS platforms, and headless/JAMstack front ends.

The connector is a small Python 3 daemon (standard library only) that receives
webhooks from PeeStream, verifies the HMAC-SHA256 signature, removes duplicate
deliveries, and hands each event to one or more actions that you configure:
a shell hook, a local webhook forward, and/or a JSON/SQLite sink.

It follows `AUTOPUBLISH_API_V2_CONTRACT.md` (v1.1).

---

## 1. Requirements

- Python **3.8+** (tested on 3.13). No third-party packages.
- Inbound network path from PeeStream to your receiver. PeeStream requires a
  public **HTTPS** `webhook_url` for site registration, so in practice you put
  the connector behind nginx/Caddy/a tunnel that terminates TLS. The connector
  itself speaks plain HTTP and should listen on `127.0.0.1` unless it is behind
  a reverse proxy on the same host.

## 2. Install

```bash
useradd --system --home /opt/peestream-connector --shell /usr/sbin/nologin peestream
install -d -o peestream -g peestream /opt/peestream-connector /var/lib/peestream-connector
cp -r peestream_connector /opt/peestream-connector/
install -d -m 750 /etc/peestream-connector
cp config.json.example /etc/peestream-connector/config.json
chmod 640 /etc/peestream-connector/config.json   # root:peestream
install -m 0644 packaging/peestream-connector.service /etc/systemd/system/
systemctl daemon-reload
```

Before starting, register the site with PeeStream and put the returned secret in
the config (see section 3).

```bash
curl -X POST https://peestream.com/api/v2/sites/register \
  -H "Authorization: Bearer $PEESTREAM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"canonical_url":"https://example.com","webhook_url":"https://example.com/peestream/webhook","platform":"other","adapter":"universal"}'
```

The response contains `site_id`, `webhook_secret` and `webhook_version`. The
secret is shown **only** at registration and on `rotate-secret`; store it in the
config or in an environment file, never in a shell history or in a URL.

## 3. Configuration

`config.json` (path from `--config` or `$PEESTREAM_CONNECTOR_CONFIG`):

```json
{
  "site_id": "site_8f3k2m1q",
  "server": {
    "host": "127.0.0.1",
    "port": 8855,
    "signature_header": "X-PeeStream-Signature",
    "dedupe_ttl_hours": 72,
    "max_body_bytes": 1048576,
    "webhook_secret": "the value from POST /api/v2/sites/register"
  },
  "db_path": "/var/lib/peestream-connector/connector.sqlite3",
  "log_path": "/var/lib/peestream-connector/connector.log",
  "actions": {
    "shell": {
      "enabled": false,
      "command": ["/usr/local/bin/my-cms-import"],
      "mode": "stdin",
      "timeout_seconds": 120,
      "pass_env": {"CMS_API_URL": "http://127.0.0.1:8080/api/import"}
    },
    "webhook": {
      "enabled": false,
      "url": "http://127.0.0.1:8090/internal/publish",
      "timeout_seconds": 15,
      "extra_headers": {"X-Internal-Token": "change-me"},
      "resign_internal": false,
      "internal_secret": ""
    },
    "json_sink": {
      "enabled": true,
      "directory": "/var/lib/peestream-connector/items",
      "index_sqlite": true,
      "sqlite_path": "/var/lib/peestream-connector/catalog.sqlite3"
    }
  }
}
```

### Secret handling

Resolution order for the webhook secret:

1. `PEESTREAM_WEBHOOK_SECRET` (environment variable)
2. `PEESTREAM_WEBHOOK_SECRET_FILE` (path to a file, e.g. a systemd `LoadCredential`)
3. `server.webhook_secret` in the config file

Placeholder values such as `REPLACE_ME...` are rejected at startup. The secret is
never written to the journal, the log, or the health endpoint; `check` prints a
sanitized view only.

### Actions

At least one action must be enabled. All enabled actions run for every accepted
event; the response is `200` only if all of them succeeded.

| Action | Purpose |
|---|---|
| `shell` | Run your own importer. `mode: "stdin"` writes the raw JSON body to the process stdin; `mode: "argument"` passes it as the last argv element. `PEESTREAM_EVENT`, `PEESTREAM_EVENT_ID` and `PEESTREAM_SITE_ID` are added to the environment. Reached via `subprocess.run` with a list argv (no shell expansion). |
| `webhook` | POST the **same raw body** to an internal endpoint. `X-PeeStream-Signature` is forwarded verbatim so your API can verify it with the same secret, unless `resign_internal` is on - then it is re-signed with `internal_secret` and marked `X-PeeStream-Resigned: connector`. |
| `json_sink` | Write one JSON file per content identity (`data/items/<key>.json`, atomic replace) and, optionally, upsert a row into a local SQLite catalog. |

**Canonical keys** used by the sink and its index:

| Event | Key |
|---|---|
| movie | `movie:tmdb:<id>`, or `movie:imdb:<id>` when TMDb is missing |
| episode | `episode:tmdb:<series>:s<season>e<episode>` (season `0` = specials, kept as `0`) |
| `site.test` | `test:<site_id>` |

## 4. Run

```bash
python3 -m peestream_connector check      # validate config, print sanitized summary
python3 -m peestream_connector serve      # run the receiver (systemd does this)
python3 -m peestream_connector simulate --event site.test   # signed local smoke test
systemctl enable --now peestream-connector
curl -s http://127.0.0.1:8855/healthz
```

Then press **Test** for the site in PeeStream (`POST /api/v2/sites/{id}/test`).
That sends a real signed `site.test` event and expects a `2xx`.

### Reverse proxy (nginx example)

```nginx
location = /peestream/webhook {
    client_max_body_size 1m;
    proxy_pass http://127.0.0.1:8855/webhook;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header Host $host;
}
```

Do not rewrite or buffer the body in a way that changes bytes: the signature is
computed over the exact bytes PeeStream sent.

## 5. Protocol behaviour

| Situation | HTTP response | Side effects |
|---|---|---|
| Valid signature, known `event`, first delivery | `200` | actions run, event recorded |
| Invalid / missing / malformed signature | `401` | none (logged) |
| Body not JSON, not an object, or no `event_id` | `400` | none (logged) |
| `event_id` already seen | `200` `{"duplicate": true}` | none |
| `occurred_at` older than `dedupe_ttl_hours` (default 72 h) | `200` `{"ignored": "expired"}` | none |
| Unknown `event` value (e.g. `media.updated`) | `200` `{"ignored": "unknown_event"}` | none |
| Body larger than `max_body_bytes` | `413` | none (logged) |
| At least one action failed | `500` | outcome stored as `partial`/`failed` |

The event row is inserted **before** actions run (`INSERT OR IGNORE` on the
primary key), so two concurrent duplicate deliveries cannot both execute. A
repeated delivery of an event that was already handled is always answered `200`
without work, even after it has aged past the TTL.

`dedupe_ttl_hours` must stay at or above 72: backend retries for `media.ready`
span roughly 32.6 hours, and rejecting a retry would drop the content.

### Local state

`db_path` (default `data/connector.sqlite3`):

- `events` - dedupe ledger, one row per `event_id` with its outcome and per-action result.
- `journal` - append-only log of every request: rejected signatures, dedupe hits,
  expired events, processed events. Use this to answer "what arrived and what did we do".

Inspect it:

```bash
sqlite3 /var/lib/peestream-connector/connector.sqlite3 \
  "SELECT received_at, event, status FROM events ORDER BY received_at DESC LIMIT 20;"
sqlite3 /var/lib/peestream-connector/connector.sqlite3 \
  "SELECT outcome, http_status, COUNT(*) FROM journal GROUP BY 1,2;"
```

## 6. Tests

```bash
python3 -m unittest discover -s tests -v
```

The suite is dependency-free and uses only synthetic secrets generated in the
test process. It covers: signature validity (wrong secret, tampered body,
malformed headers, uppercase hex, multi-byte UTF-8), canonical identity, season
`0`, TTL parsing/expiry, config validation and placeholder rejection, 20x
duplicate delivery, unknown/expired events, shell stdin and argument modes,
failing hook -> `500`, the JSON/SQLite sink, and live HTTP behaviour
(signed/unsigned, `413`, `404`, `/healthz`).

## 7. Security notes

- The receiver listens on loopback by default. If you expose it directly, put TLS
  in front of it; PeeStream only registers HTTPS webhook URLs.
- Signature comparison is constant time (`hmac.compare_digest`).
- The raw body is read with a hard size limit before any parsing.
- Shell hooks use argv lists, never `shell=True`, and get a timeout.
- Secrets are excluded from logs, `check` output, `/healthz`, and the sink files.

## 8. Versioning

Connector `1.0.0` targets PeeStream API contract **v1.1**. Locally delivered
events are never replayed to PeeStream; this component only receives.

## 9. License & Support

- **License:** GNU General Public License v2 (GPLv2) - see `LICENSE`.
- **Security policy:** see `SECURITY.md`. Report vulnerabilities privately; never put secrets or event payloads in a public Issue.
- **Official support:** `https://peestream.com` or the WJunction support thread.
- **Release integrity:** `SHA256SUMS` lists the SHA-256 of the release archive `peestream-autopublish-connector_v1.0.0.tar.gz`. Download the asset from Releases into any directory, place `SHA256SUMS` beside it, and run `sha256sum -c SHA256SUMS`. Rebuild the archive locally with `scripts/build-release.sh` (byte-reproducible).
