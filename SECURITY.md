# Security Policy

Security matters for a daemon that accepts signed webhooks from a PeeStream account and runs
local actions on the host it is installed on.

## Supported release status

Connector `1.0.0` is the first public release. It is **beta software** and **has not been
validated end-to-end against a live PeeStream account**; what has been verified is the local
receiver, HMAC-SHA256 verification of `X-PeeStream-Signature`, the 72-hour `occurred_at` window,
`event_id` deduplication, and the configured actions, all with synthetic secrets. Run it on a
staging host first.

| Version | Supported |
| ------- | --------- |
| 1.0.x   | yes       |

Requires Python 3.8 or newer.

## Reporting a vulnerability

Please **do not post webhook secrets, API keys, tokens, passwords, private URLs, event payloads
with credentials, database dumps, or exploit details in public Issues**.

If GitHub offers **Report a vulnerability** in this repository's **Security** tab, use that
private channel. Otherwise use the [official PeeStream website](https://peestream.com) to request
a private security contact method before sharing sensitive details.

Include the connector version, Python version, operating system, how the receiver is started
(systemd unit, manual, container), the affected event type, the impact, and a minimal reproduction
with sanitized payloads and logs.

## Safe configuration and operation

- `config.json` holds `webhook_secret`. Install it `0640` owned by `root:peestream`, or point
  `$PEESTREAM_CONNECTOR_CONFIG` at an environment-managed file. The connector never writes the
  secret to logs, to `/healthz`, to `check` output, or to the sink files.
- Listen on `127.0.0.1` behind nginx/Caddy/an TLS tunnel. PeeStream registers HTTPS webhook URLs
  only, and the receiver itself speaks plain HTTP.
- Shell hooks run with the service user's privileges. Treat `actions.hooks` as trusted
  administrator configuration: argv lists only, no `shell=True`, no interpolation of untrusted
  event fields into command strings.
- Keep `db_path`, the JSON sink directory and the log directory writable only by the service user.
- After a secret or host compromise, rotate with
  `POST /api/v2/sites/{site_id}/rotate-secret`; signatures made with the old secret fail
  immediately.
- The `occurred_at` window rejects stale deliveries by design. After a long outage, re-register or
  replay deliberately from PeeStream instead of widening the window on the receiver.
- Events outside the window are refused, not queued, so a flood of old deliveries cannot fill the
  dedupe ledger.

## Verifying a release

Each release archive is listed in `SHA256SUMS`:

```bash
sha256sum -c SHA256SUMS
```
