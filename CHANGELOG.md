# Changelog

## 1.0.0 - 2026-11-15

Initial standalone connector for non-WordPress, non-DLE sites, implementing
PeeStream API contract v1.1.

- HTTP receiver (`http.server`) with configurable host/port, body size limit and
  `GET /healthz`.
- HMAC-SHA256 verification of `X-PeeStream-Signature` over the raw body, constant
  time comparison, `401` on any malformed header.
- Dedupe ledger in SQLite keyed by `event_id`, plus `occurred_at` age window
  (default 72 h). Event is claimed before actions run.
- Unknown events acknowledged with `200` and ignored (forward compatibility).
- Actions: shell hook (`stdin`/`argument`), webhook forward (verbatim or
  re-signed), JSON file sink with optional SQLite catalog index keyed by
  IMDb/TMDb identity, season `0` preserved.
- Append-only local journal of every request outcome for operator forensics.
- CLI: `serve`, `check`, `simulate`.
- `config.json.example`, hardened systemd unit, English README.
- 42 stdlib-only tests.
- GPLv2 `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md`, `SHA256SUMS` and a
  byte-reproducible `scripts/build-release.sh`; release archive
  `dist/peestream-autopublish-connector_v1.0.0.tar.gz`.
