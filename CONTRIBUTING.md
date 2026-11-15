# Contributing to the PeeStream AutoPublish Connector

Thank you for helping improve the PeeStream ecosystem.

## Before opening an issue

- Search [existing issues](https://github.com/PeeStream/peestream-autopublish-connector/issues) for duplicates.
- State the connector version, Python version, operating system, reverse proxy and how the receiver is started (systemd, manual, container).
- Share concise reproduction steps, the failing event type and sanitized logs. Never include webhook secrets, API keys, tokens or unsanitized payloads.

## Pull requests

1. Open an issue describing the intended change before substantial work.
2. Keep the runtime dependency-free: Python 3.8+ standard library only.
3. Add or update tests in `tests/` and run `python3 -m unittest discover -s tests -v`. Identify mocked behaviour explicitly and never label a simulation as a live end-to-end test.
4. Do not weaken signature verification, the `occurred_at` window or `event_id` dedupe; a change there must come with a test for the case it closes.
5. Keep the receiver loopback-first and TLS outside the process. Do not log secrets.
6. Do not add retries that push events back to PeeStream. This component only receives.
7. Public documentation, issue templates and pull request descriptions are in professional English. Do not commit production configuration or credentials.
