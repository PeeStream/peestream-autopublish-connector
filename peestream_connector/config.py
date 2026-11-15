"""Configuration loading for the PeeStream AutoPublish standalone connector.

The connector is deliberately dependency-free: a single JSON file (``config.json``)
is enough for a first run. Secrets may also be supplied through the environment,
which is the preferred route on shared hosts, because it keeps the raw secret out
of the file system and out of backups of the application directory.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

DEFAULT_PORT = 8855
DEFAULT_HOST = "127.0.0.1"
DEFAULT_MAX_BODY_BYTES = 1 * 1024 * 1024
DEFAULT_DEDUPE_TTL_HOURS = 72
DEFAULT_TIMEOUT_SECONDS = 15

_ENV_SECRET = "PEESTREAM_WEBHOOK_SECRET"
_ENV_SECRET_FILE = "PEESTREAM_WEBHOOK_SECRET_FILE"
_SITE_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


class ConfigError(ValueError):
    """Raised when the configuration is unusable; the daemon must refuse to start."""


def _as_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError("%s must be an integer, got %r" % (name, value)) from exc
    if parsed < minimum or parsed > maximum:
        raise ConfigError("%s must be between %d and %d, got %d"
                          % (name, minimum, maximum, parsed))
    return parsed


@dataclass
class SignatureConfig:
    secret: str
    header: str = "X-PeeStream-Signature"
    algorithm: str = "sha256"
    ttl_hours: int = DEFAULT_DEDUPE_TTL_HOURS


@dataclass
class ShellActionConfig:
    enabled: bool = False
    command: List[str] = field(default_factory=list)
    mode: str = "stdin"  # "stdin" (hook receives JSON on stdin) or "argument"
    timeout_seconds: int = 120
    pass_env: Dict[str, str] = field(default_factory=dict)


@dataclass
class WebhookActionConfig:
    enabled: bool = False
    url: str = ""
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    extra_headers: Dict[str, str] = field(default_factory=dict)
    # When true, the verified PeeStream signature is re-signed for the internal
    # endpoint with ``internal_secret`` instead of being forwarded verbatim.
    resign_internal: bool = False
    internal_secret: str = ""


@dataclass
class JsonSinkConfig:
    enabled: bool = False
    directory: str = "data/items"
    index_sqlite: bool = True
    sqlite_path: str = "data/catalog.sqlite3"


@dataclass
class ConnectorConfig:
    site_id: str
    host: str
    port: int
    db_path: str
    log_path: Optional[str]
    max_body_bytes: int
    signature: SignatureConfig
    shell: ShellActionConfig
    webhook: WebhookActionConfig
    json_sink: JsonSinkConfig
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], env: Optional[Dict[str, str]] = None) -> "ConnectorConfig":
        env = os.environ if env is None else env
        if not isinstance(data, dict):
            raise ConfigError("config root must be a JSON object")

        site_id = str(data.get("site_id") or "").strip()
        if not _SITE_ID_RE.match(site_id):
            raise ConfigError("site_id must match %s (got %r)" % (_SITE_ID_RE.pattern, site_id))

        server = data.get("server") or {}
        if not isinstance(server, dict):
            raise ConfigError("server must be an object")

        secret = _resolve_secret(server, env)
        if len(secret) < 32:
            raise ConfigError("webhook secret must be at least 32 characters")

        signature = SignatureConfig(
            secret=secret,
            header=str(server.get("signature_header") or "X-PeeStream-Signature"),
            ttl_hours=_as_int(server.get("dedupe_ttl_hours", DEFAULT_DEDUPE_TTL_HOURS),
                              "server.dedupe_ttl_hours", 1, 24 * 30),
        )

        actions = data.get("actions") or {}
        if not isinstance(actions, dict):
            raise ConfigError("actions must be an object")

        shell = _parse_shell(actions.get("shell") or {})
        webhook = _parse_webhook(actions.get("webhook") or {}, env)
        sink = _parse_sink(actions.get("json_sink") or {})

        if not (shell.enabled or webhook.enabled or sink.enabled):
            raise ConfigError("at least one action (shell, webhook, json_sink) must be enabled")

        return cls(
            site_id=site_id,
            host=str(server.get("host") or DEFAULT_HOST),
            port=_as_int(server.get("port", DEFAULT_PORT), "server.port", 1, 65535),
            db_path=str(data.get("db_path") or "data/connector.sqlite3"),
            log_path=data.get("log_path"),
            max_body_bytes=_as_int(server.get("max_body_bytes", DEFAULT_MAX_BODY_BYTES),
                                   "server.max_body_bytes", 1024, 64 * 1024 * 1024),
            signature=signature,
            shell=shell,
            webhook=webhook,
            json_sink=sink,
            raw=dict(data),
        )

    @classmethod
    def load(cls, path: str, env: Optional[Dict[str, str]] = None) -> "ConnectorConfig":
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError as exc:
            raise ConfigError("config file not found: %s" % path) from exc
        except json.JSONDecodeError as exc:
            raise ConfigError("config file is not valid JSON (%s): %s" % (path, exc)) from exc
        return cls.from_dict(data, env)

    def sanitized(self) -> Dict[str, Any]:
        """Config copy safe for logs and diagnostics (no secret material)."""
        return {
            "site_id": self.site_id,
            "server": {
                "host": self.host,
                "port": self.port,
                "signature_header": self.signature.header,
                "dedupe_ttl_hours": self.signature.ttl_hours,
                "max_body_bytes": self.max_body_bytes,
            },
            "db_path": self.db_path,
            "actions": {
                "shell": {"enabled": self.shell.enabled, "mode": self.shell.mode},
                "webhook": {"enabled": self.webhook.enabled, "url": self.webhook.url},
                "json_sink": {"enabled": self.json_sink.enabled,
                              "directory": self.json_sink.directory},
            },
        }


def _resolve_secret(server: Dict[str, Any], env: Dict[str, str]) -> str:
    secret = (env.get(_ENV_SECRET) or "").strip()
    if secret:
        return _reject_placeholder(secret)
    secret_file = (env.get(_ENV_SECRET_FILE) or "").strip()
    if secret_file:
        try:
            with open(secret_file, "r", encoding="utf-8") as handle:
                return _reject_placeholder(handle.read().strip())
        except OSError as exc:
            raise ConfigError("cannot read %s: %s" % (_ENV_SECRET_FILE, exc)) from exc
    return _reject_placeholder(str(server.get("webhook_secret") or "").strip())


_PLACEHOLDERS = ("replace_me", "changeme", "your_secret", "todo", "xxxx")


def _reject_placeholder(secret: str) -> str:
    """Refuse the shipped example value and other obvious placeholders.

    ``REPLACE_ME_WITH_THE_SECRET_...`` is longer than 32 characters, so a length
    check alone would let a user start the daemon with a publicly known secret.
    """
    lowered = secret.lower()
    for marker in _PLACEHOLDERS:
        if lowered.startswith(marker) or marker in lowered:
            raise ConfigError("webhook secret still looks like a placeholder (%r); "
                              "use the value returned by POST /api/v2/sites/register"
                              % secret[:24])
    return secret


def _parse_shell(raw: Any) -> ShellActionConfig:
    if not isinstance(raw, dict) or not raw.get("enabled"):
        return ShellActionConfig()
    command = raw.get("command")
    if isinstance(command, str):
        command = command.split()
    if not isinstance(command, list) or not command or not all(isinstance(c, str) for c in command):
        raise ConfigError("actions.shell.command must be a non-empty array of strings")
    mode = str(raw.get("mode") or "stdin")
    if mode not in ("stdin", "argument"):
        raise ConfigError("actions.shell.mode must be 'stdin' or 'argument'")
    pass_env = raw.get("pass_env") or {}
    if not isinstance(pass_env, dict):
        raise ConfigError("actions.shell.pass_env must be an object")
    return ShellActionConfig(
        enabled=True,
        command=list(command),
        mode=mode,
        timeout_seconds=_as_int(raw.get("timeout_seconds", 120),
                                "actions.shell.timeout_seconds", 1, 3600),
        pass_env={str(k): str(v) for k, v in pass_env.items()},
    )


def _parse_webhook(raw: Any, env: Dict[str, str]) -> WebhookActionConfig:
    if not isinstance(raw, dict) or not raw.get("enabled"):
        return WebhookActionConfig()
    url = str(raw.get("url") or "").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ConfigError("actions.webhook.url must be an http(s) URL")
    extra = raw.get("extra_headers") or {}
    if not isinstance(extra, dict):
        raise ConfigError("actions.webhook.extra_headers must be an object")
    resign = bool(raw.get("resign_internal"))
    internal_secret = str(env.get("PEESTREAM_INTERNAL_WEBHOOK_SECRET")
                          or raw.get("internal_secret") or "").strip()
    if resign and len(internal_secret) < 16:
        raise ConfigError("resign_internal requires a 16+ char internal secret")
    return WebhookActionConfig(
        enabled=True,
        url=url,
        timeout_seconds=_as_int(raw.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS),
                                "actions.webhook.timeout_seconds", 1, 300),
        extra_headers={str(k): str(v) for k, v in extra.items()},
        resign_internal=resign,
        internal_secret=internal_secret,
    )


def _parse_sink(raw: Any) -> JsonSinkConfig:
    if not isinstance(raw, dict) or not raw.get("enabled"):
        return JsonSinkConfig()
    return JsonSinkConfig(
        enabled=True,
        directory=str(raw.get("directory") or "data/items"),
        index_sqlite=bool(raw.get("index_sqlite", True)),
        sqlite_path=str(raw.get("sqlite_path") or "data/catalog.sqlite3"),
    )
