"""PeeStream AutoPublish standalone connector (universal webhook receiver)."""

__version__ = "1.0.0"

from .config import ConfigError, ConnectorConfig  # noqa: F401

__all__ = ["ConnectorConfig", "ConfigError", "__version__"]
