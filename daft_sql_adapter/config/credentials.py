"""Credentials loading: env vars and optional config file. No secrets in logs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from daft_sql_adapter.exceptions import ConfigError


@dataclass(frozen=True)
class UnityCredentials:
    """Unity Catalog connection parameters. Immutable."""

    host: str
    token: str

    def validate(self) -> None:
        """Raise ConfigError if required fields are missing or invalid."""
        if not self.host or not self.host.strip():
            raise ConfigError("DATABRICKS_HOST is missing or empty")
        host_lower = self.host.strip().lower()
        if not (host_lower.startswith("https://") or host_lower.startswith("http://")):
            raise ConfigError("DATABRICKS_HOST must be a valid URL (e.g. https://<workspace>.cloud.databricks.com)")
        if not self.token or not self.token.strip():
            raise ConfigError("DATABRICKS_TOKEN is missing or empty")


class CredentialsProvider(Protocol):
    """Abstraction for providing Unity Catalog credentials (DIP)."""

    def get_credentials(self) -> UnityCredentials:
        """Return credentials. Raise ConfigError if not available."""
        ...


class EnvCredentialsProvider:
    """Load credentials from environment variables only."""

    HOST_KEY = "DATABRICKS_HOST"
    TOKEN_KEY = "DATABRICKS_TOKEN"
    CONFIG_PATH_KEY = "DAFT_SQL_ADAPTER_CONFIG"

    def get_credentials(self) -> UnityCredentials:
        host = os.environ.get(self.HOST_KEY, "").strip()
        token = os.environ.get(self.TOKEN_KEY, "").strip()

        if not host and not token:
            config_path = os.environ.get(self.CONFIG_PATH_KEY)
            if config_path and Path(config_path).exists():
                return self._load_from_file(Path(config_path))
            raise ConfigError(
                f"Unity credentials not found. Set {self.HOST_KEY} and {self.TOKEN_KEY}, "
                f"or set {self.CONFIG_PATH_KEY} to a config file path."
            )

        creds = UnityCredentials(host=host or "", token=token or "")
        creds.validate()
        return creds

    def _load_from_file(self, path: Path) -> UnityCredentials:
        """Load host and token from a simple key=value file. No secrets in logs."""
        if not path.exists():
            raise ConfigError(f"Config file not found: {path}")
        host = ""
        token = ""
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    key, value = key.strip().lower(), value.strip()
                    if key == "databricks_host":
                        host = value
                    elif key == "databricks_token":
                        token = value
        if not host or not token:
            raise ConfigError(
                f"Config file {path} must define databricks_host and databricks_token"
            )
        creds = UnityCredentials(host=host, token=token)
        creds.validate()
        return creds


def load_credentials(provider: CredentialsProvider | None = None) -> UnityCredentials:
    """Load credentials using the given provider or default env-based one."""
    if provider is None:
        provider = EnvCredentialsProvider()
    return provider.get_credentials()
