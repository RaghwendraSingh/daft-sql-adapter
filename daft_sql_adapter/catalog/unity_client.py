"""Build Unity Catalog client from credentials."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from daft_sql_adapter.config import UnityCredentials
from daft_sql_adapter.exceptions import RunnerError

if TYPE_CHECKING:
    from daft.unity_catalog import UnityCatalog


class UnityCatalogFactory(Protocol):
    """Abstraction for creating a Unity Catalog client (DIP)."""

    def create(self, credentials: UnityCredentials) -> UnityCatalog:
        """Build and return a configured UnityCatalog instance."""
        ...


def create_unity_catalog(credentials: UnityCredentials) -> UnityCatalog:
    """Create a Daft UnityCatalog instance from credentials."""
    try:
        from daft.unity_catalog import UnityCatalog
    except ImportError as e:
        raise RunnerError(
            "Unity Catalog support requires daft[unity]. Install with: pip install 'daft[unity]'"
        ) from e
    return UnityCatalog(
        endpoint=credentials.host.strip(),
        token=credentials.token.strip(),
    )
