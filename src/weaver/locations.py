"""Resolved physical locations.

A :class:`Location` is what a host resolution produces: the concrete place an
item, folder or table lives. It exists because ``pathlib.Path`` cannot be the
common currency — ``Path("abfss://ws@onelake.dfs.fabric.microsoft.com/lh")``
silently collapses the double slash and yields a broken root with no error.

So a location always carries a string and always joins by string. ``.path`` is
available when, and only when, the location is a filesystem path; asking a URL
location for a ``Path`` is a mistake worth raising on rather than corrupting.

Local resolution (checkpoint 2) produces filesystem locations. Fabric
resolution (checkpoint 7) will produce ``abfss://`` and OneLake URL locations
through the same type, so everything downstream is written once.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .errors import IdentityError

_URL_MARKER = "://"


@dataclass(frozen=True)
class Location:
    """One resolved location — a filesystem path or a URL."""

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str):
            raise IdentityError(f"location must be a string, got {type(self.value).__name__}")
        value = self.value.strip()
        if not value:
            raise IdentityError("location must not be empty")
        if len(value) > 1:
            value = value.rstrip("/")
        object.__setattr__(self, "value", value)

    @property
    def is_url(self) -> bool:
        return _URL_MARKER in self.value

    @property
    def path(self) -> Path:
        """The filesystem path. Raises for URL locations."""

        if self.is_url:
            raise IdentityError(
                f"{self.value!r} is a URL location and has no filesystem path — "
                "use a Store to read or write it"
            )
        return Path(self.value)

    def join(self, *parts: str) -> "Location":
        """Append path segments. Always a string join, never ``Path``."""

        joined = self.value
        for part in parts:
            if not isinstance(part, str):
                raise IdentityError(f"location segment must be a string, got {part!r}")
            segment = part.strip().strip("/")
            if not segment:
                raise IdentityError(f"location segment must not be empty: {part!r}")
            joined = f"{joined.rstrip('/')}/{segment}"
        return Location(joined)

    def __truediv__(self, part: str) -> "Location":
        return self.join(part)

    @property
    def name(self) -> str:
        """The final segment."""

        return self.value.rstrip("/").rsplit("/", 1)[-1]

    def __str__(self) -> str:
        return self.value
