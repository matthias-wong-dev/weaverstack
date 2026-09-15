"""Resolved filesystem and URL locations.

Locations use string joins so filesystem paths and Fabric URLs preserve their
transport-specific syntax.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .errors import IdentityError

_URL_MARKER = "://"


@dataclass(frozen=True)
class Location:
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str):
            raise IdentityError(
                f"location must be a string, got {type(self.value).__name__}"
            )
        value = self.value.strip()
        if not value:
            raise IdentityError("location must not be empty")
        # Downstream readers split only on "/". Backslashes cannot occur in
        # object or schema names, so Windows path separators are safe to normalise.
        value = value.replace("\\", "/")
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
                f"{self.value!r} is a URL location and has no filesystem path. "
                "Use a Store to read or write it"
            )
        return Path(self.value)

    def join(self, *parts: str) -> "Location":
        """Append segments without applying filesystem semantics to URLs."""

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
        return self.value.rstrip("/").rsplit("/", 1)[-1]

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class LakehouseSparkLocation:
    """Explicit roots let one Spark session address multiple Lakehouses.

    Roots remain strings because Spark addresses them as ``abfss://`` URLs.
    """

    item: str
    tables_root: str
    files_root: str

    def schema_root(self, schema: str) -> str:
        return f"{self.tables_root.rstrip('/')}/{_segment(schema)}"

    def table_path(self, schema: str, name: str) -> str:
        return f"{self.schema_root(schema)}/{_segment(name)}"

    def folder_path(self, schema: str, name: str) -> str:
        return f"{self.files_root.rstrip('/')}/{_segment(schema)}/{_segment(name)}"

    def __str__(self) -> str:
        return f"{self.item} (tables={self.tables_root}, files={self.files_root})"


def _segment(value: str) -> str:
    """Reject segments that could escape the Lakehouse root."""

    segment = value.strip().strip("/")
    if not segment or segment in (".", ".."):
        raise IdentityError(f"path segment must be a real name, got {value!r}")
    if "/" in segment or "\\" in segment:
        raise IdentityError(f"path segment must not contain a separator: {value!r}")
    return segment
