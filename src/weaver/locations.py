"""Resolved physical locations.

A :class:`Location` is what a workspace resolution produces: the concrete place an
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
        # One separator, everywhere. A Windows caller reaches this with
        # backslashes — `LocalWorkspace` normalises its root through `Path`, and
        # `str()` of a `WindowsPath` uses them — while everything downstream
        # treats "/" as the only separator: `join`, `name`, and the segment
        # splitting in the Weaver document reader. Left alone, a repository read from a
        # Windows checkout takes its whole path as its catalogue name.
        #
        # Safe against real names: "\" is rejected in object and schema names
        # (see targets._ILLEGAL_IN_NAME), so a backslash here is always a
        # separator. URLs never carry one either.
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


@dataclass(frozen=True)
class LakehouseSparkLocation:
    """One destination Lakehouse's physical roots, resolved once.

    The Spark session is attached to the **Weaver Lakehouse** — that is the fixed
    control-plane context, and it is why Weaver's own catalogue tables are reached
    as ordinary two-part names in schema ``_``. Destination Lakehouses are the
    variable data plane, so they are reached through explicit roots instead, and
    never by making the session point somewhere else.

    That distinction is what lets one session build several Lakehouses in one
    invocation. Switching the current catalogue between targets would make two
    destinations that share a schema name indistinguishable; resolving each
    target's roots keeps them separate by construction::

        locations = {
            target: resolver.lakehouse_spark_location(target)
            for target in bound_lakehouse_targets
        }

    Roots are plain strings rather than :class:`Location` values because this is
    what *Spark* addresses: an ``abfss://`` URL on Fabric, a filesystem path in
    the local emulator. Same contract, different transport.

    A resolved location is deliberately **not** carried in a build bundle. It is
    derived from the item at install time, because on Fabric it embeds workspace
    and item ids and locally it embeds a temporary directory — and a bundle whose
    identity moved with a temporary path would not be comparable between
    environments (build-philosophy §10).
    """

    #: The Lakehouse this resolves, by its logical name.
    item: str
    tables_root: str
    files_root: str

    def schema_root(self, schema: str) -> str:
        """Where a schema's managed tables live."""

        return f"{self.tables_root.rstrip('/')}/{_segment(schema)}"

    def table_path(self, schema: str, name: str) -> str:
        """Where one managed Delta table lives."""

        return f"{self.schema_root(schema)}/{_segment(name)}"

    def folder_path(self, schema: str, name: str) -> str:
        """Where one managed folder lives, under the Files area."""

        return (
            f"{self.files_root.rstrip('/')}/{_segment(schema)}/{_segment(name)}"
        )

    def __str__(self) -> str:
        return f"{self.item} (tables={self.tables_root}, files={self.files_root})"


def _segment(value: str) -> str:
    """One path segment, checked rather than trusted.

    These strings are concatenated into paths Spark writes through, so a segment
    that escaped its parent would write outside the Lakehouse it names.
    """

    segment = value.strip().strip("/")
    if not segment or segment in (".", ".."):
        raise IdentityError(f"path segment must be a real name, got {value!r}")
    if "/" in segment or "\\" in segment:
        raise IdentityError(f"path segment must not contain a separator: {value!r}")
    return segment
