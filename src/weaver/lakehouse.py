"""Resolved Lakehouse destinations for authored objects."""

from __future__ import annotations

import io
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import LoadError
from .locations import LakehouseSparkLocation
from .resolution import TABLES_AREA
from .spark import FabricSparkTarget, identifier
from .targets import FILES_AREA, ItemRef

#: The Spark-facing root of a Fabric item. The same template as
#: :func:`weaver.fabric.onelake.abfss_root`, repeated so that inferring an
#: attached Lakehouse needs no import from ``weaver.fabric``. This module is
#: reached by authored object code, which should pull in no transport.
#: ``test_lakehouse`` asserts the two stay identical.
_ABFSS_ROOT = "abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{item}"

#: Session settings Fabric sets for the attached Lakehouse. Read in order; the
#: first that answers wins.
_WORKSPACE_KEYS = ("trident.workspace.id", "trident.artifact.workspace.id")
_LAKEHOUSE_ID_KEYS = ("trident.lakehouse.id",)
_LAKEHOUSE_NAME_KEYS = ("trident.lakehouse.name",)

#: The same three facts as the notebook runtime reports them, for a host that
#: carries the context but not the session settings.
_CONTEXT_WORKSPACE_KEYS = ("defaultLakehouseWorkspaceId", "currentWorkspaceId")
_CONTEXT_LAKEHOUSE_ID_KEYS = ("defaultLakehouseId",)
_CONTEXT_LAKEHOUSE_NAME_KEYS = ("defaultLakehouseName",)


@dataclass(frozen=True)
class Lakehouse:
    """A resolved destination Lakehouse for authored code.

    ``destination`` is required to name catalogue-only objects such as views.
    Only :func:`default_lakehouse` may use the session's attached catalogue.
    """

    name: str
    spark_root: str
    destination: "FabricSparkTarget | AttachedLakehouse | None" = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "spark_root", _root(self.spark_root, what="root"))
        if not str(self.name).strip():
            raise LoadError("a Lakehouse must be named")

    # --- one object's physical location ------------------------------------

    @property
    def location(self) -> LakehouseSparkLocation:
        return _areas(self.name, self.spark_root)

    def table_path(self, schema: str, name: str) -> str:
        """Return the table's ``abfss://`` Delta path."""

        return self.location.table_path(schema, name)

    def files_root(self) -> str:
        """The ``Files`` area as Python can address it, resolved on use.

        The returned mount path is session-scoped and must not be stored;
        ``spark_root`` is the durable identity.
        """

        return _join(_mounted(self.name, self.spark_root), FILES_AREA)

    def folder_path(self, schema: str, name: str) -> Path:
        """Return the folder's session-scoped :class:`pathlib.Path`."""

        return Path(_join(self.files_root(), schema, name))

    def folder_spark_path(self, schema: str, name: str) -> str:
        """Return the folder's ``abfss://`` path for Spark reads."""

        return self.location.folder_path(schema, name)

    def qualify(self, schema: str, name: str) -> str:
        """One object, as a statement in this session must name it."""

        if self.destination is None:
            raise LoadError(
                f"Lakehouse {self.name!r} was resolved without a Spark destination, so "
                "a statement cannot name its objects. Resolve it with "
                "weaver.lakehouse_for(resolver, item), which supplies one"
            )
        return self.destination.qualify(schema, name)

    def __str__(self) -> str:
        return f"{self.name} ({self.spark_root})"


@dataclass(frozen=True)
class AttachedLakehouse:
    """Two-part naming for the Lakehouse this session already has attached.

    The one exception to naming every destination in full, and it is bounded by
    the same rule: here the session's catalogue is the destination, so
    ``Schema.Object`` resolves to this Lakehouse and nowhere else. Every other
    destination carries a :class:`~weaver.spark.FabricSparkTarget`.
    """

    lakehouse: str

    @property
    def item(self) -> str:
        return self.lakehouse

    def qualified_schema(self, schema: str) -> str:
        return identifier(schema)

    def qualify(self, schema: str, name: str) -> str:
        return f"{identifier(schema)}.{identifier(name)}"


def lakehouse_for(resolver: Any, item: ItemRef | str) -> Lakehouse:
    """Resolve a named Lakehouse outside an authored object."""

    reference = ItemRef(item) if isinstance(item, str) else item
    return Lakehouse(
        name=reference.name,
        spark_root=resolver.spark_root(reference),
        destination=resolver.spark_destination(reference),
    )


def default_lakehouse(spark: Any) -> Lakehouse:
    """Return the Lakehouse attached to this Fabric session."""

    workspace, item, name = _attached_from_settings(spark)
    if not (workspace and item):
        # Field by field, so a host that answers half through the session and half
        # through its runtime context still resolves, and so a blank second source
        # never erases what the first one supplied.
        settings = (workspace, item, name)
        context = _attached_from_runtime_context()
        workspace, item, name = tuple(a or b for a, b in zip(settings, context))

    if not item:
        raise LoadError(
            "no Lakehouse is attached to this Spark session, so there is no "
            "destination to infer. Attach a Lakehouse to the notebook, or "
            "construct the object with lakehouse=<resolved Lakehouse>"
        )
    if not workspace:
        raise LoadError(
            "no workspace is available for the attached Lakehouse, so its OneLake address "
            "is unavailable. Construct the object with "
            "lakehouse=<resolved Lakehouse>"
        )
    return Lakehouse(
        name=name or item,
        # The attachment's storage is reached the same way every other Lakehouse
        # is, by its OneLake root. The ``/lakehouse/default`` mount addresses the
        # same bytes, but only from a session that attached it, so nothing here
        # depends on one.
        spark_root=_ABFSS_ROOT.format(workspace=workspace, item=item),
        # The one place plain two-part naming is correct: this Lakehouse is what
        # the session is attached to, so its catalogue is the session's own.
        destination=AttachedLakehouse(lakehouse=name or item),
    )


# --- the files root ---------------------------------------------------------

#: Mount points already established in this session, by ``abfss://`` root. A
#: session is one process, so this is process state: a second load of the same
#: Lakehouse reuses the mount rather than asking Fabric to make another, which
#: it refuses.
_MOUNTS: dict[str, str] = {}

#: Where Weaver mounts a Lakehouse. Keyed by item id rather than fixed, because
#: an estate spans several Lakehouses and one session may load from more than
#: one, and a single fixed point would let the second address the first.
_MOUNT_POINT = "/weaver/{item}"

#: Mount configuration. ``fileCacheTimeout=0`` because Weaver reaches the same
#: Files area two ways, ``abfss://`` for storage work and this mount for authored
#: Python, so a change made outside the mount must be visible through
#: it at once. With caching on, a directory listing still holds entries the
#: storage no longer has, and ``shutil.rmtree`` fails with ``ENOTEMPTY``.
#:
#: Invalidating afterwards does not work: dropping Weaver's record of the mount
#: leaves the host's in place, and asking again recovers the same stale view.
MOUNT_OPTIONS = {"fileCacheTimeout": 0}


@contextmanager
def _quiet_mount():
    """Suppress mount output while leaving errors and warnings visible."""

    try:
        from IPython.utils.capture import capture_output
    except ImportError:
        with redirect_stdout(io.StringIO()):
            yield
        return

    with capture_output(stdout=True, stderr=False, display=True):
        yield


def _mounted(name: str, spark_root: str) -> str:
    """Return a session mount for this resolved OneLake root."""

    cached = _MOUNTS.get(spark_root)
    if cached:
        return cached

    utils = _notebook_utils()
    if utils is None:
        raise LoadError(
            f"Lakehouse {name!r} Files are unavailable outside a Fabric session. "
            "Run this Folder load in Fabric."
        )

    point = _MOUNT_POINT.format(item=_item_of(spark_root))
    try:
        with _quiet_mount():
            utils.fs.mount(spark_root, point, MOUNT_OPTIONS)
    except Exception:
        # The host may already hold the mount even when this cache does not.
        pass
    try:
        local = utils.fs.getMountPath(point)
    except Exception as exc:
        raise LoadError(
            f"Lakehouse {name!r} Files could not be mounted: {exc}"
        ) from exc
    if not local:
        raise LoadError(f"Lakehouse {name!r} mount returned no filesystem path")
    _MOUNTS[spark_root] = local
    return local


def _notebook_utils() -> Any:
    for module_name in ("notebookutils", "mssparkutils"):
        try:
            return __import__(module_name)
        except Exception:
            continue
    return None


def _item_of(spark_root: str) -> str:
    return spark_root.rstrip("/").rsplit("/", 1)[-1]


def _join(root: str, *parts: str) -> str:
    joined = root.rstrip("/")
    for part in parts:
        joined = f"{joined}/{str(part).strip('/')}"
    return joined


# --- reading the host -------------------------------------------------------


def _attached_from_settings(spark: Any) -> tuple[str, str, str]:
    def setting(key: str) -> str:
        try:
            return _text(spark.conf.get(key, None))
        except Exception:  # pragma: no cover - a host that raises for unset keys
            return ""

    return (
        _first(setting, _WORKSPACE_KEYS),
        _first(setting, _LAKEHOUSE_ID_KEYS),
        _first(setting, _LAKEHOUSE_NAME_KEYS),
    )


def _attached_from_runtime_context() -> tuple[str, str, str]:
    context: Any = None
    for module_name in ("notebookutils", "mssparkutils"):
        try:
            module = __import__(module_name)
        except Exception:
            continue
        context = getattr(getattr(module, "runtime", None), "context", None)
        if context:
            break
    if not isinstance(context, dict):
        return "", "", ""

    def entry(key: str) -> str:
        return _text(context.get(key))

    return (
        _first(entry, _CONTEXT_WORKSPACE_KEYS),
        _first(entry, _CONTEXT_LAKEHOUSE_ID_KEYS),
        _first(entry, _CONTEXT_LAKEHOUSE_NAME_KEYS),
    )


def _first(read, keys: tuple[str, ...]) -> str:
    for key in keys:
        value = read(key)
        if value:
            return value
    return ""


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _root(value: Any, *, what: str) -> str:
    """Require the OneLake ``abfss://`` address used by Spark and mounts."""

    if not isinstance(value, str) or not value.strip():
        raise LoadError(f"a Lakehouse {what} must be a non-empty string, got {value!r}")
    cleaned = value.strip().replace("\\", "/").rstrip("/")
    if not cleaned.startswith("abfss://"):
        raise LoadError(
            f"a Lakehouse {what} must be a OneLake abfss:// address, got {value!r}"
        )
    return cleaned


def _areas(name: str, root: str) -> LakehouseSparkLocation:
    return LakehouseSparkLocation(
        item=name,
        tables_root=f"{root}/{TABLES_AREA}",
        files_root=f"{root}/{FILES_AREA}",
    )
