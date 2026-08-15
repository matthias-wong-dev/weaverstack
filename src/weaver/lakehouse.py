"""Resolved Lakehouse destinations for authored objects.

Notebook code can infer an attached default Lakehouse. Orchestration resolves an
explicit destination so a session can address multiple Lakehouses.
"""

from __future__ import annotations

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
#: attached Lakehouse needs no import from ``weaver.fabric`` — this module is
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
    """One destination Lakehouse, resolved once, as authored code reaches it.

    ``destination`` is how a statement names this Lakehouse, needed only by
    objects with no path of their own — a view exists as a catalogue name and
    nothing else. It has no default, because a bare ``Schema.Object`` resolves
    through whatever the session is attached to. The one Lakehouse that may be
    named that way is the session's own attachment, via
    :func:`default_lakehouse`.
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
        """The two areas this Lakehouse presents, joined by one arithmetic."""

        return _areas(self.name, self.spark_root)

    def table_path(self, schema: str, name: str) -> str:
        """Where one table's Delta files live.

        The Spark root, because Spark reads ``abfss://`` natively and a table is
        only ever reached through it.
        """

        return self.location.table_path(schema, name)

    def files_root(self) -> str:
        """The ``Files`` area as *Python* can address it, resolved on use.

        Two roots, because Spark takes ``abfss://`` while ``open()`` and
        ``pathlib`` cannot parse a URL — and a Folder's authored code is
        ordinary Python that writes files. Weaver mounts its own root and
        returns the mount path; a write through it is a write to OneLake,
        visible at once at the ``abfss://`` address.

        Session-scoped and never to be stored: Fabric spells it
        ``/synfs/notebook/<session id>/…``, valid only inside the session that
        made it. Durable identity is ``spark_root``.
        """

        return _join(_mounted(self.name, self.spark_root), FILES_AREA)

    def folder_path(self, schema: str, name: str) -> Path:
        """Where one folder object's files live, as *Python* addresses them.

        A real :class:`pathlib.Path`, because a Folder's authored code globs,
        opens and writes files: Weaver's mount of the resolved OneLake root, so
        a write through it is a write to OneLake.

        Session-scoped like :meth:`files_root`, and never to be stored.
        """

        return Path(_join(self.files_root(), schema, name))

    def folder_spark_path(self, schema: str, name: str) -> str:
        """The same folder, as *Spark* addresses it.

        What one object hands another when the reader is an engine. A table
        reads a folder's files with ``spark.read``, which wants the ``abfss://``
        form: given a mount path it resolves against its own default filesystem
        and asks for a path that does not exist.
        """

        return self.location.folder_path(schema, name)

    def qualify(self, schema: str, name: str) -> str:
        """One object, as a statement in this session must name it."""

        if self.destination is None:
            raise LoadError(
                f"Lakehouse {self.name!r} was resolved without a Spark destination, so "
                "a statement cannot name its objects — resolve it with "
                "weaver.lakehouse_for(resolver, item), which supplies one"
            )
        return self.destination.qualify(schema, name)

    def __str__(self) -> str:
        return f"{self.name} ({self.spark_root})"


@dataclass(frozen=True)
class AttachedLakehouse:
    """Two-part naming for the Lakehouse this session already has attached.

    The one exception to naming every destination in full, and it is bounded by
    the same rule: here the session's catalogue *is* the destination, so
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
    """Resolve a Lakehouse by name, through a workspace resolver.

    The orchestrator's path, and how a caller reaches a Lakehouse that is not
    the attached default. Name resolution stays outside the authored object: an
    object is given a resolved Lakehouse, never a name to look up.
    """

    reference = ItemRef(item) if isinstance(item, str) else item
    return Lakehouse(
        name=reference.name,
        spark_root=resolver.spark_root(reference),
        destination=resolver.spark_destination(reference),
    )


def default_lakehouse(spark: Any) -> Lakehouse:
    """The Lakehouse this Fabric session has attached, or fail saying so.

    Only ever the default attachment. A session with none, or a host that is
    not Fabric, raises rather than answering: a wrong answer would write a build
    into whichever Lakehouse happened to be first.
    """

    workspace, item, name = _attached_from_settings(spark)
    if not (workspace and item):
        # Field by field, so a host that answers half through the session and half
        # through its runtime context still resolves — and so a blank second source
        # never erases what the first one knew.
        settings = (workspace, item, name)
        context = _attached_from_runtime_context()
        workspace, item, name = tuple(a or b for a, b in zip(settings, context))

    if not item:
        raise LoadError(
            "no Lakehouse is attached to this Spark session, so there is no "
            "destination to infer — attach a default Lakehouse to the notebook, or "
            "construct the object with lakehouse=<resolved Lakehouse>"
        )
    if not workspace:
        raise LoadError(
            "this session reports an attached Lakehouse but no workspace, so its "
            "storage root cannot be composed — construct the object with "
            "lakehouse=<resolved Lakehouse>"
        )
    return Lakehouse(
        name=name or item,
        # The attachment's storage is reached the same way every other Lakehouse
        # is — by its OneLake root. The ``/lakehouse/default`` mount addresses the
        # same bytes, but only from a session that attached it, so nothing here
        # depends on one.
        spark_root=_ABFSS_ROOT.format(workspace=workspace, item=item),
        # The one place plain two-part naming is correct: this Lakehouse *is* what
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
#: one — a single fixed point would let the second quietly address the first.
_MOUNT_POINT = "/weaver/{item}"

#: Mount configuration. ``fileCacheTimeout=0`` because Weaver reaches the same
#: Files area two ways — ``abfss://`` for storage work and this mount for
#: authored Python — so a change made outside the mount must be visible through
#: it at once. With caching on, a directory listing still holds entries the
#: storage no longer has, and ``shutil.rmtree`` fails with ``ENOTEMPTY``.
#:
#: Invalidating afterwards does not work: dropping Weaver's record of the mount
#: leaves the host's in place, and asking again recovers the same stale view.
MOUNT_OPTIONS = {"fileCacheTimeout": 0}


def _mounted(name: str, spark_root: str) -> str:
    """Mount this Lakehouse's OneLake root, or reuse the mount already made.

    A mount turns the remote root into a local address; writes through it go
    straight to OneLake, so nothing is copied or flushed. Scoped to the job,
    which is why it is resolved on use rather than carried on the
    :class:`Lakehouse`.

    The root is one Weaver resolved by name, so this works detached — unlike the
    ``/lakehouse/default`` attachment.
    """

    cached = _MOUNTS.get(spark_root)
    if cached:
        return cached

    utils = _notebook_utils()
    if utils is None:
        raise LoadError(
            f"Lakehouse {name!r} is in OneLake, and reaching its Files area as a "
            "filesystem needs the Fabric notebook utilities, which are not "
            "available here. A Folder's authored code writes ordinary files, so "
            "there is no way to address them from outside a Fabric session."
        )

    point = _MOUNT_POINT.format(item=_item_of(spark_root))
    try:
        utils.fs.mount(spark_root, point, MOUNT_OPTIONS)
    except Exception:
        # Already mounted, by us in a path that did not reach the cache or by the
        # host itself. Mounting twice is an error, so the useful move is to ask
        # where it landed and carry on.
        pass
    try:
        local = utils.fs.getMountPath(point)
    except Exception as exc:
        raise LoadError(
            f"Lakehouse {name!r} could not be mounted at {point!r}: {exc}"
        ) from exc
    if not local:
        raise LoadError(f"Lakehouse {name!r} mounted at {point!r} reports no path")
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
    """The item id in an ``abfss://ws@host/item`` root — the mount's name."""

    return spark_root.rstrip("/").rsplit("/", 1)[-1]


def _join(root: str, *parts: str) -> str:
    joined = root.rstrip("/")
    for part in parts:
        joined = f"{joined}/{str(part).strip('/')}"
    return joined


# --- reading the host -------------------------------------------------------


def _attached_from_settings(spark: Any) -> tuple[str, str, str]:
    """What the Spark session's own settings say. Silent when it says nothing."""

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
    """What the notebook runtime says. Silent when it is not present."""

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
    """A Lakehouse root is a OneLake address, because a Lakehouse is in OneLake.

    Checked here so a path that looks like storage cannot stand in for
    one: everything below assumes a mount is available for the Files area and an
    ``abfss://`` URL is what Spark reads.
    """

    if not isinstance(value, str) or not value.strip():
        raise LoadError(f"a Lakehouse {what} must be a non-empty string, got {value!r}")
    cleaned = value.strip().replace("\\", "/").rstrip("/")
    if not cleaned.startswith("abfss://"):
        raise LoadError(
            f"a Lakehouse {what} must be a OneLake abfss:// address, got {value!r}"
        )
    return cleaned


def _areas(name: str, root: str) -> LakehouseSparkLocation:
    """One root, split into the two areas a Lakehouse presents."""

    return LakehouseSparkLocation(
        item=name,
        tables_root=f"{root}/{TABLES_AREA}",
        files_root=f"{root}/{FILES_AREA}",
    )
