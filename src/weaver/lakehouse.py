"""One resolved Lakehouse — where an authored object's bytes are.

An authored object is an ordinary Python object bound to a Spark session, and
this is the second half of that binding: the *destination*. It exists because a
Spark session cannot answer "which Lakehouse" on its own. A session builds
several destinations in one invocation (see
:class:`~weaver.locations.LakehouseSparkLocation`), so the destination is
resolved and supplied, never inferred from whatever the session happens to be
attached to.

There is exactly one exception, and it is the notebook case:

* a developer working in a Fabric notebook has attached a default Lakehouse, and
  that attachment *is* the answer — :func:`default_lakehouse` reads it;
* the orchestrator, and anyone addressing more than one Lakehouse, resolves the
  destination by name and passes it in — :func:`lakehouse_for`.

Both produce the same value, so authored code cannot tell which path built it.

**Two roots, one location.** ``spark_root`` is what Spark and Hadoop address —
an ``abfss://`` URL on Fabric, a directory locally. ``fuse_root`` is what
ordinary filesystem calls address — ``/lakehouse/default`` on Fabric, the same
directory locally. They name the same storage through different mechanisms, so
the path arithmetic is shared: both are turned into a
:class:`~weaver.locations.LakehouseSparkLocation` and joined by it, rather than
by a second set of string joins that could drift.

``fuse_root`` is None when there is no mount — a Fabric Lakehouse that is not the
session's default has no FUSE path, and asking for a folder path there fails with
that reason rather than composing one that does not exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import LoadError
from .locations import LakehouseSparkLocation
from .resolution import TABLES_AREA
from .spark.destination import SparkDestination
from .targets import FILES_AREA, ItemRef

#: Where Fabric mounts the notebook's attached Lakehouse for ordinary file access.
FUSE_DEFAULT_ROOT = "/lakehouse/default"

#: The Spark-facing root of a Fabric item. The same template as
#: :func:`weaver.fabric.onelake.abfss_root`, repeated because the core imports
#: without the optional ``fabric`` extra and a notebook must be able to infer its
#: own Lakehouse with nothing installed beyond Weaver. ``test_lakehouse`` asserts
#: the two stay identical.
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

    ``destination`` is how a *statement* names this Lakehouse, and it is only
    needed by objects that have no path of their own — a view exists as a
    catalogue name and nothing else. It has no default: a bare ``Schema.Object``
    resolves through whatever the session is attached to, which is the
    ambient-context anti-pattern the rest of Weaver refuses. The one Lakehouse
    that may be named that way is the session's own attachment, and
    :func:`default_lakehouse` says so explicitly.
    """

    name: str
    spark_root: str
    fuse_root: str | None = None
    destination: SparkDestination | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "spark_root", _root(self.spark_root, what="spark root"))
        if self.fuse_root is not None:
            object.__setattr__(self, "fuse_root", _root(self.fuse_root, what="FUSE root"))
        if not str(self.name).strip():
            raise LoadError("a Lakehouse must be named")

    # --- the two transports ------------------------------------------------

    @property
    def location(self) -> LakehouseSparkLocation:
        """The Spark-addressed roots — what Delta reads and writes through."""

        return _areas(self.name, self.spark_root)

    @property
    def fuse_location(self) -> LakehouseSparkLocation:
        """The filesystem-addressed roots. Fails when nothing is mounted."""

        if self.fuse_root is None:
            raise LoadError(
                f"Lakehouse {self.name!r} has no FUSE mount, so it has no ordinary "
                "filesystem path — only the notebook's attached Lakehouse is mounted, "
                "and any other is reached through Spark or a store"
            )
        return _areas(self.name, self.fuse_root)

    # --- one object's physical location ------------------------------------

    def table_path(self, schema: str, name: str) -> str:
        """Where one table's Delta files live, for Spark to read."""

        return self.location.table_path(schema, name)

    def folder_path(self, schema: str, name: str) -> str:
        """Where one folder object's files live, for ordinary file access."""

        return self.fuse_location.folder_path(schema, name)

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


def lakehouse_for(resolver: Any, item: ItemRef | str) -> Lakehouse:
    """Resolve a Lakehouse by name, through a workspace resolver.

    This is the orchestrator's path, and the one an advanced caller uses to reach
    a Lakehouse that is not the attached default. Name resolution stays here,
    outside the authored object: an object is given a resolved Lakehouse, never a
    name to look up.
    """

    reference = ItemRef(item) if isinstance(item, str) else item
    return Lakehouse(
        name=reference.name,
        spark_root=resolver.spark_root(reference),
        fuse_root=resolver.fuse_root(reference),
        destination=resolver.spark_destination(reference),
    )


def default_lakehouse(spark: Any) -> Lakehouse:
    """The Lakehouse this Fabric session has attached, or fail saying so.

    Only ever the *default* attachment. A session with none, or a host that is not
    Fabric, cannot answer — and answering wrongly would write a build into
    whichever Lakehouse happened to be first, so it raises instead.
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
        spark_root=_ABFSS_ROOT.format(workspace=workspace, item=item),
        fuse_root=FUSE_DEFAULT_ROOT,
        # The one place plain two-part naming is correct: this Lakehouse *is* what
        # the session is attached to, so its catalogue is the session's own.
        destination=SparkDestination(item=name or item),
    )


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
    if not isinstance(value, str) or not value.strip():
        raise LoadError(f"a Lakehouse {what} must be a non-empty string, got {value!r}")
    return value.strip().replace("\\", "/").rstrip("/")


def _areas(name: str, root: str) -> LakehouseSparkLocation:
    """One root, split into the two areas a Lakehouse presents."""

    return LakehouseSparkLocation(
        item=name,
        tables_root=f"{root}/{TABLES_AREA}",
        files_root=f"{root}/{FILES_AREA}",
    )
