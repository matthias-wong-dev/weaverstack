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

**One root, both areas.** ``spark_root`` is what Spark and Hadoop address — an
``abfss://`` URL on Fabric, a directory locally — and *everything* an authored
object reaches hangs off it: tables under ``Tables/``, folders under ``Files/``.
It is turned into a :class:`~weaver.locations.LakehouseSparkLocation` and joined
by that, so there is one piece of path arithmetic rather than a second set of
string joins that could drift.

Deliberately not a mount. ``/lakehouse/default`` addresses whichever Lakehouse a
notebook attached, and orchestration runs detached against Lakehouses it resolved
by name — so a folder that could only be reached through a mount could not be
loaded at all by the thing that loads it. The Hadoop-compatible root reaches
every resolved Lakehouse, attached or not, which is why it is the only one here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import LoadError
from .locations import LakehouseSparkLocation
from .resolution import TABLES_AREA
from .spark.destination import SparkDestination
from .targets import FILES_AREA, ItemRef

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
    destination: SparkDestination | None = None

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

        Two roots, because two things read them. Spark takes ``abfss://`` and is
        content; ``open()`` and ``pathlib`` cannot parse a URL at all, and a
        Folder object's authored code is ordinary Python — it writes files. So a
        folder needs the same bytes presented as a filesystem path.

        Locally the two are the same directory and this returns it unchanged. In
        Fabric the storage is object storage, so Weaver mounts its own root and
        returns the mount path. Nothing is copied: a write through the mount is a
        write to OneLake, visible immediately at the ``abfss://`` address.

        **The result is session-scoped and must never be stored.** Fabric spells
        it ``/synfs/notebook/<session id>/…`` — valid only inside the session
        that made it, and different in the next one. Durable identity is
        ``spark_root``; this is derived on use and thrown away, which is why it
        is a method rather than a field.
        """

        return _files_root(self.name, self.spark_root)

    def folder_path(self, schema: str, name: str) -> str:
        """Where one folder object's files live, as Python addresses them."""

        return _join(self.files_root(), schema, name)

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
        # The attachment's storage is reached the same way every other Lakehouse
        # is — by its OneLake root. The ``/lakehouse/default`` mount addresses the
        # same bytes, but only from a session that attached it, so nothing here
        # depends on one.
        spark_root=_ABFSS_ROOT.format(workspace=workspace, item=item),
        # The one place plain two-part naming is correct: this Lakehouse *is* what
        # the session is attached to, so its catalogue is the session's own.
        destination=SparkDestination(item=name or item),
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


def _files_root(name: str, spark_root: str) -> str:
    """``Files`` as a path ``open()`` understands, for whichever host this is."""

    if not spark_root.startswith("abfss://"):
        # The emulator: storage already *is* a filesystem, so the two roots are
        # the same directory and there is nothing to mount.
        return _join(spark_root, FILES_AREA)
    return _join(_mounted(name, spark_root), FILES_AREA)


def _mounted(name: str, spark_root: str) -> str:
    """Mount this Lakehouse's OneLake root, or reuse the mount already made.

    A mount turns the remote root into a local address; writes through it go
    straight to OneLake, so nothing is copied and nothing needs flushing. It is
    scoped to the job, which is why it is resolved here on use rather than
    carried in the :class:`Lakehouse`.

    Weaver mounts a root it resolved by name, so this works detached — it is not
    the ``/lakehouse/default`` attachment, which only ever addresses whatever a
    notebook happened to attach.
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
        utils.fs.mount(spark_root, point)
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
