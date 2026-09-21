"""The execution intent a bundle freezes at build time.

A bundle names where it installs: the workload workspace, the catalogue
Warehouse, the Fabric Environment its remote programs need, and the Lakehouse a
Spark session attaches to. Install reads that descriptor; it never takes those
decisions from the caller's configuration.

Physical destinations stay in :class:`~weaver.build_bundle.targets.BoundTarget`.
The descriptor references them by manifest id rather than repeating them.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping

from ..errors import BuildError
from .targets import LAKEHOUSE_TARGET, WAREHOUSE_TARGET

#: Executors whose actions cannot run without a Spark session.
SPARK_EXECUTORS = frozenset({"spark_sql", "spark_sql_batch", "spark_table"})


@dataclass(frozen=True)
class BundleEnvironment:
    """The Fabric Environment a bundle's remote programs are published to.

    ``workspace`` preserves a qualified ``Workspace/Environment`` reference;
    ``None`` means the workload workspace owns it.
    """

    name: str
    workspace: str | None = None
    item_id: str | None = None

    @property
    def reference(self) -> str:
        return f"{self.workspace}/{self.name}" if self.workspace else self.name

    def to_mapping(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "workspace": self.workspace,
            "item_id": self.item_id,
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BundleEnvironment":
        return cls(
            name=mapping["name"],
            workspace=mapping.get("workspace"),
            item_id=mapping.get("item_id"),
        )


@dataclass(frozen=True)
class ExecutionIdentity:
    """What the orchestration boundary resolves before planning starts.

    The planner turns this into a :class:`BundleExecution` once it knows the
    catalogue target and whether the action set needs Spark. It resolves
    nothing itself.
    """

    workspace_name: str
    workspace_id: str | None = None
    environment: BundleEnvironment | None = None


@dataclass(frozen=True)
class BundleExecution:
    """Where a frozen bundle installs, and what it needs to get there.

    ``catalogue_target_id`` and ``spark_home_target_id`` name targets in the same
    manifest, so the catalogue and the Spark attachment cannot drift from the
    destinations the plan was generated against. ``spark_home_target_id`` is
    ``None`` when no planned action needs Spark.
    """

    workspace_name: str
    catalogue_target_id: str
    workspace_id: str | None = None
    environment: BundleEnvironment | None = None
    spark_home_target_id: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "workspace_name": self.workspace_name,
            "workspace_id": self.workspace_id,
            "catalogue_target_id": self.catalogue_target_id,
            "environment": (
                None if self.environment is None else self.environment.to_mapping()
            ),
            "spark_home_target_id": self.spark_home_target_id,
        }

    @classmethod
    def of(
        cls,
        identity: ExecutionIdentity,
        *,
        catalogue_target_id: str,
        spark_home_target_id: str | None,
    ) -> "BundleExecution":
        return cls(
            workspace_name=identity.workspace_name,
            catalogue_target_id=catalogue_target_id,
            workspace_id=identity.workspace_id,
            environment=identity.environment,
            spark_home_target_id=spark_home_target_id,
        )

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "BundleExecution":
        environment = mapping.get("environment")
        return cls(
            workspace_name=mapping["workspace_name"],
            catalogue_target_id=mapping["catalogue_target_id"],
            workspace_id=mapping.get("workspace_id"),
            environment=(
                None
                if environment is None
                else BundleEnvironment.from_mapping(environment)
            ),
            spark_home_target_id=mapping.get("spark_home_target_id"),
        )


def resolve_execution_identity(workspace, *, session) -> ExecutionIdentity:
    """Freeze the workspace and Environment a build installs into.

    Ids come from the Session's resolver where it can supply them. Resolution is
    not made a build requirement: a workspace that cannot be resolved here still
    produces an installable bundle, addressed by the names it was built for.
    """

    identity = ExecutionIdentity(
        workspace_name=str(workspace.workspace),
        workspace_id=_workspace_id(workspace, session=session),
    )
    reference = getattr(workspace, "environment", None)
    if reference is None:
        return identity
    return replace(
        identity,
        environment=BundleEnvironment(
            name=reference.name,
            workspace=reference.workspace,
            item_id=_environment_id(reference, workspace, session=session),
        ),
    )


def _workspace_id(workspace, *, session) -> str | None:
    try:
        return getattr(session.resolve_workspace(workspace), "id", None)
    except Exception:  # noqa: BLE001 - an unresolvable id is not a build failure
        return None


def _environment_id(reference, workspace, *, session) -> str | None:
    from ..fabric.resources import ENVIRONMENT

    if reference.workspace and reference.workspace != workspace.workspace:
        # A qualified reference names another workspace, which this Session's
        # scope does not resolve items in.
        return None
    try:
        return getattr(
            session.resolve_item(
                reference.name, item_type=ENVIRONMENT, workspace=workspace
            ),
            "id",
            None,
        )
    except Exception:  # noqa: BLE001 - an unresolvable id is not a build failure
        return None


def plan_needs_spark(plan) -> bool:
    """Whether any planned action has to run through a Spark session."""

    return any(
        action.executor in SPARK_EXECUTORS
        for _sequence, _batch, action in plan.actions()
    )


def spark_home_of(targets):
    """The Lakehouse a Spark session attaches to for these targets, or ``None``.

    The first Lakehouse in the order given. A build and the planner read the
    same ordered bound targets, so the attachment a build requires before Spark
    starts is the one the bundle freezes.
    """

    for target in targets:
        if target.kind == LAKEHOUSE_TARGET:
            return target
    return None


def select_spark_home(targets, *, needed: bool) -> str | None:
    """The id of the Lakehouse target a Spark session attaches to, or ``None``.

    Warehouse-only work needs no attachment and gets none, which is what keeps
    it off Livy.
    """

    if not needed:
        return None
    home = spark_home_of(targets)
    if home is not None:
        return home.id
    raise BuildError(
        "This build installs Spark work but binds no Lakehouse to attach a Spark "
        "session to. Bind a Lakehouse item, or build only Warehouse items."
    )


def validate_execution(execution: BundleExecution, plan) -> None:
    """Reject a descriptor that does not agree with the plan it travels with."""

    if not execution.workspace_name:
        raise BuildError(
            "The build bundle does not name the workspace it installs into. "
            "Regenerate it with this Weaver version."
        )

    by_id = {target.id: target for target in plan.targets}
    catalogue = by_id.get(execution.catalogue_target_id)
    if catalogue is None:
        raise BuildError(
            f"The build bundle names catalogue target "
            f"{execution.catalogue_target_id!r}, which it does not declare. "
            "Regenerate it with this Weaver version."
        )
    if catalogue.kind != WAREHOUSE_TARGET:
        raise BuildError(
            f"The build bundle's catalogue target {catalogue.id!r} is a "
            f"{catalogue.kind}, not a Warehouse. Regenerate it with this Weaver "
            "version."
        )

    home_id = execution.spark_home_target_id
    if home_id is not None:
        home = by_id.get(home_id)
        if home is None:
            raise BuildError(
                f"The build bundle attaches Spark to target {home_id!r}, which it "
                "does not declare. Regenerate it with this Weaver version."
            )
        if home.kind != LAKEHOUSE_TARGET:
            raise BuildError(
                f"The build bundle attaches Spark to target {home.id!r}, which is "
                f"a {home.kind}, not a Lakehouse. Regenerate it with this Weaver "
                "version."
            )
    elif plan_needs_spark(plan):
        raise BuildError(
            "The build bundle installs Spark work but names no Lakehouse for the "
            "Spark session to attach to. Regenerate it with this Weaver version."
        )

    for target in plan.targets:
        if target.workspace_name and target.workspace_name != execution.workspace_name:
            raise BuildError(
                f"The build bundle installs into workspace "
                f"{execution.workspace_name!r} but sends target {target.id!r} to "
                f"{target.workspace_name!r}. Regenerate it with this Weaver "
                "version."
            )

    environment = execution.environment
    if environment is not None and not environment.name:
        raise BuildError(
            "The build bundle names an Environment with no name. Regenerate it "
            "with this Weaver version."
        )


def execution_workspace(execution: BundleExecution, plan):
    """The :class:`~weaver.workspaces.Workspace` an installation runs against.

    Built from the manifest alone. Nothing here reads workspace configuration,
    so installing the same bundle from any directory addresses the same estate.
    """

    from ..workspaces import Workspace

    catalogue = next(
        target for target in plan.targets if target.id == execution.catalogue_target_id
    )
    environment = execution.environment
    return Workspace(
        workspace=execution.workspace_name,
        catalogue=f"Warehouse/{catalogue.name}",
        environment=None if environment is None else environment.reference,
    )


def execution_spark_home(execution: BundleExecution, plan) -> str | None:
    """The display name of the Lakehouse a Spark session must attach to."""

    if execution.spark_home_target_id is None:
        return None
    return next(
        target.name
        for target in plan.targets
        if target.id == execution.spark_home_target_id
    )


__all__ = [
    "SPARK_EXECUTORS",
    "BundleEnvironment",
    "BundleExecution",
    "ExecutionIdentity",
    "resolve_execution_identity",
    "execution_spark_home",
    "execution_workspace",
    "plan_needs_spark",
    "select_spark_home",
    "spark_home_of",
    "validate_execution",
]
