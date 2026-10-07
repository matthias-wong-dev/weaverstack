"""Bind a frozen Build plan's targets to Session capabilities.

Every planned action receives one result. Capabilities resolve through the
Session; nothing here reads the source repository or changes the plan.

The workspace every capability is reached through comes from the bundle, not
from the Session. A Session supplies credentials, transport and reusable
resources; it does not decide where a frozen bundle installs.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Mapping

from ..delta_protocol import ProtocolMinima, ProtocolOptions
from ..errors import InstallError
from ..store import Store
from ..targets import ItemRef
from .executors import default_executors
from .executors.base import (
    ActionExecutor,
    InstallationContext,
    ResolvedTarget,
    SkippedExecution,
    Waiting,
)
from .models import InstallAction
from .report import (
    FAILED,
    SKIPPED,
    SUCCEEDED,
    ActionResult,
)
from .targets import WAREHOUSE_TARGET, BoundTarget

REPORT_FILENAME = "install-report.yml"


class _Deferred:
    """A Warehouse connection opened by the first executor that uses it."""

    __slots__ = ("_acquire", "_session")

    def __init__(self, acquire) -> None:
        object.__setattr__(self, "_acquire", acquire)
        object.__setattr__(self, "_session", None)

    def _resolved(self):
        if object.__getattribute__(self, "_session") is None:
            object.__setattr__(
                self, "_session", object.__getattribute__(self, "_acquire")()
            )
        return object.__getattribute__(self, "_session")

    def __getattr__(self, name):
        return getattr(self._resolved(), name)

    def __repr__(self) -> str:
        acquired = object.__getattribute__(self, "_session")
        return "<not yet acquired>" if acquired is None else repr(acquired)


class MutationBindings:
    """Execute an already-planned bundle through a Session."""

    def __init__(
        self,
        session,
        *,
        executors: dict[str, ActionExecutor] | None = None,
    ) -> None:
        self.session = session
        self._delegate_bundle = executors is None
        self._default_executor_types = {
            name: type(executor) for name, executor in default_executors().items()
        }
        self.executors = default_executors() if executors is None else executors
        self.workspace: Any = None

    def bind(self, workspace: Any) -> "MutationBindings":
        """Reach this workspace's capabilities, for a caller with no bundle.

        A caller assembling one action's context by hand has no manifest to read
        a workspace from and must not inherit one. :meth:`install` binds from
        the manifest and overwrites whatever was set here, so this cannot
        redirect a frozen bundle.
        """

        self.workspace = workspace
        return self

    def _bind(self, plan) -> Any:
        """Bind this installation to the workspace its bundle names.

        Done before any capability is reached, so a mismatched live Spark
        session is reported instead of quietly installing somewhere else.
        """

        from .execution import execution_spark_home, execution_workspace

        workspace = execution_workspace(plan.execution, plan)
        self.bind(workspace)
        self.session.require_spark_home(
            execution_spark_home(plan.execution, plan), workspace=workspace
        )
        return workspace

    @property
    def store(self) -> Store:
        return self.session.store(self.workspace)

    @property
    def resolver(self) -> Any:
        return self.session.resolver(self.workspace)

    @property
    def spark(self) -> Any:
        return self.session.spark(self.workspace)

    def spark_sql(self):
        session = self.session
        workspace = self.workspace

        def run(statement: str, *, exact_case: bool = False):
            return session.execute_spark_sql(
                statement, exact_case=exact_case, workspace=workspace
            )

        return run

    def spark_sql_batch(self):
        """Run ordered statements in one submission and identifier-case scope."""

        session = self.session
        workspace = self.workspace

        def run(statements, *, exact_case: bool = False):
            return session.execute_spark_sql_batch(
                statements, exact_case=exact_case, workspace=workspace
            )

        return run

    def spark_sql_actions(self):
        """Run labelled Spark actions in one Session-owned submission."""

        session = self.session
        workspace = self.workspace

        def run(actions, *, exact_case: bool = False):
            return session.execute_spark_sql_actions(
                actions, exact_case=exact_case, workspace=workspace
            )

        return run

    def spark_query_shapes(self):
        session = self.session
        workspace = self.workspace

        def describe(actions):
            return session.describe_spark_query_actions(actions, workspace=workspace)

        return describe

    def delta_table_creator(self):
        session = self.session
        workspace = self.workspace

        def create(
            qualified_name,
            columns,
            *,
            identity_column=None,
            column_mapping=True,
            protocol_minima: ProtocolMinima | None = None,
        ):
            options: ProtocolOptions = (
                {"protocol_minima": protocol_minima}
                if protocol_minima is not None
                else {}
            )
            return session.create_delta_table(
                qualified_name,
                columns,
                identity_column=identity_column,
                column_mapping=column_mapping,
                workspace=workspace,
                **options,
            )

        return create

    def delta_table_actions(self):
        session = self.session
        workspace = self.workspace

        def create(actions):
            return session.create_delta_table_actions(actions, workspace=workspace)

        return create

    def direct_delta_table_creator(self):
        session = self.session
        workspace = self.workspace

        def create(
            qualified_name,
            columns,
            *,
            identity_column=None,
            protocol_minima: ProtocolMinima | None = None,
        ):
            options: ProtocolOptions = (
                {"protocol_minima": protocol_minima}
                if protocol_minima is not None
                else {}
            )
            return session.create_direct_delta_table(
                qualified_name,
                columns,
                identity_column=identity_column,
                workspace=workspace,
                **options,
            )

        return create

    def direct_delta_table_actions(self):
        session = self.session
        workspace = self.workspace

        def create(actions):
            return session.create_direct_delta_table_actions(
                actions, workspace=workspace
            )

        return create

    def sql_for(self, bound: BoundTarget) -> Any:
        """Return a deferred Warehouse connection, or ``None`` for a Lakehouse."""

        if bound.kind != WAREHOUSE_TARGET:
            return None
        from ..targets import WarehouseTarget

        return _Deferred(
            lambda: self.session.sql_executor(
                WarehouseTarget(ItemRef(bound.item_id)), workspace=self.workspace
            )
        )

    def semantic_model(self, bound):
        if bound.kind != "semanticmodel":
            raise InstallError(f"{bound.display} is not a SemanticModel")
        resolved = self.session.resolve_item(
            bound.name, item_type="SemanticModel", workspace=self.workspace
        )
        if (
            bound.workspace_id is not None
            and resolved.workspace_id != bound.workspace_id
        ):
            raise InstallError(f"{bound.display} resolved in a different workspace")
        if bound.item_id != bound.name and resolved.id != bound.item_id:
            raise InstallError(
                f"{bound.display} has a different item ID; regenerate the bundle"
            )
        return self.session.semantic_model(bound.name, workspace=self.workspace)

    def report_item(self, bound):
        if bound.kind != "report":
            raise InstallError(f"{bound.display} is not a Report")
        resolved = self.session.resolve_item(
            bound.name, item_type="Report", workspace=self.workspace
        )
        if resolved.workspace_id != bound.workspace_id or resolved.id != bound.item_id:
            raise InstallError(
                f"{bound.display} has a different physical binding; regenerate the bundle"
            )
        return self.session.report_item(bound.name, workspace=self.workspace)

    def resolve_target(self, bound: BoundTarget) -> ResolvedTarget:
        # Resolve once so executors never derive paths or inherit a Lakehouse.
        item = ItemRef(bound.item_id)
        return ResolvedTarget(
            bound=bound,
            lakehouse=item,
            location=self._resolved(bound, item, "lakehouse_spark_location"),
            destination=self._resolved(bound, item, "spark_destination"),
        )

    def _resolved(self, bound: BoundTarget, item: ItemRef, method: str):
        """Resolve a Lakehouse address; Warehouse actions use TDS instead."""

        if bound.kind != "lakehouse":
            return None
        resolve = getattr(self.resolver, method, None)
        if resolve is None:
            return None
        return resolve(item)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def execute_install_action(
    action: InstallAction,
    payload: bytes | None = None,
    *,
    context: InstallationContext,
    executors: Mapping[str, ActionExecutor] | None = None,
) -> ActionResult:
    """Execute one action; failures populate its ``ActionResult``."""

    return _execute(
        action,
        lambda: payload,
        context=context,
        target_id=context.target.bound.id,
        executors=default_executors() if executors is None else executors,
    )


def _execute(
    action: InstallAction,
    load_payload,
    *,
    context: InstallationContext,
    target_id: str,
    executors: Mapping[str, ActionExecutor],
) -> ActionResult:
    """Run an action, including deferred payload reads, under one result boundary."""

    started = _now()
    executor = executors.get(action.executor)
    if executor is None:
        return _failed(
            action,
            target_id,
            started,
            InstallError(f"no executor named {action.executor!r}"),
        )

    try:
        payload = load_payload()
        if getattr(executor, "resumable", False):
            execution = executor.execute(action, payload, context, state=None)
            # A standalone action has nothing else to run, so it waits here.
            while isinstance(execution, Waiting):
                time.sleep(execution.delay)
                execution = executor.execute(
                    action, payload, context, state=execution.state
                )
        else:
            execution = executor.execute(action, payload, context)
    except Exception as exc:  # a failing action is data, not a crash
        return _failed(action, target_id, started, exc)

    finished = _now()
    skipped = isinstance(execution, SkippedExecution)
    return ActionResult(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        source_path=action.source_path,
        target_id=target_id,
        executor=action.executor,
        status=SKIPPED if skipped else SUCCEEDED,
        started_at=started,
        finished_at=finished,
        duration_seconds=(finished - started).total_seconds(),
        details=execution.details if skipped else (execution or None),
    )


def _failed(
    action: InstallAction, target_id: str, started: datetime, exc: Exception
) -> ActionResult:
    finished = _now()
    return ActionResult(
        action_id=action.id,
        resource_node_id=action.resource_node_id,
        source_path=action.source_path,
        target_id=target_id,
        executor=action.executor,
        status=FAILED,
        started_at=started,
        finished_at=finished,
        duration_seconds=(finished - started).total_seconds(),
        error_type=type(exc).__name__,
        error_message=str(exc),
    )
