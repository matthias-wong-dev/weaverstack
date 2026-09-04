"""Whether Weaver can reach the Fabric surfaces a project uses.

A connectivity check and nothing else. It answers "can I get there", never "is
the estate healthy", which is `weaver.health`, and never "does my repository
parse", which is `weaver.operations.check`.

What it can check depends on what it was given. With no workspace it proves
sign-in and the Fabric REST API by listing workspaces. With a workspace it
resolves that too. With workspace configuration it knows the items, so it opens
TDS to each Warehouse, reads each Lakehouse's Files over OneLake, and runs one
Spark SQL statement through Livy where a Lakehouse is configured.

Each check is a name, an outcome and, where it failed, the reason and what to do
about it. Nothing here prints.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..errors import WeaverError

#: What one check found.
OK = "ok"
FAILED = "failed"

#: The Spark statement the Livy check runs. Cheap, and it proves a session.
LIVY_PROBE = "select 1"

#: The T-SQL a Warehouse answers to prove its endpoint is reachable.
TDS_PROBE = "select 1"


@dataclass(frozen=True)
class Check:
    """One crossing, and whether Weaver made it."""

    name: str
    status: str
    detail: str | None = None
    remedy: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == OK

    def to_mapping(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "remedy": self.remedy,
        }


@dataclass(frozen=True)
class DoctorReport:
    """Every check this run made, in the order it made them."""

    checks: tuple[Check, ...] = field(default_factory=tuple)
    workspace: str | None = None

    @property
    def succeeded(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failures(self) -> tuple[Check, ...]:
        return tuple(check for check in self.checks if not check.passed)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace,
            "succeeded": self.succeeded,
            "checks": [check.to_mapping() for check in self.checks],
        }


def doctor(
    *,
    workspace: str | None = None,
    workspace_config=None,
    catalogue: str | None = None,
    session=None,
    client=None,
) -> DoctorReport:
    """Check that Weaver can reach Microsoft Fabric, and what a project uses.

    ``workspace`` and ``workspace_config`` say how much can be checked. Naming
    neither proves sign-in and the Fabric REST API; naming a workspace resolves
    it as well; naming a configuration adds the endpoints its items are reached
    through.
    """

    checks = [_rest_check(client=client)]
    if not checks[0].passed:
        return DoctorReport(checks=tuple(checks))

    configured = _configured(workspace, workspace_config, catalogue, session)
    if configured is None:
        return DoctorReport(checks=tuple(checks))

    checks.append(_workspace_check(configured, client=client))
    if not checks[-1].passed:
        return DoctorReport(checks=tuple(checks), workspace=configured.workspace)

    checks.extend(_endpoint_checks(configured, session=session))
    return DoctorReport(checks=tuple(checks), workspace=configured.workspace)


def _configured(workspace, workspace_config, catalogue, session):
    """The workspace this run checks, or None where nothing named one.

    The order every operation resolves in: an explicit name, then a
    configuration file, then the Session this command is running inside, then
    `workspace-config.yml` in the working directory. Naming nothing anywhere is
    a state, and what can still be proven is Fabric REST.
    """

    from ..config import discovered_workspace_config, resolve_workspace

    if workspace is None and workspace_config is None:
        inherited = getattr(session, "workspace", None)
        if inherited is not None:
            if catalogue is None or inherited.catalogue == catalogue:
                return inherited
            from dataclasses import replace

            return replace(inherited, catalogue=catalogue)
        workspace_config = discovered_workspace_config()
        if workspace_config is None:
            return None
    return resolve_workspace(
        workspace=workspace, catalogue=catalogue, workspace_config=workspace_config
    )


def _rest_check(*, client) -> Check:
    """Sign-in and the Fabric REST API, proven by listing workspaces.

    One call answers both: a listing that comes back means a token was issued
    and the control plane accepted it.
    """

    def listing():
        from ..fabric.client import FabricClient

        (client or FabricClient()).paged("workspaces")

    return _attempt(
        "Fabric REST",
        listing,
        remedy=(
            "Check that you are signed in and that this machine can reach\n"
            "api.fabric.microsoft.com."
        ),
    )


def _workspace_check(configured, *, client) -> Check:
    """The named workspace, resolved to a Fabric item."""

    def resolve():
        from ..fabric.resources import find_workspace

        find_workspace(configured.workspace, client=client)

    return _attempt(
        f"Workspace {configured.workspace}",
        resolve,
        remedy="Check the name, and that your account has access to it.",
    )


def _endpoint_checks(configured, *, session) -> list[Check]:
    """The endpoints this project's own items are reached through.

    A Warehouse is TDS. A Lakehouse is OneLake for its files and Livy for the
    Spark its authored Python runs on. The catalogue is a Warehouse, so a
    Lakehouse-only project still reaches TDS.
    """

    from ..sessions.host import use_or_create_session
    from ..targets import ItemRef

    warehouses, lakehouses = _items(configured)
    checks: list[Check] = []
    with use_or_create_session(session, workspace=configured) as opened:
        for name in warehouses:
            checks.append(_tds_check(opened, configured, name))
        for name in lakehouses:
            checks.append(_onelake_check(opened, configured, ItemRef(name)))
        if lakehouses:
            checks.append(_livy_check(opened, configured))
    return checks


def _items(configured) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The Warehouses and Lakehouses this configuration names, catalogue first."""

    from ..declaration.model import LAKEHOUSE, WAREHOUSE

    warehouses = []
    lakehouses = []
    if configured.catalogue:
        warehouses.append(configured.catalogue_item.name)
    for item, declaration in configured.targets.items():
        if item.item_type == WAREHOUSE and declaration.physical not in warehouses:
            warehouses.append(declaration.physical)
        elif item.item_type == LAKEHOUSE and declaration.physical not in lakehouses:
            lakehouses.append(declaration.physical)
    return tuple(warehouses), tuple(lakehouses)


def _tds_check(session, configured, name: str) -> Check:
    """One Warehouse, asked a question over TDS."""

    from ..targets import ItemRef, WarehouseTarget

    def query():
        session.query_tsql(
            TDS_PROBE, target=WarehouseTarget(ItemRef(name)), workspace=configured
        )

    return _attempt(
        f"Warehouse/{name} TDS",
        query,
        remedy=(
            "Check that the Warehouse exists and that its SQL endpoint has\n"
            "finished provisioning."
        ),
    )


def _onelake_check(session, configured, item) -> Check:
    """One Lakehouse's Files area, read over OneLake."""

    def read():
        resolver = session.resolver(configured)
        session.store(configured).exists(resolver.files_root(item))

    return _attempt(
        f"Lakehouse/{item.name} OneLake",
        read,
        remedy="Check that the Lakehouse exists and that you can read its files.",
    )


def _livy_check(session, configured) -> Check:
    """One Spark statement, which is what starting a session amounts to proving."""

    def run():
        session.execute_spark_sql(LIVY_PROBE, workspace=configured)

    return _attempt(
        "Spark session",
        run,
        remedy=(
            "Check that the capacity is running and that the Environment named\n"
            "in workspace configuration has been published."
        ),
    )


def _attempt(name: str, work, *, remedy: str) -> Check:
    """Run one crossing, and turn whatever it raised into a reason.

    Every failure is reported, including one from a library Weaver does not
    own: a connectivity check that let a transport error escape would say
    nothing about the checks after it.
    """

    try:
        work()
    except WeaverError as exc:
        return Check(name, FAILED, detail=str(exc), remedy=remedy)
    except Exception as exc:
        return Check(name, FAILED, detail=f"{type(exc).__name__}: {exc}", remedy=remedy)
    return Check(name, OK)
