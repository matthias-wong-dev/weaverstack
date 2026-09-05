"""Check workspace connectivity through the Session's Fabric clients."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from ..errors import WeaverError

OK = "ok"
MISSING = "missing"
FAILED = "failed"
ERROR = "error"
LIVY_PROBE = "SELECT 1"
TDS_PROBE = "SELECT 1"


@dataclass(frozen=True)
class Check:
    """One probe and the physical item used to perform it."""

    name: str
    status: str
    detail: str | None = None
    remedy: str | None = None
    via: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == OK

    def to_mapping(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DoctorReport:
    """Ordered connectivity evidence for one named workspace."""

    checks: tuple[Check, ...] = ()
    workspace: str | None = None
    authentication: dict = field(default_factory=dict)
    workspaces: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return (
            bool(self.checks)
            and not self.failures
            and any(
                check.name.startswith("Workspace ") and check.passed
                for check in self.checks
            )
        )

    @property
    def failures(self) -> tuple[Check, ...]:
        return tuple(
            check
            for check in self.checks
            if check.status in (FAILED, ERROR)
            or check.name.startswith("Workspace ")
            and not check.passed
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace,
            "authentication": self.authentication,
            "workspaces": list(self.workspaces),
            "succeeded": self.succeeded,
            "checks": [check.to_mapping() for check in self.checks],
        }


def doctor(*, workspace: str, session=None, client=None) -> DoctorReport:
    """Prove authentication, REST, OneLake, TDS and Spark in a named workspace."""

    from ..sessions.host import use_or_create_session
    from ..targets import ItemRef, WarehouseTarget
    from ..workspaces import Workspace

    configured = Workspace(workspace=workspace)
    checks = []
    authentication = {}
    visible = ()

    def report():
        return DoctorReport(tuple(checks), workspace, authentication, visible)

    with use_or_create_session(session, workspace=configured) as opened:
        try:
            rest = client if client is not None else opened.resolver(configured).client
            authentication = rest.authenticate()
            checks.append(Check("Authentication", OK, authentication.get("path")))
        except Exception as exc:
            checks.append(
                Check(
                    "Authentication",
                    ERROR,
                    type(exc).__name__,
                    "Sign in again and retry.",
                )
            )
            return report()
        try:
            visible = tuple(rest.paged("workspaces"))
            checks.append(
                Check(
                    "Fabric REST",
                    OK if visible else FAILED,
                    f"{len(visible)} workspaces visible"
                    if visible
                    else "Fabric responded, but this identity cannot see any workspaces.",
                )
            )
        except Exception as exc:
            checks.append(Check("Fabric REST", ERROR, str(exc)))
            return report()
        if not visible:
            return report()
        if not any(item.get("displayName") == workspace for item in visible):
            checks.append(
                Check(
                    f"Workspace {workspace}",
                    MISSING,
                    f"This identity cannot see {workspace}.",
                )
            )
            return report()
        try:
            resolver = opened.resolver(configured)
            discovered = resolver.discover(workspaces=visible, client=rest)
            checks.append(Check(f"Workspace {workspace}", OK))
            items = sorted(
                discovered, key=lambda item: (item.name.casefold(), item.name, item.id)
            )
        except Exception as exc:
            checks.append(Check("Workspace discovery", ERROR, str(exc)))
            return report()
        lakehouse = next((item for item in items if item.type == "Lakehouse"), None)
        warehouse = next((item for item in items if item.type == "Warehouse"), None)
        if lakehouse:
            item = ItemRef(lakehouse.name)

            def read_files():
                root = opened.resolver(configured).files_root(item)
                return opened.store(configured).exists(root)

            checks.append(_attempt("OneLake", read_files, via=f"Lakehouse/{item.name}"))
        else:
            checks.append(
                Check(
                    "OneLake",
                    MISSING,
                    "No Lakehouse exists in this workspace to test OneLake.",
                )
            )
        if warehouse:
            checks.append(
                _attempt(
                    "Warehouse TDS",
                    lambda: opened.query_tsql(
                        TDS_PROBE,
                        target=WarehouseTarget(ItemRef(warehouse.name)),
                        workspace=configured,
                    ),
                    via=f"Warehouse/{warehouse.name}",
                )
            )
        else:
            checks.append(
                Check(
                    "Warehouse TDS",
                    MISSING,
                    "No Warehouse exists in this workspace to test TDS.",
                )
            )
        if lakehouse:

            def spark():
                opened.offer_spark_home((lakehouse.name,), workspace=configured)
                return opened.execute_spark_sql(LIVY_PROBE, workspace=configured)

            checks.append(
                _attempt(
                    "Fabric Spark / Livy", spark, via=f"Lakehouse/{lakehouse.name}"
                )
            )
        else:
            checks.append(
                Check(
                    "Fabric Spark / Livy",
                    MISSING,
                    "No Lakehouse exists in this workspace to start a Fabric Spark session.",
                )
            )
    return report()


def _attempt(name, work, *, via=None, remedy=None):
    """Distinguish rejected probes from transport and runtime errors."""

    from ..fabric.client import FabricError
    from ..fabric.livy import LivyError, LivyStatementError
    from ..sql.errors import SqlConnectionError, SqlPoolClosedError

    try:
        result = work()
        if result is False:
            return Check(
                name, FAILED, "The probe returned a negative result.", remedy, via
            )
    except FabricError as exc:
        status = FAILED if exc.status_code in (400, 401, 403, 404, 409) else ERROR
        return Check(name, status, str(exc), remedy, via)
    except LivyStatementError as exc:
        return Check(name, FAILED, str(exc), remedy, via)
    except (LivyError, SqlConnectionError, SqlPoolClosedError) as exc:
        return Check(name, ERROR, str(exc), remedy, via)
    except (WeaverError, PermissionError) as exc:
        return Check(name, FAILED, str(exc), remedy, via)
    except Exception as exc:
        return Check(name, ERROR, f"{type(exc).__name__}: {exc}", remedy, via)
    return Check(name, OK, via=via)
