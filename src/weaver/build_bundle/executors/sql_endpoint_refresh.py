"""Refresh a Lakehouse SQL analytics endpoint as a typed operation.

Start returns once Fabric accepts the refresh. Await consumes that handle and
stays ``Pending`` until the endpoint is known current, so unrelated work runs
while Fabric syncs metadata. One start syncs every table; the other syncs the
tables its payload names.
"""

from __future__ import annotations

import json
import time

from ...mutation.models import DriverContract

START_EXECUTOR = "start_sql_endpoint_refresh"
START_TABLES_EXECUTOR = "start_sql_endpoint_table_refresh"
TABLES_EXTENSION = ".endpoint-tables.json"
AWAIT_EXECUTOR = "await_sql_endpoint_refresh"
REFRESH_RESULT = "sql_endpoint_refresh"
#: The minimum interval between observations when Fabric gives no Retry-After.
POLL_INTERVAL = 2.0
#: Fabric syncs each changed table in turn, about half a second apiece, so a
#: refresh after a build of about 1,900 objects outlasts the ten minutes an
#: action is otherwise allowed. Fabric is given the same allowance.
REFRESH_TIMEOUT = 1800.0

START_CONTRACT = DriverContract(
    START_EXECUTOR, None, produces=REFRESH_RESULT, starts_operation=True
)
START_TABLES_CONTRACT = DriverContract(
    START_TABLES_EXECUTOR,
    TABLES_EXTENSION,
    produces=REFRESH_RESULT,
    starts_operation=True,
)
AWAIT_CONTRACT = DriverContract(
    AWAIT_EXECUTOR, None, consumes=REFRESH_RESULT, settles_operation=True
)
CONTRACTS = (START_CONTRACT, START_TABLES_CONTRACT, AWAIT_CONTRACT)


def tables_payload(tables) -> bytes:
    """The payload naming the ``(schema, table)`` pairs a refresh syncs."""

    return (
        json.dumps({"tables": [list(each) for each in sorted(tables)]}, indent=2) + "\n"
    ).encode("utf-8")


_UNSUPPORTED = {
    "skipped": True,
    "reason": "SQL endpoint refresh is unsupported in this environment",
}


def endpoint_refresh_drivers(contexts, *, outcome, clock=time):
    """Bind the start and await drivers to resolved target contexts.

    ``outcome`` converts a Weaver error into the executor outcome.
    """

    from ...errors import WeaverError
    from ...mutation.executor import (
        Completed,
        MutationDriver,
        Pending,
        TypedValue,
    )

    contexts = dict(contexts)

    def start(request):
        context = contexts[request.action.target_id]
        begin = getattr(context.resolver, "start_sql_endpoint_refresh", None)
        tables = (
            None
            if request.payload is None
            else [tuple(each) for each in json.loads(request.payload)["tables"]]
        )
        try:
            refresh = (
                _UNSUPPORTED
                if begin is None
                else begin(
                    context.target.lakehouse, tables=tables, timeout=REFRESH_TIMEOUT
                )
            )
        except WeaverError as error:
            return outcome(error)
        return Completed(TypedValue(REFRESH_RESULT, refresh))

    def observe(request):
        refresh = request.continuation or request.input.value
        if refresh.get("skipped"):
            return Completed(dict(refresh))
        if not refresh["done"]:
            context = contexts[request.action.target_id]
            try:
                refresh = context.resolver.observe_sql_endpoint_refresh(refresh)
            except WeaverError as error:
                return outcome(error)
        if refresh["done"]:
            from ...fabric.resources import refresh_details

            return Completed(refresh_details(refresh))
        delay = max(refresh.get("retry_after") or 0, POLL_INTERVAL)
        return Pending(refresh, clock.monotonic() + delay)

    return {
        START_EXECUTOR: MutationDriver(
            start, contract=START_CONTRACT, timeout=REFRESH_TIMEOUT
        ),
        START_TABLES_EXECUTOR: MutationDriver(
            start, contract=START_TABLES_CONTRACT, timeout=REFRESH_TIMEOUT
        ),
        AWAIT_EXECUTOR: MutationDriver(observe, contract=AWAIT_CONTRACT),
    }
