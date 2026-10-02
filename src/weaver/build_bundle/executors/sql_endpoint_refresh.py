"""Refresh a Lakehouse SQL analytics endpoint.

Where something in the plan reads through the endpoint, the refresh is a typed
operation: start returns once Fabric accepts it, and await stays ``Pending``
until the endpoint is current. Otherwise one request asks Fabric to refresh and
finishes once Fabric accepts, and the endpoint catches up after the Build.
"""

from __future__ import annotations

import time

from ...mutation.models import DriverContract

REQUEST_EXECUTOR = "request_sql_endpoint_refresh"
START_EXECUTOR = "start_sql_endpoint_refresh"
AWAIT_EXECUTOR = "await_sql_endpoint_refresh"
REFRESH_RESULT = "sql_endpoint_refresh"
#: The minimum interval between observations when Fabric gives no Retry-After.
POLL_INTERVAL = 2.0

START_CONTRACT = DriverContract(
    START_EXECUTOR, None, produces=REFRESH_RESULT, starts_operation=True
)
AWAIT_CONTRACT = DriverContract(
    AWAIT_EXECUTOR, None, consumes=REFRESH_RESULT, settles_operation=True
)
CONTRACTS = (START_CONTRACT, AWAIT_CONTRACT)

_UNSUPPORTED = {
    "skipped": True,
    "reason": "SQL endpoint refresh is unsupported in this environment",
}


class SqlEndpointRefreshRequestExecutor:
    name = REQUEST_EXECUTOR

    def execute(self, action, payload, context):
        begin = getattr(context.resolver, "start_sql_endpoint_refresh", None)
        if begin is None:
            return dict(_UNSUPPORTED)
        from ...fabric.resources import refresh_details

        return refresh_details(begin(context.target.lakehouse))


def endpoint_refresh_drivers(contexts, *, failed, clock=time):
    """Bind the start and await drivers to resolved target contexts.

    ``failed`` converts a known Weaver failure into the executor outcome.
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
        try:
            refresh = _UNSUPPORTED if begin is None else begin(context.target.lakehouse)
        except WeaverError as error:
            return failed(error)
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
                return failed(error)
        if refresh["done"]:
            from ...fabric.resources import refresh_details

            return Completed(refresh_details(refresh))
        delay = max(refresh.get("retry_after") or 0, POLL_INTERVAL)
        return Pending(refresh, clock.monotonic() + delay)

    return {
        START_EXECUTOR: MutationDriver(start, contract=START_CONTRACT),
        AWAIT_EXECUTOR: MutationDriver(observe, contract=AWAIT_CONTRACT),
    }
