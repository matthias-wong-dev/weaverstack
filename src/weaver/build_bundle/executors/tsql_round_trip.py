"""Send several ready T-SQL actions on one Warehouse in one round trip.

The SQL executor runs each action's statements as one group and reports each
group's own outcome, so the batch reports one outcome per action.
"""

from __future__ import annotations

import json

from ...tokens import substitute_build_datetime


def statements_of(action, payload: bytes, *, build_datetime) -> list[str]:
    """The statements one ``tsql`` or ``tsql_batch`` action runs, in order."""

    if action.executor == "tsql":
        return [payload.decode("utf-8")]
    # The publication instant is installation-scoped; see TSqlBatchExecutor.
    return [
        substitute_build_datetime(statement, build_datetime)
        for statement in json.loads(payload.decode("utf-8"))
    ]


def round_trip_driver(contexts, *, details):
    """A batch hook for ``tsql`` and ``tsql_batch`` drivers.

    ``details(action, payload)`` gives a succeeded action's result, as the
    single-action executor would.
    """

    from ...mutation.executor import Completed, Failed, Uncertain

    def batch(requests, emit):
        context = contexts[requests[0].action.target_id]
        members = [
            statements_of(
                request.action, request.payload, build_datetime=context.build_datetime
            )
            for request in requests
        ]
        try:
            outcomes = context.sql.execute_each(members)
        except Exception as error:  # noqa: BLE001 - which actions ran is unknown
            return [
                (request.action.id, Uncertain(f"{type(error).__name__}: {error}"))
                for request in requests
            ]
        return [
            (
                request.action.id,
                Completed(details(request.action, request.payload))
                if outcomes[position] is None
                else Failed(outcomes[position]),
            )
            for position, request in enumerate(requests)
        ]

    return batch
