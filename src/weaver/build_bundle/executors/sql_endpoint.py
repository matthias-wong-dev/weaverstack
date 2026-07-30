"""Syncing one Lakehouse's SQL analytics endpoint with its Delta tables.

The action carries no payload because there is nothing to freeze: it names the
target through its batch, and "catch up with what just happened here" is the whole
instruction. What it *guards* is described in
:mod:`weaver.build_bundle.endpoints` — everything that reads a Lakehouse as SQL
reads this endpoint, and Fabric synchronises it behind the mutation rather than
with it.

The local emulator has no such endpoint. It is skipped there, explicitly and in
the report, rather than being given an invented local equivalent: a step that
claimed to have refreshed something that does not exist would make the local suite
stop testing the thing that actually matters.
"""

from __future__ import annotations

from typing import Any

from ..models import BuildAction
from .base import InstallationContext


class SqlEndpointExecutor:
    name = "sql_endpoint"

    def execute(
        self,
        action: BuildAction,
        payload: bytes | None,
        context: InstallationContext,
    ) -> dict[str, Any] | None:
        refresh = getattr(context.resolver, "refresh_sql_endpoint_metadata", None)
        if refresh is None:
            return {
                "skipped": (
                    f"{context.target.bound.name} has no SQL analytics endpoint in "
                    "this environment"
                )
            }
        return refresh(context.target.lakehouse)
