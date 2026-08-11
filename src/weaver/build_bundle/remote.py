"""The far side of a decomposed install: one action, run where Spark is.

Most of what an install does needs no Spark. Files go to OneLake, T-SQL goes to
TDS, shortcuts and endpoint refreshes are REST — and every one of those reaches
Fabric from a desktop directly. What is left is the Lakehouse work that is
genuinely Spark: schemas, views, tables, and the read probe that proves an alias
is addressable.

So the Installer runs on the desktop and only those actions cross:

.. code-block:: text

    desktop Installer
      ├── write_file          → OneLake
      ├── build_procedure     → TDS
      ├── refresh_sql_endpoint→ REST
      └── build_table (Spark) → install_actions(...) ─► Fabric session

**One entry point, not one per executor.** The remote surface is a place where
the desktop and the published wheel have to agree, and every symbol added to it
is another thing that can be out of step. So this takes an action and runs
whichever executor the bundle already named, rather than growing a remote API
shaped like the executor registry.

**Nothing is re-decided here.** The action, its payload and its target are all
frozen in the bundle; this reconstructs them and calls the same executor an
in-session install calls. What crosses is a decision that has already been made.

Payloads cross with their action, and they are DDL — a statement or a small
JSON instruction. The payloads that are *bulk*, the deployed Python tree, are
written straight to OneLake by an executor that needs no Spark and therefore
never comes here. That is the point of routing by capability rather than
shipping an archive: the big bytes take the short path.
"""

from __future__ import annotations

import base64
import time


def install_actions(
    *,
    actions: list,
    target: dict,
    targets: list,
    epoch: str | None = None,
    session=None,
    workspace=None,
) -> list:
    """Run a batch's Spark actions here, in order, and report each one.

    A list rather than one action, because measurement said so. Crossing per
    action cost about four seconds of submission overhead apiece — six actions
    in a small estate paid twenty-four seconds of pure transport, which was most
    of a regression against shipping an archive. The plan's own answer: keep
    InstallAction as the semantic unit and let the executor layer batch the
    physical effects, rather than forcing one action to mean one network call.

    Each entry is ``{"action": mapping, "payload": base64 or None}``. Every one
    is reported, including after a failure, because they are independent units
    and a result that happened is worth more than a result rewritten as skipped.
    ``targets`` is every target the plan declared, because one action
    legitimately spans two of them — an alias points a name in its own target at
    an object in another.
    """

    from .executors import default_executors
    from .executors.base import InstallationContext, SkippedExecution
    from .installer import Installer
    from .models import InstallAction
    from .targets import BoundTarget

    if session is None:
        from ..session.host import session_for

        session = session_for(workspace)

    bound = BoundTarget.from_mapping(target)
    installer = Installer(session, workspace=workspace)
    resolved = {
        one.id: installer.resolve_target(one)
        for one in (BoundTarget.from_mapping(each) for each in targets)
    }
    context = InstallationContext(
        spark=installer.spark,
        resolver=installer.resolver,
        store=installer.store,
        target=resolved[bound.id],
        sql=installer.sql_for(bound),
        targets=resolved,
        epoch=epoch,
    )
    registry = default_executors()

    answers = []
    for entry in actions:
        frozen = InstallAction.from_mapping(entry["action"])
        payload = entry.get("payload")
        # Timed here, per action. The near side cannot see inside one
        # submission, so a batch that reported only its own duration would give
        # every action in it the whole batch's time — which is precisely the
        # kind of number the timing model exists to stop people believing.
        started = time.monotonic()
        try:
            executor = registry.get(frozen.executor)
            if executor is None:
                raise LookupError(f"no executor named {frozen.executor!r}")
            execution = executor.execute(
                frozen,
                None if payload is None else base64.b64decode(payload),
                context,
            )
        except Exception as exc:  # a failing action is data on the way back
            # Carried rather than raised: the near side records one result per
            # action with its own timing, and an exception here would lose every
            # answer this submission had already produced.
            answers.append(
                {
                    "id": frozen.id,
                    "seconds": time.monotonic() - started,
                    "failed": True,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                }
            )
            continue
        seconds = time.monotonic() - started
        if isinstance(execution, SkippedExecution):
            answers.append(
                {
                    "id": frozen.id,
                    "seconds": seconds,
                    "skipped": True,
                    "details": execution.details,
                }
            )
        else:
            answers.append(
                {
                    "id": frozen.id,
                    "seconds": seconds,
                    "skipped": False,
                    "details": execution or None,
                }
            )
    return answers


__all__ = ["install_actions"]
