"""Execute a persisted Build plan and present its physical action results."""

from collections.abc import Mapping
from datetime import datetime, timezone

from ..mutation.executor import TypedValue, validate_inputs
from .report import ActionResult, InstallationReport, SequenceResult


def execute_bundle(bundle, session, *, executors=None, build_datetime=None):
    payloads = {
        a.payload: bundle.store.read(bundle.location.join(*a.payload.split("/")))
        for _, _, a in bundle.plan.actions()
        if a.payload is not None
    }
    validate_inputs(bundle.plan, payloads)
    started = datetime.now(timezone.utc)
    options = {"build_datetime": build_datetime}
    if executors is not None:
        options["executors"] = executors
    report = session.execute_mutation(bundle.plan, payloads, **options)
    finished = datetime.now(timezone.utc)
    sequences = []
    for sequence in bundle.plan.sequences:
        actions = []
        for batch in sequence.batches:
            for action in batch.actions:
                if action.executor == "completion_gate":
                    continue
                result = report.by_id[action.id]
                value = result.value
                if isinstance(value, TypedValue):
                    value = value.value
                status = (
                    "skipped"
                    if isinstance(value, Mapping) and value.get("skipped") is True
                    else result.status
                    if result.status in {"succeeded", "failed"}
                    else ("failed" if result.status == "uncertain" else "skipped")
                )
                duration = result.active_seconds + result.wait_seconds
                actions.append(
                    ActionResult(
                        action_id=action.id,
                        resource_node_id=action.resource_node_id,
                        source_path=action.source_path,
                        target_id=action.target_id,
                        executor=action.executor,
                        status=status,
                        started_at=None,
                        finished_at=None,
                        duration_seconds=duration,
                        error_type=(
                            "UncertainMutation"
                            if result.status == "uncertain"
                            else "MutationFailure"
                        )
                        if result.error
                        else None,
                        error_message=result.error,
                        details=dict(value) if isinstance(value, Mapping) else None,
                    )
                )
        if actions:
            status = (
                "failed"
                if any(a.status == "failed" for a in actions)
                else (
                    "skipped"
                    if all(a.status == "skipped" for a in actions)
                    else "succeeded"
                )
            )
            sequences.append(
                SequenceResult(
                    sequence.number, sequence.description, status, tuple(actions)
                )
            )
    presented = InstallationReport(
        bundle.bundle_id,
        "succeeded" if report.succeeded else "failed",
        started,
        finished,
        tuple(sequences),
    )
    bundle.store.write(
        bundle.location / "install-report.yml", presented.to_yaml().encode("utf-8")
    )
    return presented
