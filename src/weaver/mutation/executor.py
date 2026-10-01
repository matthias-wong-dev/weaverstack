"""Internal execution of frozen physical actions."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass, replace
from math import isfinite
from queue import Empty, Queue
from typing import Any, Callable, Mapping
from uuid import uuid4

from .models import DriverContract, MutationAction, MutationPlan


@dataclass(frozen=True)
class TypedValue:
    result_type: str
    value: Any


@dataclass(frozen=True)
class Completed:
    value: Any = None


@dataclass(frozen=True)
class Failed:
    error: str


@dataclass(frozen=True)
class Uncertain:
    error: str


@dataclass(frozen=True)
class Pending:
    continuation: Any
    next_poll_at: float


def _valid_outcome(outcome):
    if isinstance(outcome, Completed):
        return True
    if isinstance(outcome, (Failed, Uncertain)):
        return isinstance(outcome.error, str) and bool(outcome.error.strip())
    if isinstance(outcome, Pending):
        return (
            outcome.continuation is not None
            and type(outcome.next_poll_at) in {int, float}
            and isfinite(outcome.next_poll_at)
        )
    return False


@dataclass(frozen=True)
class DriverRequest:
    action: MutationAction
    payload: bytes | None
    continuation: Any = None
    resource_keys: tuple[str, ...] = ()
    deadline: float = float("inf")
    input: TypedValue | None = None
    cancelling: bool = False


@dataclass(frozen=True)
class MutationDriver:
    """Run one action or resume its opaque continuation.

    Batch hooks emit known member outcomes before returning the remaining pairs.
    Each hook owns its inner work and returns only after that work drains.
    """

    run: Callable[[DriverRequest], Completed | Pending | Failed | Uncertain]
    contract: DriverContract | None = None
    batch: Callable | None = None
    batch_size: int = 1
    resources: tuple[str, ...] = ()
    preflight: Callable | None = None
    serial_resources: tuple[str, ...] = ()
    cancel: Callable | None = None


@dataclass(frozen=True)
class LedgerEvent:
    kind: str
    action_id: str
    at: float
    value: Any = None
    plan_id: str = ""
    invocation_id: str = ""


@dataclass(frozen=True)
class Operation:
    action_id: str
    handle: TypedValue
    deadline: float
    exclusions: tuple[str, ...]
    status: str = "acknowledged"


@dataclass(frozen=True)
class MutationResult:
    """Invocation timings are local observations.

    Wait time spans Pending observations. Resource contention may overlap that
    span. Ready-queue time measures the first admission, after prerequisites.
    """

    action_id: str
    status: str
    value: Any = None
    attempts: int = 0
    observations: int = 0
    error: str | None = None
    active_seconds: float = 0
    wait_seconds: float = 0
    ready_queue_seconds: float = 0
    resource_queue_seconds: float = 0


@dataclass(frozen=True)
class MutationReport:
    plan_id: str
    results: tuple[MutationResult, ...]
    ledger: tuple[LedgerEvent, ...] = ()
    operations: tuple[Operation, ...] = ()
    invocation_id: str = ""
    journal_errors: tuple[str, ...] = ()
    retained_exclusions: tuple[tuple[str, str], ...] = ()

    @property
    def succeeded(self):
        return (
            not self.journal_errors
            and all(r.status == "succeeded" for r in self.results)
            and all(op.status == "settled" for op in self.operations)
        )

    @property
    def by_id(self):
        return {r.action_id: r for r in self.results}


@dataclass
class _State:
    action: MutationAction
    pending: Pending | None = None
    attempts: int = 0
    observations: int = 0
    deadline: float | None = None
    ready_at: float = 0
    wait_at: float | None = None
    active_seconds: float = 0
    wait_seconds: float = 0
    ready_queue_seconds: float = 0
    resource_queue_seconds: float = 0
    resource_at: float | None = None
    cancelling: bool = False


def validate_inputs(plan, payloads):
    from hashlib import sha256

    from ..errors import BuildError
    from .validation import validate_mutation_plan

    if not isinstance(plan, MutationPlan):
        raise BuildError("MutationExecutor requires a MutationPlan")
    validate_mutation_plan(plan)
    if not plan.bundle_id:
        raise BuildError("mutation execution requires a sealed identity")
    payloads = dict(payloads or {})
    referenced = {a.payload for _, _, a in plan.actions() if a.payload is not None}
    if set(payloads) != referenced:
        raise BuildError("mutation payload inventory does not match the plan")
    for _, _, action in plan.actions():
        if action.payload is not None:
            data = payloads[action.payload]
            if (
                not isinstance(data, bytes)
                or sha256(data).hexdigest() != action.payload_sha256
            ):
                raise BuildError(f"invalid payload for action {action.id!r}")
    return payloads


class MutationExecutor:
    def __init__(
        self,
        drivers: Mapping[str, MutationDriver],
        *,
        workers=1,
        clock=time,
        limits=None,
        timeout=600,
        journal=None,
        failure_policy="continue_independent",
    ):
        from ..errors import BuildError

        if type(workers) is not int or workers < 1:
            raise BuildError("workers must be a positive integer")
        if type(timeout) not in {int, float} or not isfinite(timeout) or timeout <= 0:
            raise BuildError("timeout must be finite and positive")
        if any(type(v) is not int or v < 1 for v in (limits or {}).values()):
            raise BuildError("resource limits must be positive integers")
        if failure_policy not in {"continue_independent", "fail_fast"}:
            raise BuildError("unsupported mutation failure policy")
        self.drivers = dict(drivers)
        self.workers = workers
        self.clock = clock
        self.limits = dict(limits or {})
        self.timeout = timeout
        self.journal = journal
        self.failure_policy = failure_policy

    def execute(
        self,
        plan: MutationPlan,
        payloads: Mapping[str, bytes] | None = None,
        *,
        cancellation=None,
        invocation_id=None,
    ):
        from ..errors import BuildError

        payloads = validate_inputs(plan, payloads)
        selected = tuple(a.id for _, _, a in plan.actions())
        external = {}
        if invocation_id is not None and (
            not isinstance(invocation_id, str) or not invocation_id
        ):
            raise BuildError("invalid mutation invocation identity")
        contracts = {c.executor: c for c in plan.driver_contracts}
        for _, _, action in plan.actions():
            if action.id not in selected:
                continue
            if action.executor == "completion_gate":
                if action.executor in self.drivers:
                    raise BuildError("completion_gate cannot be overridden")
                if any(key not in self.limits for key in action.resources):
                    raise BuildError(f"undeclared execution resource for {action.id!r}")
                continue
            driver = self.drivers.get(action.executor)
            if driver is None or not callable(driver.run):
                raise BuildError(f"unsupported required driver {action.executor!r}")
            if driver.contract != contracts.get(action.executor):
                raise BuildError(f"driver contract mismatch for {action.executor!r}")
            if any(
                key not in self.limits for key in (*action.resources, *driver.resources)
            ):
                raise BuildError(f"undeclared execution resource for {action.id!r}")
            if any(self.limits[key] != 1 for key in driver.serial_resources):
                raise BuildError("shared execution lane requires a limit of one")
            if driver.preflight is not None:
                driver.preflight(
                    action, None if action.payload is None else payloads[action.payload]
                )
        return _Invocation(
            self, plan, payloads, cancellation, selected, external, invocation_id
        ).execute()


@dataclass
class _Task:
    states: tuple[_State, ...]
    keys: tuple[str, ...]
    batch: bool
    invalid: bool = False


class _Invocation:
    def __init__(
        self, executor, plan, payloads, cancellation, selected, external, invocation_id
    ):
        self.executor = executor
        self.invocation_id = invocation_id or uuid4().hex
        self.journal_errors = []
        self.plan = plan
        self.payloads = payloads
        self.cancellation = cancellation
        self.states = {
            a.id: _State(a, ready_at=self.now())
            for _, _, a in plan.actions()
            if a.id in selected
        }
        self.order = {key: i for i, key in enumerate(self.states)}
        self.contracts = {c.executor: c for c in plan.driver_contracts}
        self.results = dict(external)
        self.operations = {}
        self.ledger = []
        self.leases = {}
        self.operation_leases = {}
        self.used = {}
        self.running = {}
        self.active = set()
        self.events = Queue()
        self.ready = set()
        self.remaining = {}
        self.children = {key: [] for key in self.states}
        for key, state in self.states.items():
            action = state.action
            self.remaining[key] = sum(
                p in self.states for p in action.depends_on + action.settle_after
            )
            for parent in action.depends_on:
                if parent in self.states:
                    self.children[parent].append((key, True))
            for parent in action.settle_after:
                if parent in self.states:
                    self.children[parent].append((key, False))
            if not self.remaining[key]:
                self.ready.add(key)

    def now(self):
        return self.executor.clock.monotonic()

    def record(self, kind, state, value=None):
        event = LedgerEvent(
            kind,
            state.action.id,
            self.now(),
            value,
            self.plan.bundle_id,
            self.invocation_id,
        )
        if self.executor.journal is not None and not self.journal_errors:
            try:
                self.executor.journal(event)
            except Exception as exc:
                self.journal_errors.append(str(exc))
        self.ledger.append(event)
        return not self.journal_errors

    def flush_journal(self):
        if (
            isinstance(self.executor.journal, MutationJournal)
            and not self.journal_errors
        ):
            try:
                self.executor.journal.flush()
            except Exception as exc:
                self.journal_errors.append(str(exc))

    def terminal(self, state, status, value=None, error=None):
        key = state.action.id
        self.ready.discard(key)
        if state.wait_at is not None:
            state.wait_seconds += self.now() - state.wait_at
            state.wait_at = None
        result = MutationResult(
            key,
            status,
            value,
            state.attempts,
            state.observations,
            error,
            state.active_seconds,
            state.wait_seconds,
            state.ready_queue_seconds,
            state.resource_queue_seconds,
        )
        self.results[key] = result
        self.record("terminal", state, result)
        queue = [(key, status)]
        while queue:
            parent, outcome = queue.pop()
            for child, success in self.children[parent]:
                if child in self.results:
                    continue
                if success and outcome != "succeeded":
                    self.ready.discard(child)
                    blocked = MutationResult(
                        child, "blocked", error="unsuccessful prerequisite"
                    )
                    self.results[child] = blocked
                    self.record("terminal", self.states[child], blocked)
                    queue.append((child, "blocked"))
                elif success or outcome in {"succeeded", "failed", "blocked"}:
                    self.remaining[child] -= 1
                    if not self.remaining[child]:
                        self.states[child].ready_at = self.now()
                        self.ready.add(child)

    def finish(self, state, outcome):
        action = state.action
        contract = self.contracts.get(action.executor)
        if not _valid_outcome(outcome) or isinstance(outcome, Pending):
            outcome = Uncertain("invalid driver outcome")
        if isinstance(outcome, Completed) and contract and contract.produces:
            if (
                not isinstance(outcome.value, TypedValue)
                or outcome.value.result_type != contract.produces
            ):
                outcome = Uncertain("invalid typed driver result")
        if isinstance(outcome, Completed) and contract and contract.starts_operation:
            self.operations[action.id] = Operation(
                action.id, outcome.value, state.deadline, action.exclusions
            )
            if not self.record("acknowledged", state, self.operations[action.id]):
                self.operations[action.id] = replace(
                    self.operations[action.id], status="uncertain"
                )
                outcome = Uncertain("operation acknowledgement was not persisted")
        status = (
            "succeeded"
            if isinstance(outcome, Completed)
            else "failed"
            if isinstance(outcome, Failed)
            else "uncertain"
        )
        self.terminal(
            state,
            status,
            outcome.value if isinstance(outcome, Completed) else None,
            None if isinstance(outcome, Completed) else outcome.error,
        )
        owner = action.id
        if contract and contract.settles_operation:
            owner = action.result_from.action_id
            if self.operations[owner].status != "settled":
                self.operations[owner] = replace(
                    self.operations[owner],
                    status="settled"
                    if status in {"succeeded", "failed"}
                    else "uncertain",
                )
        retain = status == "uncertain" or (
            contract and contract.starts_operation and status == "succeeded"
        )
        if contract and contract.starts_operation and action.id in self.operations:
            for key in action.exclusions:
                self.operation_leases[key] = action.id
            retain = False
        if not retain:
            for key in list(self.leases):
                if self.leases[key] == action.id:
                    del self.leases[key]
        if (
            contract
            and contract.settles_operation
            and status in {"succeeded", "failed"}
        ):
            for key in list(self.operation_leases):
                if self.operation_leases[key] == owner:
                    del self.operation_leases[key]

    def owner(self, state):
        contract = self.contracts.get(state.action.executor)
        if contract and contract.settles_operation:
            return state.action.result_from.action_id
        return state.action.id

    def keys(self, state):
        driver = self.executor.drivers.get(state.action.executor)
        return tuple(
            sorted(
                set(state.action.resources)
                | set(() if driver is None else driver.resources)
            )
        )

    def available(self, state):
        action = state.action
        owner = self.owner(state)
        return not any(
            self.used.get(k, 0) >= self.executor.limits[k] for k in self.keys(state)
        ) and not any(
            (k in self.leases and self.leases[k] != action.id)
            or (k in self.operation_leases and self.operation_leases[k] != owner)
            for k in action.exclusions
        )

    def request(self, state):
        action = state.action
        owner = self.owner(state)
        if owner != action.id:
            state.deadline = self.operations[owner].deadline
        elif state.deadline is None:
            state.deadline = self.now() + self.executor.timeout
        if state.pending is None:
            state.ready_queue_seconds += self.now() - state.ready_at
            state.attempts += 1
            self.record("dispatched", state)
        else:
            state.wait_seconds += self.now() - state.wait_at
            state.wait_at = None
            state.observations += 1
            self.record("observed", state)
        if state.resource_at is not None:
            state.resource_queue_seconds += self.now() - state.resource_at
            state.resource_at = None
        return DriverRequest(
            action,
            None if action.payload is None else self.payloads[action.payload],
            None if state.pending is None else state.pending.continuation,
            self.keys(state),
            state.deadline,
            None
            if action.result_from is None
            else self.results[action.result_from.action_id].value,
            state.cancelling,
        )

    def stopped(self):
        return (
            bool(self.journal_errors)
            or (self.cancellation is not None and self.cancellation.is_set())
            or (
                self.executor.failure_policy == "fail_fast"
                and any(
                    r.status in {"failed", "uncertain"} for r in self.results.values()
                )
            )
        )

    def dispatch(self, pool):
        for key in sorted(self.ready, key=self.order.get):
            if len(self.running) >= self.executor.workers:
                break
            state = self.states[key]
            if self.stopped() and not state.cancelling:
                continue
            if key in self.active or key in self.results:
                continue
            owner = self.owner(state)
            if owner != key:
                state.deadline = self.operations[owner].deadline
            if state.deadline is not None and self.now() >= state.deadline:
                self.finish(state, Uncertain("operation deadline expired"))
                continue
            if state.pending and state.pending.next_poll_at > self.now():
                continue
            if not self.available(state):
                if state.resource_at is None:
                    state.resource_at = self.now()
                continue
            driver = self.executor.drivers.get(state.action.executor)
            group = [state]
            if (
                driver
                and driver.batch
                and driver.batch_size > 1
                and state.pending is None
                and not state.action.exclusions
                and state.action.executor not in self.contracts
            ):
                for other_key in sorted(self.ready, key=self.order.get):
                    other = self.states[other_key]
                    if (
                        other_key == key
                        or other_key in self.active
                        or other_key in self.results
                        or other.pending
                    ):
                        continue
                    if (
                        other.action.executor,
                        other.action.target_id,
                        other.action.resources,
                        other.action.exclusions,
                    ) == (
                        state.action.executor,
                        state.action.target_id,
                        state.action.resources,
                        (),
                    ):
                        group.append(other)
                        if len(group) >= driver.batch_size:
                            break
            keys = self.keys(state)
            task = _Task(tuple(group), keys, len(group) > 1)
            task_id = key
            requests = tuple(self.request(member) for member in group)
            self.flush_journal()
            if self.journal_errors:
                for member in group:
                    if member.pending is None:
                        self.record("admission_refused", member, self.journal_errors[0])
                        member.attempts = 0
                        self.terminal(
                            member,
                            "not_dispatched",
                            error="dispatch journal unavailable",
                        )
                    else:
                        self.finish(
                            member, Uncertain("observation journal unavailable")
                        )
                continue
            for member in group:
                self.active.add(member.action.id)
                for exclusion in member.action.exclusions:
                    self.leases[exclusion] = member.action.id
            for resource in keys:
                self.used[resource] = self.used.get(resource, 0) + 1
            self.running[task_id] = task
            pool.submit(
                copy_context().run, self.work, task_id, driver, requests, task.batch
            )

    def work(self, task_id, driver, requests, batch):
        started = self.now()
        try:
            if batch:
                value = driver.batch(
                    requests,
                    lambda key, outcome: self.events.put(
                        ("member", task_id, (key, outcome), started, self.now())
                    ),
                )
                value = list(value)
            else:
                if requests[0].cancelling:
                    value = driver.cancel(requests[0])
                    if isinstance(value, Pending):
                        value = Uncertain("cancellation remains unresolved")
                else:
                    value = Completed() if driver is None else driver.run(requests[0])
        except BaseException as exc:
            value = Uncertain(str(exc) or type(exc).__name__)
        self.events.put(("done", task_id, value, started, self.now()))

    def accept(self, state, outcome, started=None, ended=None):
        if started is not None:
            state.active_seconds += ended - started
        if self.now() >= state.deadline and not (
            isinstance(outcome, Failed) and _valid_outcome(outcome)
        ):
            contract = self.contracts.get(state.action.executor)
            if (
                isinstance(outcome, Completed)
                and contract
                and contract.starts_operation
                and isinstance(outcome.value, TypedValue)
                and outcome.value.result_type == contract.produces
            ):
                operation = Operation(
                    state.action.id,
                    outcome.value,
                    state.deadline,
                    state.action.exclusions,
                    "uncertain",
                )
                self.operations[state.action.id] = operation
                self.record("acknowledged", state, operation)
            outcome = Uncertain("operation deadline expired")
        if isinstance(outcome, Pending) and (
            outcome.continuation is None
            or type(outcome.next_poll_at) not in {int, float}
            or not isfinite(outcome.next_poll_at)
            or outcome.next_poll_at <= (self.now() if ended is None else ended)
        ):
            outcome = Uncertain("invalid Pending continuation")
        if isinstance(outcome, Pending):
            state.pending = outcome
            state.wait_at = self.now() if ended is None else ended
            self.record("pending", state, outcome)
        else:
            self.finish(state, outcome)

    def event(self, event):
        kind, task_id, value, started, ended = event
        task = self.running[task_id]
        members = {s.action.id: s for s in task.states}
        if kind == "member":
            key, outcome = value
            if (
                task.invalid
                or not isinstance(key, str)
                or key not in members
                or key in self.results
                or not _valid_outcome(outcome)
                or isinstance(outcome, Pending)
            ):
                task.invalid = True
                return
            self.accept(members[key], outcome, started, ended)
            return
        for resource in task.keys:
            self.used[resource] -= 1
        self.active.difference_update(members)
        del self.running[task_id]
        if not task.batch:
            self.accept(task.states[0], value, started, ended)
            return
        unresolved = {key for key in members if key not in self.results}
        try:
            outcomes = list(value)
            ids = [key for key, outcome in outcomes]
            valid = (
                not task.invalid
                and len(ids) == len(set(ids))
                and set(ids) == unresolved
                and all(
                    _valid_outcome(outcome)
                    and (
                        not isinstance(outcome, Pending) or outcome.next_poll_at > ended
                    )
                    for key, outcome in outcomes
                )
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            for key in sorted(unresolved, key=self.order.get):
                self.accept(
                    members[key],
                    Uncertain("invalid batch member outcomes"),
                    started,
                    ended,
                )
        else:
            for key, outcome in outcomes:
                self.accept(members[key], outcome, started, ended)

    def execute(self):
        with ThreadPoolExecutor(max_workers=self.executor.workers) as pool:
            while not self.states.keys() <= self.results.keys() or self.running:
                if self.stopped():
                    for key, state in self.states.items():
                        if key not in self.results and key not in self.active:
                            driver = self.executor.drivers.get(state.action.executor)
                            if (
                                state.pending
                                and driver
                                and driver.cancel
                                and not self.journal_errors
                            ):
                                state.cancelling = True
                                state.pending = replace(
                                    state.pending, next_poll_at=self.now()
                                )
                                continue
                            if state.attempts:
                                self.finish(
                                    state,
                                    Uncertain("cancelled with unresolved operation"),
                                )
                            else:
                                self.terminal(
                                    state, "not_dispatched", error="admission stopped"
                                )
                self.dispatch(pool)
                if self.running:
                    try:
                        self.event(self.events.get(timeout=0.01))
                        while True:
                            self.event(self.events.get_nowait())
                    except Empty:
                        pass
                    continue
                pending = [
                    s
                    for key, s in self.states.items()
                    if key not in self.results and s.pending
                ]
                if pending:
                    due = min(
                        min(s.pending.next_poll_at, s.deadline)
                        if s.pending.next_poll_at > self.now()
                        else s.deadline
                        for s in pending
                    )
                    delay = max(0, due - self.now())
                    if self.cancellation is not None:
                        delay = min(delay, 0.05)
                    if self.executor.clock is time and callable(
                        getattr(self.cancellation, "wait", None)
                    ):
                        self.cancellation.wait(delay)
                    else:
                        self.executor.clock.sleep(delay)
                elif not self.states.keys() <= self.results.keys():
                    for key, state in self.states.items():
                        if key not in self.results:
                            self.terminal(
                                state,
                                "not_dispatched",
                                error="unsettled prerequisite or exclusion",
                            )
        self.flush_journal()
        return MutationReport(
            self.plan.bundle_id,
            tuple(self.results[key] for key in self.states),
            tuple(self.ledger),
            tuple(
                self.operations[key] for key in self.states if key in self.operations
            ),
            self.invocation_id,
            tuple(self.journal_errors),
            tuple(
                sorted(set(self.leases.items()) | set(self.operation_leases.items()))
            ),
        )


class MutationJournal:
    """Checkpoint ordinary events and persist operation acknowledgements inline."""

    def __init__(self, write, *, checkpoint_size=64):
        self.write = write
        self.checkpoint_size = checkpoint_size
        self.buffer = []

    def __call__(self, event):
        self.buffer.append(event)
        if event.kind == "acknowledged" or len(self.buffer) >= self.checkpoint_size:
            self.flush()

    def flush(self):
        if self.buffer:
            self.write(tuple(self.buffer))
            self.buffer.clear()


def physical_driver(
    executor, contexts, *, lane, required_capabilities, allow_skipped=False
):
    """Bind existing executors to one owned runtime lane.

    Use the same lane for a shared Spark Session, TDS connection or inner pool.
    Contexts and capability requirements are supplied before dispatch.
    """
    from ..build_bundle.executors.base import SkippedExecution
    from ..errors import BuildError

    contexts = dict(contexts)
    capabilities = tuple(required_capabilities)

    def preflight(action, payload):
        context = contexts.get(action.target_id)
        if context is None:
            raise BuildError(f"missing physical context for {action.target_id!r}")
        for capability in capabilities:
            if getattr(context, capability, None) is None:
                raise BuildError(
                    f"required physical capability {capability!r} is unavailable"
                )

    def run(request):
        result = executor.execute(
            request.action, request.payload, contexts[request.action.target_id]
        )
        if isinstance(result, SkippedExecution):
            if allow_skipped:
                return Completed({**(result.details or {}), "skipped": True})
            return Failed("required physical action was skipped")
        return Completed(result)

    return MutationDriver(
        run, resources=(lane,), serial_resources=(lane,), preflight=preflight
    )
