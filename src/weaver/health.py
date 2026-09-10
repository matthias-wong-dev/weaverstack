"""What the installed estate's operational state adds up to.

Three sections over one installed graph.

.. code-block:: text

    Load    every node holding rows, its current _.LoadStatus, and its freshness
    Tests   every Test and Assumption, its current _.TestStatus, and its freshness
    Build   what installed state contradicts itself

Overall health is the worst section. Nothing here holds a status of its own that
section state could disagree with.

Health overlays evidence onto :class:`weaver.installed.InstalledDag`. Dependency
direction, ancestry and what is installed are that graph's answers, read here and
not recomputed.

Load freshness reads ``_.LoadStatus``. A load settles a table or a folder, and a
successful build settles a View. ``as_of`` against a subject's completion instant
says whether it is overdue, and the same instant against its ancestors' says
whether it is behind its sources.

A Static object is asked neither question. It is loaded once, so it stays Green
however long ago that was. Its own load still counts against everything
downstream, which is what makes a reload of a reference table put its consumers
behind.

This module is pure. Everything it reads is on the catalogue it is given: the
graph, the status tables, and the current load state that catalogue was read
with. See :mod:`weaver.operations.health` for the operation that reads it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Mapping, Sequence

from .catalogue.state import Catalogue
from .catalogue.tables import (
    BLOCKED,
    ERROR,
    FAILED,
    FOLDER_DICTIONARY,
    LOAD_STATUS,
    PENDING,
    REJECTED,
    ROLE_DATA,
    SKIPPED,
    SUCCEEDED,
    TABLE_DICTIONARY,
    TEST_STATUS,
)
from .declaration.model import WeaverDocumentId, WeaverItemId
from .installed import InstalledDag, InstalledNode, stored_identity
from .targets import PhysicalTargetRef

#: The whole severity vocabulary, and the order that makes one worse.
GREEN = "green"
AMBER = "amber"
RED = "red"
SEVERITIES = (GREEN, AMBER, RED)
_WORSE = {severity: rank for rank, severity in enumerate(SEVERITIES)}

#: The three sections a report carries, in the order it renders them.
LOAD = "load"
TESTS = "tests"
BUILD = "build"
AREAS = (LOAD, TESTS, BUILD)

#: Stable machine-readable finding codes, so a JSON consumer never parses prose.
LOAD_PENDING = "load_pending"
LOAD_FAILED = "load_failed"
LOAD_REJECTED = "load_rejected"
LOAD_STALE_TIME = "load_stale_time"
LOAD_STALE_ANCESTOR = "load_stale_ancestor"
LOAD_ANCESTOR_UNESTABLISHED = "load_ancestor_unestablished"
TEST_PENDING = "test_pending"
TEST_FAILED = "test_failed"
TEST_STALE_DEPENDENCY = "test_stale_dependency"
MISSING_VALIDATION_ARTEFACT = "missing_validation_artefact"
NOT_INSTALLED = "not_installed"
CERTIFIED_MISSING = "certified_missing"
DEPENDENCY_UNRESOLVED = "dependency_unresolved"
AMBIGUOUS_INSTALLATION = "ambiguous_installation"

#: How long an installed object may go without a settled load before it reads
#: as stale. One day, resolved against the clock at the start of the operation.
DEFAULT_AGE_HOURS = 24

#: Load outcomes that establish nothing about the data. A blocked node did no
#: work and an errored one cannot say what it did, so neither is a reason to
#: call a downstream object behind. A Static skip is not here: it is a
#: lifecycle touch, recorded in ``_.LoadStatus`` with the workflow that made
#: it, and ancestry reads it like any other touch.
_NO_DATA_ESTABLISHED = (FAILED, ERROR, BLOCKED, PENDING)

#: The load outcomes that make a subject Red.
_LOAD_RED = (FAILED, ERROR, BLOCKED)

#: The outcomes that carry an instant a descendant can be measured against. A
#: rejecting load moved rows and completed. A Static skip consumed no window,
#: but it is a lifecycle touch: ``_.LoadStatus`` records when it happened, and
#: ancestry measures against that record.
_ESTABLISHED = (SUCCEEDED, REJECTED, SKIPPED)

#: What Registry records a View as. A Warehouse shortcut destination is recorded
#: the same way and is told apart by its role.
VIEW_OBJECT_TYPE = "view"

#: The validation outcomes that make a subject Red.
_TEST_RED = (FAILED, ERROR, BLOCKED)

#: The format version of :meth:`HealthReport.to_mapping`. Version 2 replaced
#: ``latest_load``, one workflow read from ``_.Log``, with ``current_load``,
#: the workflows behind current ``_.LoadStatus`` state.
FORMAT_VERSION = 2


def worst(severities) -> str:
    """The worst of some severities, or Green where there are none."""

    return max((*severities, GREEN), key=lambda severity: _WORSE[severity])


# --- what the catalogue said about one object ---------------------------------


@dataclass(frozen=True)
class RuntimeStatus:
    """One ``_.LoadStatus`` or ``_.TestStatus`` row, as health reads it."""

    identity: WeaverDocumentId
    result: str
    workflow_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    failure_count: int | None = None


def load_statuses(catalogue: Catalogue) -> Mapping[WeaverDocumentId, RuntimeStatus]:
    """Every current load status the catalogue holds, by logical identity."""

    return _statuses(catalogue, LOAD_STATUS)


def test_statuses(catalogue: Catalogue) -> Mapping[WeaverDocumentId, RuntimeStatus]:
    """Every current validation status the catalogue holds, by logical identity.

    Keyed as a validation, because ``_.TestStatus`` holds nothing else.
    """

    return _statuses(catalogue, TEST_STATUS, validation=True)


def _statuses(
    catalogue: Catalogue, table, *, validation: bool = False
) -> Mapping[WeaverDocumentId, RuntimeStatus]:
    found: dict[WeaverDocumentId, RuntimeStatus] = {}
    for row in catalogue.table_rows(table):
        identity = _row_identity(row, validation=validation)
        failure_count = row.get("failure_count")
        found[identity] = RuntimeStatus(
            identity=identity,
            result=str(row.get("result") or ""),
            workflow_id=_text(row.get("workflow_id")),
            started_at=_aware(row.get("started_datetime")),
            completed_at=_aware(row.get("completed_datetime")),
            failure_count=None if failure_count is None else int(failure_count),
        )
    return MappingProxyType(found)


@dataclass(frozen=True)
class EffectiveLoadState:
    """Current load state for the estate a report is about.

    A mirrored object's rows are written where it mirrors, so its lifecycle
    state is the source catalogue's. Everything else is the selected
    catalogue's. A fork copies ``_.LoadStatus`` when it is made, so a mirrored
    object carries a row here from the moment of the fork; that copy is
    replaced, and where the source holds nothing the object reads as
    unestablished.
    """

    statuses: Mapping[WeaverDocumentId, RuntimeStatus]
    history: object | None = None


def effective_load_state(catalogue: Catalogue, *, source=None) -> EffectiveLoadState:
    """One estate's load state, with mirrored identities taken from ``source``.

    With no ``_.Mirror`` rows this is the catalogue's own state unchanged, which
    is every estate that forks nothing.
    """

    mirrored = frozenset(catalogue.mirrors)
    if not mirrored:
        return EffectiveLoadState(
            statuses=load_statuses(catalogue), history=catalogue.load_history
        )
    from_source = load_statuses(source) if source is not None else {}
    statuses = {
        identity: status
        for identity, status in load_statuses(catalogue).items()
        if identity not in mirrored
    }
    statuses.update(
        (identity, from_source[identity])
        for identity in mirrored
        if identity in from_source
    )
    return EffectiveLoadState(
        statuses=MappingProxyType(statuses),
        history=_effective_history(
            catalogue.load_history,
            source=None if source is None else source.load_history,
            statuses=statuses,
            mirrored=mirrored,
        ),
    )


def _effective_history(local, *, source, statuses, mirrored):
    """The window behind effective state, drawn from both catalogues.

    Statistics for a mirrored object come from the source, which is also the
    only place they exist: a fork copies ``_.LoadStatus`` and leaves
    ``_.LoadStatistic`` behind. The summary is recounted from the merged
    statuses, so what the report totals and what it assesses are one thing.
    """

    from .catalogue.history import LoadHistory

    if local is None and source is None:
        return None
    rows = [
        row
        for row in (local.statistics if local is not None else ())
        if _row_identity(row) not in mirrored
    ]
    rows.extend(
        row
        for row in (source.statistics if source is not None else ())
        if _row_identity(row) in mirrored
    )
    if not statuses and not rows:
        # Bootstrap on both sides. Nothing has settled a load, and a report
        # carries no window at all.
        return None
    counts: dict[str, int] = {}
    for status in statuses.values():
        counts[status.result] = counts.get(status.result, 0) + 1
    started = [status.started_at for status in statuses.values() if status.started_at]
    completed = [
        status.completed_at for status in statuses.values() if status.completed_at
    ]
    return LoadHistory(
        workflow_ids=tuple(
            sorted(
                {
                    status.workflow_id
                    for status in statuses.values()
                    if status.workflow_id
                }
            )
        ),
        started_at=min(started) if started else None,
        completed_at=max(completed) if completed else None,
        counts=MappingProxyType(counts),
        statistics=tuple(rows),
    )


def _row_identity(
    row: Mapping[str, object], *, validation: bool = False
) -> WeaverDocumentId:
    from .declaration.metadata import ObjectId
    from .declaration.model import WeaverDocumentId as _Document
    from .declaration.model import WeaverItemId

    item = WeaverItemId(
        str(row.get("item_type") or ""), str(row.get("item_name") or "")
    )
    schema = str(row.get("schema_name") or "")
    name = str(row.get("object_name") or "")
    if validation:
        return _Document.validation(item, ObjectId(schema, name))
    return stored_identity(item, schema, name)


# --- the report ---------------------------------------------------------------


@dataclass(frozen=True)
class HealthFinding:
    """One thing worth an operator's attention, in one section.

    ``severity`` is the health vocabulary and ``status`` the runtime one, so a
    consumer reads how bad it is and what actually happened without either word
    standing in for the other.
    """

    area: str
    code: str
    severity: str
    message: str
    object_id: str | None = None
    target: str | None = None
    status: str | None = None
    workflow_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    failure_count: int | None = None

    @property
    def sort_key(self) -> tuple:
        """Worst first, then by code and by the object it is about."""

        return (
            -_WORSE[self.severity],
            self.code,
            self.target or "",
            self.object_id or "",
        )

    def to_mapping(self) -> dict:
        return {
            "area": self.area,
            "code": self.code,
            "severity": self.severity,
            "status": self.status,
            "message": self.message,
            "object_id": self.object_id,
            "target": self.target,
            "workflow_id": self.workflow_id,
            "started_at": _isoformat(self.started_at),
            "completed_at": _isoformat(self.completed_at),
            "failure_count": self.failure_count,
        }


@dataclass(frozen=True)
class HealthSection:
    """One area's findings and the counts behind them."""

    area: str
    findings: tuple[HealthFinding, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)
    #: How many subjects this section considered.
    subjects: int = 0

    @property
    def status(self) -> str:
        """The worst finding's severity, or Green where there are none."""

        return worst(finding.severity for finding in self.findings)

    def to_mapping(self) -> dict:
        return {
            "status": self.status,
            "subjects": self.subjects,
            "counts": dict(sorted(self.counts.items())),
            "findings": [finding.to_mapping() for finding in self.findings],
        }


@dataclass(frozen=True)
class LoadActivity:
    """What one recorded load did, as counts."""

    object_id: str
    target: str | None
    workflow_id: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: int | None = None
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_rejected: int = 0
    is_reload: bool = False
    is_static_skip: bool = False

    def to_mapping(self) -> dict:
        return {
            "object_id": self.object_id,
            "target": self.target,
            "workflow_id": self.workflow_id,
            "started_at": _isoformat(self.started_at),
            "completed_at": _isoformat(self.completed_at),
            "duration_ms": self.duration_ms,
            "rows_read": self.rows_read,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "rows_rejected": self.rows_rejected,
            "is_reload": self.is_reload,
            "is_static_skip": self.is_static_skip,
        }


@dataclass(frozen=True)
class CurrentLoad:
    """What the estate's current load state is, and when it was reached.

    All of it summarises ``_.LoadStatus``, so it covers every current object
    including the ones no statistic describes. ``counts`` are those objects'
    results. ``workflow_ids`` are the workflows the state came from, sorted:
    a partial load leaves the objects it did not touch explained by the load
    that last did, so current state spans as many workflows as it took.
    ``completed_at`` is the estate's last load activity.

    The runtime records orchestrated and standalone loads under the same
    ``load`` task type, so none of this claims to be a scheduled run.
    """

    workflow_ids: tuple[str, ...] = ()
    started_at: datetime | None = None
    completed_at: datetime | None = None
    counts: Mapping[str, int] = field(default_factory=dict)

    def to_mapping(self) -> dict:
        return {
            "workflow_ids": list(self.workflow_ids),
            "started_at": _isoformat(self.started_at),
            "completed_at": _isoformat(self.completed_at),
            "counts": dict(sorted(self.counts.items())),
        }


@dataclass(frozen=True)
class HealthReport:
    """What the installed estate's operational state adds up to."""

    generated_at: datetime
    as_of: datetime
    load: HealthSection
    tests: HealthSection
    build: HealthSection
    #: The physical targets reported on, in the grammar a request names them.
    targets: tuple[str, ...] = ()
    current_load: CurrentLoad | None = None
    load_activity: tuple[LoadActivity, ...] = ()

    @property
    def status(self) -> str:
        """The worst section's status."""

        return worst(section.status for section in self.sections)

    @property
    def sections(self) -> tuple[HealthSection, ...]:
        return (self.load, self.tests, self.build)

    @property
    def findings(self) -> tuple[HealthFinding, ...]:
        return tuple(
            finding for section in self.sections for finding in section.findings
        )

    @property
    def is_healthy(self) -> bool:
        return self.status == GREEN

    def slowest(self, limit: int = 5) -> tuple[LoadActivity, ...]:
        """The longest recorded loads in the window, longest first."""

        timed = [each for each in self.load_activity if each.duration_ms is not None]
        timed.sort(key=lambda each: (-each.duration_ms, each.object_id))
        return tuple(timed[:limit])

    def moved(self, limit: int = 5) -> tuple[LoadActivity, ...]:
        """The recorded loads that changed the most rows, largest first."""

        def changed(each: LoadActivity) -> int:
            return each.rows_inserted + each.rows_updated + each.rows_deleted

        moved = [each for each in self.load_activity if changed(each)]
        moved.sort(key=lambda each: (-changed(each), each.object_id))
        return tuple(moved[:limit])

    def to_mapping(self) -> dict:
        return {
            "format_version": FORMAT_VERSION,
            "generated_at": _isoformat(self.generated_at),
            "as_of": _isoformat(self.as_of),
            "status": self.status,
            "targets": list(self.targets),
            "sections": {
                LOAD: self.load.to_mapping(),
                TESTS: self.tests.to_mapping(),
                BUILD: self.build.to_mapping(),
            },
            "current_load": None
            if self.current_load is None
            else self.current_load.to_mapping(),
            "load_activity": [each.to_mapping() for each in self.load_activity],
        }


def current_load(history) -> CurrentLoad | None:
    """The window a catalogue was read with, as a report carries it."""

    if history is None:
        return None
    return CurrentLoad(
        workflow_ids=tuple(history.workflow_ids),
        started_at=_aware(history.started_at),
        completed_at=_aware(history.completed_at),
        counts=MappingProxyType(dict(history.counts)),
    )


def load_activity(history, *, targets=None) -> tuple[LoadActivity, ...]:
    """The window's ``_.LoadStatistic`` rows, with each object's target named.

    ``targets`` maps a logical item to the physical target it is bound to, which
    a statistic row does not carry: it holds logical identity, and where that
    object lives is the Installation's to say.
    """

    if history is None:
        return ()
    bound = dict(targets or {})
    found = []
    for row in history.statistics:
        identity = _row_identity(row)
        target = bound.get(identity.item)
        duration = row.get("duration_milliseconds")
        found.append(
            LoadActivity(
                object_id=str(identity),
                target=None if target is None else str(target),
                workflow_id=str(row.get("workflow_id") or ""),
                started_at=_aware(row.get("started_datetime")),
                completed_at=_aware(row.get("completed_datetime")),
                duration_ms=None if duration is None else int(duration),
                rows_read=_count(row, "rows_read"),
                rows_inserted=_count(row, "rows_inserted"),
                rows_updated=_count(row, "rows_updated"),
                rows_deleted=_count(row, "rows_deleted"),
                rows_rejected=_count(row, "rows_rejected"),
                is_reload=bool(row.get("is_reload")),
                is_static_skip=bool(row.get("is_static_skip")),
            )
        )
    return tuple(found)


def _count(row, name: str) -> int:
    value = row.get(name)
    return 0 if value is None else int(value)


# --- the canonical Load assessment --------------------------------------------


@dataclass(frozen=True)
class LoadSubjectHealth:
    """One loadable, its ``_.LoadStatus`` row and its findings.

    No findings is Green. ``runtime_status`` is carried so a report can render
    what happened.
    """

    node: InstalledNode
    runtime_status: RuntimeStatus | None = None
    findings: tuple[HealthFinding, ...] = ()

    @property
    def identity(self) -> WeaverDocumentId:
        return self.node.identity

    @property
    def severity(self) -> str:
        return worst(finding.severity for finding in self.findings)


@dataclass(frozen=True)
class LoadAssessment:
    """Every loadable in scope, assessed once.

    :func:`assess` renders it as the report's Load section, and
    ``weaver load --stale`` runs the subjects it does not call Green.
    """

    subjects: tuple[LoadSubjectHealth, ...] = ()

    @property
    def status(self) -> str:
        return worst(subject.severity for subject in self.subjects)

    def unsettled(self) -> tuple[LoadSubjectHealth, ...]:
        """The subjects that are not Green, in subject order."""

        return tuple(subject for subject in self.subjects if subject.severity != GREEN)

    def unsettled_identities(self) -> tuple[WeaverDocumentId, ...]:
        """The non-Green subjects a load may run here.

        A mirrored subject is assessed and never selected: its rows are written
        in the catalogue it mirrors. A local descendant of one is selected in
        the ordinary way, which is how a source that advanced reaches this
        estate.
        """

        return tuple(
            subject.identity for subject in self.unsettled() if subject.node.can_load
        )

    def to_health_section(self) -> HealthSection:
        """The Load section of a health report."""

        findings: list[HealthFinding] = []
        counts: dict[str, int] = {}
        for subject in self.subjects:
            word = _word(subject.runtime_status)
            counts[word] = counts.get(word, 0) + 1
            findings.extend(subject.findings)
        return HealthSection(
            area=LOAD,
            findings=_ordered(findings),
            counts=MappingProxyType(counts),
            subjects=len(self.subjects),
        )


class _LoadHealth:
    """What a loadable's Load health is.

    ``as_of`` against its completion instant says whether the object is overdue,
    and the same instant against its ancestors' says whether it is behind its
    sources. A Static object is asked neither.
    """

    def __init__(
        self,
        dag: InstalledDag,
        *,
        statuses: Mapping[WeaverDocumentId, RuntimeStatus],
        as_of: datetime,
    ) -> None:
        self.dag = dag
        self.statuses = statuses
        self.as_of = as_of

    def assess(self, nodes: Sequence[InstalledNode]) -> LoadAssessment:
        return LoadAssessment(subjects=tuple(self.subject(node) for node in nodes))

    def subject(self, node: InstalledNode) -> LoadSubjectHealth:
        status = self.statuses.get(node.identity)
        findings = (*self._state(node, status), *self._freshness(node, status))
        return LoadSubjectHealth(node=node, runtime_status=status, findings=findings)

    def _state(self, node: InstalledNode, status):
        if status is None or status.result == PENDING:
            # A build leaves a rebuilt loadable Pending. A missing row is read
            # the same way.
            yield _finding(
                LOAD,
                LOAD_PENDING,
                AMBER,
                node,
                PENDING,
                "no load has settled since this object was built",
                status,
            )
            return
        if status.result in _LOAD_RED:
            yield _finding(
                LOAD,
                LOAD_FAILED,
                RED,
                node,
                status.result,
                f"the last load {status.result}",
                status,
            )
            return
        if status.result == REJECTED:
            yield _finding(
                LOAD,
                LOAD_REJECTED,
                AMBER,
                node,
                status.result,
                "the last load completed with rejected rows",
                status,
            )

    def _freshness(self, node: InstalledNode, status):
        """Whether this object is overdue, and whether it is behind its sources.

        A Static object is asked neither. It is loaded once, so age says nothing
        about it and neither does an ancestor that settled since.
        """

        if status is None or status.result in _NO_DATA_ESTABLISHED:
            return
        if node.is_static:
            return
        if status.completed_at is not None and status.completed_at < self.as_of:
            yield _finding(
                LOAD,
                LOAD_STALE_TIME,
                AMBER,
                node,
                status.result,
                f"last loaded {_isoformat(status.completed_at)}",
                status,
            )
        unestablished = self._unestablished_ancestor(node)
        if unestablished is not None:
            yield _finding(
                LOAD,
                LOAD_ANCESTOR_UNESTABLISHED,
                AMBER,
                node,
                status.result,
                f"{unestablished} has no settled state for its current incarnation",
                status,
            )
        newer = self._newer_ancestor(node)
        if newer is not None:
            yield _finding(
                LOAD,
                LOAD_STALE_ANCESTOR,
                AMBER,
                node,
                status.result,
                f"{newer} has been established since this object was loaded",
                status,
            )

    # --- ancestry, over _.LoadStatus ------------------------------------------

    def _lifecycle_ancestors(self, node: InstalledNode):
        """The ancestors that carry a ``_.LoadStatus`` row.

        Loadables and Views. A shortcut destination and a table Weaver does not
        load hold no load state, so the walk passes through them. A skipped
        Static ancestor takes part like any other: its row is the object's
        latest lifecycle touch, so a descendant last touched before it is
        behind.
        """

        return tuple(
            ancestor
            for ancestor in self.dag.ancestors(node.identity)
            if participates_in_load_state(ancestor)
        )

    def established_at(self, identity) -> datetime | None:
        """When this node last settled, or ``None`` if it has not.

        A Pending, failed, errored or blocked state settled nothing, and neither
        does a missing row.
        """

        status = self.statuses.get(identity)
        if status is None or status.result not in _ESTABLISHED:
            return None
        return status.completed_at

    def _unestablished_ancestor(self, node: InstalledNode) -> str | None:
        """The ancestor that has not settled since it was built."""

        behind = [
            ancestor.node_id
            for ancestor in self._lifecycle_ancestors(node)
            if self.established_at(ancestor.identity) is None
        ]
        return sorted(behind)[0] if behind else None

    def _newer_ancestor(self, node: InstalledNode) -> str | None:
        """The ancestor that settled after this object last loaded.

        A rebuilt View settles when its build succeeds, so a new View definition
        puts everything materialised from it behind.
        """

        loaded = self.established_at(node.identity)
        if loaded is None:
            return None
        newer = [
            ancestor.node_id
            for ancestor in self._lifecycle_ancestors(node)
            if (self.established_at(ancestor.identity) or loaded) > loaded
        ]
        return sorted(newer)[0] if newer else None


def participates_in_load_state(node: InstalledNode) -> bool:
    """Whether this node carries ``_.LoadStatus`` state, wherever it is read.

    Observation, not execution. A loadable settles here, a mirrored table or
    folder settles in the catalogue it mirrors, and a View settles when its
    build succeeds. A View is not a load subject; its state lets a materialised
    descendant detect a newer View definition.
    """

    return node.can_load or node.is_mirrored or is_view_node(node)


def is_load_subject(node: InstalledNode) -> bool:
    """Whether Load health assesses this node.

    The objects that hold rows, whether those rows are their own or a mirror's.
    A View is left out, mirrored or not: a build settles it, and Build health is
    where its installation is reported. It stays in
    :func:`participates_in_load_state`, where its instant orders a materialised
    descendant against it.
    """

    return (node.can_load or node.is_mirrored) and not is_view_node(node)


def is_view_node(node: InstalledNode) -> bool:
    """Whether this node is a View this repository authored."""

    return node.role == ROLE_DATA and node.object_type == VIEW_OBJECT_TYPE


def load_health(catalogue: Catalogue, *, as_of: datetime, source=None) -> _LoadHealth:
    """The Load rules, bound to one catalogue's graph and its effective state."""

    return _LoadHealth(
        catalogue.dag(),
        statuses=effective_load_state(catalogue, source=source).statuses,
        as_of=as_of,
    )


def assess_load(
    catalogue: Catalogue,
    *,
    as_of: datetime,
    items: Sequence[WeaverItemId] | None = None,
    targets: Sequence[PhysicalTargetRef] | None = None,
    source=None,
) -> LoadAssessment:
    """Every object holding rows in scope, assessed once.

    ``items`` bounds the subjects by logical item and ``targets`` by physical
    target. Ancestry outside the scope is still read, because whether a subject
    is behind its sources is a question about the whole graph.

    A mirrored object is a subject: its source's load is what its rows are.
    ``source`` is the catalogue that load is recorded in.
    """

    dag = catalogue.dag()
    chosen = None if items is None else frozenset(items)
    where = None if targets is None else frozenset(targets)
    subjects = tuple(
        node
        for node in dag.nodes
        if is_load_subject(node)
        and (chosen is None or node.item in chosen)
        and (where is None or node.target in where)
    )
    return load_health(catalogue, as_of=as_of, source=source).assess(subjects)


def resolve_as_of(value, *, started: datetime) -> datetime:
    """The freshness cutoff, always UTC.

    Omitted, it is :data:`DEFAULT_AGE_HOURS` before the operation started. A
    naive datetime is refused: an instant with no zone names a different moment
    on every machine that reads it.
    """

    from .errors import CommandError

    if value is None:
        return started - timedelta(hours=DEFAULT_AGE_HOURS)
    if isinstance(value, str):
        text = value.strip()
        try:
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise CommandError(
                f"as-of must be an ISO-8601 instant with a zone, got {text!r}"
            ) from None
    if not isinstance(value, datetime):
        raise CommandError(
            f"as-of must be a datetime or an ISO-8601 string, got "
            f"{type(value).__name__}"
        )
    if value.tzinfo is None:
        raise CommandError(
            "as-of must carry a timezone, so the instant it names is the same "
            "on every machine that reads the report"
        )
    return value.astimezone(timezone.utc)


# --- the evaluation -----------------------------------------------------------


def assess(
    catalogue: Catalogue,
    *,
    as_of: datetime,
    generated_at: datetime,
    targets: Sequence[PhysicalTargetRef] | None = None,
    inventories: Mapping[PhysicalTargetRef, object] | None = None,
    source: Catalogue | None = None,
) -> HealthReport:
    """One catalogue's operational state, as a report.

    Everything comes from the catalogue: what is installed, what depends on what,
    the current status tables, and the current load state it was read with.

    ``targets`` restricts what is reported on. Ancestry outside it is still read,
    because whether a selected object is behind its sources is a question about
    the whole managed graph.

    ``inventories`` are read for the selected targets alone, so a certified
    object the target does not hold is reported. With none, Build health reports
    what the catalogue contradicts about itself.

    ``source`` is the catalogue this estate mirrors, where one is mirrored. It
    supplies Load state for the mirrored identities and nothing else.
    """

    dag = catalogue.dag()
    effective = effective_load_state(catalogue, source=source)
    history = effective.history
    selected = None if targets is None else tuple(dict.fromkeys(targets))
    evaluation = _Assessment(
        dag,
        catalogue=catalogue,
        as_of=as_of,
        selected=selected,
        inventories=dict(inventories or {}),
        effective=effective,
    )
    return HealthReport(
        generated_at=generated_at,
        as_of=as_of,
        load=evaluation.load(),
        tests=evaluation.tests(),
        build=evaluation.build(),
        targets=tuple(str(target) for target in (selected or dag.targets)),
        current_load=current_load(history),
        load_activity=load_activity(history, targets=dag.installations),
    )


class _Assessment:
    """One evaluation's working state, over one installed graph."""

    def __init__(
        self,
        dag: InstalledDag,
        *,
        catalogue: Catalogue,
        as_of: datetime,
        selected,
        inventories,
        effective=None,
    ) -> None:
        self.dag = dag
        self.catalogue = catalogue
        self.as_of = as_of
        self.selected = selected
        self.inventories = inventories
        self.test_status = test_statuses(catalogue)
        # The one Load implementation, over the effective statuses. Validation
        # freshness asks it the same question, so there is one answer to "when
        # was this ancestor established", and a mirrored ancestor answers it
        # with the source's instant.
        if effective is None:
            effective = effective_load_state(catalogue)
        self.load_health = _LoadHealth(dag, statuses=effective.statuses, as_of=as_of)

    # --- scope ----------------------------------------------------------------

    def _in_scope(self, node: InstalledNode) -> bool:
        return self.selected is None or node.target in self.selected

    def _subjects(self, nodes) -> tuple[InstalledNode, ...]:
        return tuple(node for node in nodes if self._in_scope(node))

    # --- load -----------------------------------------------------------------

    def load(self) -> HealthSection:
        """Every object holding rows in scope, as the Load assessment."""

        return self.load_health.assess(
            self._subjects(
                tuple(node for node in self.dag.nodes if is_load_subject(node))
            )
        ).to_health_section()

    # --- tests ----------------------------------------------------------------

    def tests(self) -> HealthSection:
        """Every validation node in scope, its result and its freshness."""

        findings: list[HealthFinding] = []
        counts: dict[str, int] = {}
        subjects = self._subjects(self.dag.validations())
        for node in subjects:
            status = self.test_status.get(node.identity)
            counts[_word(status)] = counts.get(_word(status), 0) + 1
            findings.extend(self._test_state(node, status))
        return HealthSection(
            area=TESTS,
            findings=_ordered(findings),
            counts=MappingProxyType(counts),
            subjects=len(subjects),
        )

    def _test_state(self, node: InstalledNode, status):
        if status is None:
            yield _finding(
                TESTS,
                TEST_PENDING,
                AMBER,
                node,
                PENDING,
                "this validation has not run since it was built",
            )
            return
        if status.result in _TEST_RED:
            yield _finding(
                TESTS,
                TEST_FAILED,
                RED,
                node,
                status.result,
                f"the last run {status.result}",
                status,
            )
            return
        if status.result != SUCCEEDED:
            yield _finding(
                TESTS,
                TEST_PENDING,
                AMBER,
                node,
                status.result,
                f"the last run {status.result}",
                status,
            )
            return
        moved = self._loaded_since(node, status.completed_at)
        if moved is not None:
            yield _finding(
                TESTS,
                TEST_STALE_DEPENDENCY,
                AMBER,
                node,
                status.result,
                f"{moved} has been loaded since this validation passed",
                status,
            )

    def _loaded_since(self, node: InstalledNode, at: datetime | None) -> str | None:
        """The managed ancestor that settled after this validation passed.

        A new View definition puts the validations over it behind, as it does a
        materialised consumer.
        """

        if at is None:
            return None
        moved = [
            ancestor.node_id
            for ancestor in self.dag.ancestors(node.identity)
            if participates_in_load_state(ancestor)
            and (self.load_health.established_at(ancestor.identity) or at) > at
        ]
        return sorted(moved)[0] if moved else None

    # --- build ----------------------------------------------------------------

    def build(self) -> HealthSection:
        """What installed state contradicts about itself.

        A load artefact's absence is not among them. A table may declare ``Has
        load procedure: false``, and the catalogue records no column separating
        that from an artefact that failed to install.
        """

        findings: list[HealthFinding] = []
        subjects = self._subjects(self.dag.nodes)
        findings.extend(self._missing_validation_artefacts())
        findings.extend(self._declared_but_not_installed())
        findings.extend(self._unresolved_reads())
        findings.extend(self._ambiguous_addresses())
        findings.extend(self._absent_from_inventory())
        return HealthSection(
            area=BUILD,
            findings=_ordered(findings),
            subjects=len(subjects),
        )

    def _missing_validation_artefacts(self):
        for node in self._subjects(self.dag.validations()):
            if node.is_installed:
                continue
            yield _finding(
                BUILD,
                MISSING_VALIDATION_ARTEFACT,
                RED,
                node,
                None,
                f"{node.artefact} is declared but not registered",
            )

    def _declared_but_not_installed(self):
        """A dictionary row Registry does not certify: declared, not installed."""

        for table in (TABLE_DICTIONARY, FOLDER_DICTIONARY):
            for row in self.catalogue.table_rows(table):
                identity = _row_identity(row)
                if identity in self.catalogue.registered:
                    continue
                target = self.dag.installations.get(identity.item)
                if target is None or (
                    self.selected is not None and target not in self.selected
                ):
                    continue
                yield HealthFinding(
                    area=BUILD,
                    code=NOT_INSTALLED,
                    severity=RED,
                    message=f"{table.name} declares it and Registry does not certify it",
                    object_id=str(identity),
                    target=str(target),
                )

    def _unresolved_reads(self):
        for identity, messages in sorted(
            self.dag.unresolved.items(), key=lambda pair: str(pair[0])
        ):
            node = self.dag.by_id.get(str(identity))
            if node is None or not self._in_scope(node):
                continue
            yield _finding(BUILD, DEPENDENCY_UNRESOLVED, RED, node, None, messages[0])

    def _ambiguous_addresses(self):
        for target, found in sorted(
            self.dag.ambiguous.items(), key=lambda pair: str(pair[0])
        ):
            if self.selected is not None and target not in self.selected:
                continue
            yield HealthFinding(
                area=BUILD,
                code=AMBIGUOUS_INSTALLATION,
                severity=RED,
                message=found[0],
                target=str(target),
            )

    def _absent_from_inventory(self):
        """A Registry-certified object the physical target does not hold.

        A Lakehouse view is left out. It exists in the Spark catalogue and
        nowhere in storage, and health reads a Lakehouse over storage so that a
        report never starts a Spark session.
        """

        for node in self._subjects(self.dag.nodes):
            inventory = self.inventories.get(node.target)
            if inventory is None:
                continue
            for where in self._certified(node):
                if node.target.is_lakehouse and where.object_type == "view":
                    continue
                if inventory.has_object(where.schema, where.object, where.object_type):
                    continue
                yield _finding(
                    BUILD,
                    CERTIFIED_MISSING,
                    RED,
                    node,
                    None,
                    f"{where} is certified and {node.target} does not hold it",
                )

    def _certified(self, node: InstalledNode):
        """What Registry certifies for one node, as physical addresses.

        A validation materialises nothing under its own identity, so what is
        certified for it is the artefact alone.
        """

        if node.object_type:
            yield node.physical
        if node.is_installed:
            yield node.artefact_physical(node.artefact_type)


def _finding(
    area: str,
    code: str,
    severity: str,
    node: InstalledNode,
    status_word: str | None,
    message: str,
    status: RuntimeStatus | None = None,
) -> HealthFinding:
    """One finding about one installed node, carrying the status behind it."""

    return HealthFinding(
        area=area,
        code=code,
        severity=severity,
        message=message,
        object_id=node.node_id,
        target=str(node.target),
        status=status_word,
        workflow_id=None if status is None else status.workflow_id,
        started_at=None if status is None else status.started_at,
        completed_at=None if status is None else status.completed_at,
        failure_count=None if status is None else status.failure_count,
    )


def _ordered(findings) -> tuple[HealthFinding, ...]:
    return tuple(sorted(findings, key=lambda finding: finding.sort_key))


def _word(status: RuntimeStatus | None) -> str:
    return PENDING if status is None else (status.result or PENDING)


def _aware(at) -> datetime | None:
    """One stored instant, always aware and always UTC.

    The ``_`` schema holds ``datetime2``, which carries no zone, and every
    instant Weaver writes there is UTC.
    """

    if not isinstance(at, datetime):
        return None
    return at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)


def _isoformat(at: datetime | None) -> str | None:
    return None if at is None else at.isoformat()


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "AMBER",
    "AMBIGUOUS_INSTALLATION",
    "AREAS",
    "BUILD",
    "CERTIFIED_MISSING",
    "DEFAULT_AGE_HOURS",
    "DEPENDENCY_UNRESOLVED",
    "FORMAT_VERSION",
    "GREEN",
    "HealthFinding",
    "HealthReport",
    "HealthSection",
    "LOAD",
    "LOAD_FAILED",
    "LOAD_PENDING",
    "LOAD_REJECTED",
    "LOAD_ANCESTOR_UNESTABLISHED",
    "LOAD_STALE_ANCESTOR",
    "LOAD_STALE_TIME",
    "MISSING_VALIDATION_ARTEFACT",
    "NOT_INSTALLED",
    "RED",
    "SEVERITIES",
    "TESTS",
    "TEST_FAILED",
    "TEST_PENDING",
    "TEST_STALE_DEPENDENCY",
    "LoadActivity",
    "LoadAssessment",
    "LoadSubjectHealth",
    "CurrentLoad",
    "RuntimeStatus",
    "assess",
    "assess_load",
    "current_load",
    "load_activity",
    "is_load_subject",
    "participates_in_load_state",
    "is_view_node",
    "load_health",
    "load_statuses",
    "resolve_as_of",
    "test_statuses",
    "worst",
]
