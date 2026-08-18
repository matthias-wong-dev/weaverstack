"""What a Delta keyed load submits, and what it no longer submits.

The reconciliation *semantics* are proved against a real engine, in
``tests/fabric/test_delta_table_load_primitive.py``, matched claim for claim with
the Warehouse. What is proved here is the other half, and it is the half that
changed: the **physical** shape of the execution.

Every phase of the state machine is a persisted Spark relation named by a
temporary view. So an ordinary load writes nothing durable, a phase that decided
on no rows submits no mutation for it, and whatever happens the relations are
given back. An outcome with something to troubleshoot writes durable Delta
evidence, and that is the only thing that does.

The double records statements and answers cardinalities. It does not evaluate
anything: no SQL is parsed, no relation is modelled, and every count is one the
test set. What a statement *means* is a question for the Fabric file; what
Weaver *submits* is answered by reading the recording, and answering it without
a tenant is what lets it be asserted on every commit.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import ObjectId
from weaver.errors import LoadError
from weaver.runtime.load_contract import LoadContract
from weaver.runtime.table_load import load_table

#: The target's physical shape, as ``_business_columns`` reads it off the table.
TARGET_COLUMNS = (
    ("Customer id", "string"),
    ("Customer name", "string"),
    ("Email", "string"),
    ("row_insert_datetime", "timestamp"),
    ("row_update_datetime", "timestamp"),
    ("row_delete_datetime", "timestamp"),
    ("row_signature", "string"),
)

BUSINESS = ("Customer id", "Customer name", "Email")

#: The suffixes that must never appear in a durable write on a clean load.
WORKING = ("_Staging", "_Reject", "_Delete", "_Upsert", "_StagingKeep")


# --- the recording double -----------------------------------------------------


class _Row(dict):
    """One answered row, subscripted the way a Spark ``Row`` is."""


class _Frame:
    """One statement's result. Its role is the view the load registered it under."""

    def __init__(self, spark, text: str) -> None:
        self.spark, self.text, self.view = spark, text, None

    def persist(self):
        self.spark.persisted.append(self)
        return self

    def unpersist(self):
        self.spark.unpersisted.append(self.view)
        return self

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark's name
        self.view = name
        self.spark.views.append(name)

    @property
    def role(self) -> str | None:
        return None if self.view is None else self.view.split("_")[1]

    def count(self) -> int:
        self.spark.counted.append(self.role)
        return self.spark.counts.get(self.role, 0)

    def collect(self):
        return self.spark.answer(self.text)

    def take(self, n: int):
        return self.collect()[:n]


class _Boom(RuntimeError):
    """A failure the load has no outcome for. An engine error, not a refusal."""


class _EvidenceRefused(RuntimeError):
    """The durable write a failure attempts, failing in its turn."""


@dataclass
class _Spark:
    """A session that records what it was asked to run.

    ``counts`` is the whole of what a test configures: how many rows each phase
    decided on. Nothing here derives one from another, so a flow that stopped
    asking would fail rather than quietly agree.
    """

    counts: dict = field(default_factory=dict)
    #: A statement carrying this text fails, standing for an engine error the
    #: load has no outcome for. Raised before the statement is recorded, so what
    #: was recorded is what the session actually ran.
    fail_on: str | None = None
    #: Every durable write fails, which is how the evidence a failure leaves is
    #: made to fail in its turn.
    fail_creates: bool = False
    statements: list = field(default_factory=list)
    counted: list = field(default_factory=list)
    persisted: list = field(default_factory=list)
    unpersisted: list = field(default_factory=list)
    views: list = field(default_factory=list)
    dropped_views: list = field(default_factory=list)

    def __post_init__(self) -> None:
        self.catalog = _Catalog(self)

    def table(self, name: str):
        return _Table(TARGET_COLUMNS)

    def sql(self, text: str) -> _Frame:
        if self.fail_on is not None and self.fail_on in text:
            raise _Boom(f"the engine failed on: {text.splitlines()[0]}")
        if self.fail_creates and text.startswith("CREATE TABLE"):
            raise _EvidenceRefused("the evidence could not be written")
        self.statements.append(text)
        return _Frame(self, text)

    def answer(self, text: str):
        if "GROUP BY `_Is new row`" in text:
            return [
                _Row(flag=1, n=self.counts.get("inserted", 0)),
                _Row(flag=0, n=self.counts.get("updated", 0)),
            ]
        if "weaver_merge_conflict" in text:
            return [_Row(n=self.counts.get("conflicts", 0))]
        if "count(*) AS n" in text:
            return [_Row(n=self.counts.get("target", 0))]
        raise AssertionError(f"the load asked something this test did not set: {text}")

    # --- what a test reads ----------------------------------------------------

    @property
    def created(self) -> list[str]:
        """Every durable table this load wrote, by the name it wrote."""

        return [
            statement.split("`")[5]
            for statement in self.statements
            if statement.startswith("CREATE TABLE")
        ]

    @property
    def dropped(self) -> list[str]:
        return [
            statement.split("`")[5]
            for statement in self.statements
            if statement.startswith("DROP TABLE")
        ]

    @property
    def mutations(self) -> list[str]:
        """Every statement that changes the target, in order."""

        target = "`lh`.`DWG`.`Customer`"
        return [
            statement.split("\n")[0]
            for statement in self.statements
            if statement.startswith((f"MERGE INTO {target}", f"INSERT INTO {target}"))
            or statement.startswith(f"DELETE FROM {target}")
        ]

    @property
    def leaked(self) -> list[str]:
        """Views this load registered and never dropped."""

        return sorted(set(self.views) - set(self.dropped_views))


class _Catalog:
    def __init__(self, spark) -> None:
        self._spark = spark

    def dropTempView(self, name: str) -> None:  # noqa: N802 - Spark's name
        self._spark.dropped_views.append(name)


class _Table:
    def __init__(self, columns) -> None:
        self.schema = _Schema(columns)


class _Schema:
    def __init__(self, columns) -> None:
        self.fields = [_Field(name, kind) for name, kind in columns]


class _Field:
    def __init__(self, name: str, kind: str) -> None:
        self.name = name
        self.dataType = _Type(kind)  # noqa: N815 - Spark's name


class _Type:
    def __init__(self, kind: str) -> None:
        self._kind = kind

    def simpleString(self) -> str:  # noqa: N802 - Spark's name
        return self._kind


class _Staged:
    """What ``read()`` handed over: a frame the load names but never persists."""

    def __init__(self, columns=BUSINESS) -> None:
        self.columns = list(columns)

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark's name
        self.view = name

    def take(self, n: int):
        return []


class _Lakehouse:
    def qualify(self, schema: str, name: str) -> str:
        return f"`lh`.`{schema}`.`{name}`"


# --- driving one load ---------------------------------------------------------


def _contract(**overrides) -> LoadContract:
    values = dict(
        object_id=ObjectId("DWG", "Customer"),
        primary_key=("Customer id",),
        incremental=False,
    )
    values.update(overrides)
    return LoadContract(**values)


def _load(counts, *, contract=None, staged=None, **kwargs):
    """One load against the recording double, returning the session and outcome."""

    spark = _Spark(counts=counts)
    result = load_table(
        spark,
        contract=contract or _contract(),
        lakehouse=_Lakehouse(),
        staging_frame=staged or _Staged(),
        **kwargs,
    )
    return spark, result


def _refused(counts, *, contract=None, staged=None, match=None, **kwargs):
    """One load that stops at a gate, returning the session that recorded it."""

    spark = _Spark(counts=counts)
    with pytest.raises(LoadError, match=match):
        load_table(
            spark,
            contract=contract or _contract(),
            lakehouse=_Lakehouse(),
            staging_frame=staged or _Staged(),
            **kwargs,
        )
    return spark


def _failed(
    counts, *, fail_on, fail_creates=False, contract=None, staged=None, **kwargs
):
    """One load stopped by an engine failure, returning the session that ran it."""

    spark = _Spark(counts=counts, fail_on=fail_on, fail_creates=fail_creates)
    with pytest.raises(_Boom):
        load_table(
            spark,
            contract=contract or _contract(),
            lakehouse=_Lakehouse(),
            staging_frame=staged or _Staged(),
            **kwargs,
        )
    return spark


#: Where a failure is injected, by the phase whose statement carries the text.
AT_UPSERT = "weaver_proposed"
AT_DELETE_MUTATION = "WHEN MATCHED THEN DELETE"


#: A load with rows to read, nothing refused and nothing to change.
NO_OP = {
    "staging": 3,
    "reject": 0,
    "delete": 0,
    "inserted": 0,
    "updated": 0,
    "target": 3,
}

#: A load that inserts, updates and deletes.
BUSY = {
    "staging": 3,
    "reject": 0,
    "delete": 2,
    "inserted": 1,
    "updated": 1,
    "target": 9,
}


# --- an ordinary load writes nothing durable ----------------------------------


@weaver_test()
def test_a_clean_load_creates_no_working_tables():
    """The change. Every phase is a persisted relation, so none is a Delta table.

    A table per phase cost a write, a commit and a drop for state nothing outside
    the load ever read.
    """

    spark, result = _load(BUSY)

    assert result.succeeded
    assert spark.created == []
    written = [one for one in spark.statements if not one.startswith("DROP TABLE")]
    for suffix in WORKING:
        assert not any(suffix in statement for statement in written), (
            f"a clean load still wrote something for {suffix}"
        )


@weaver_test()
def test_every_phase_is_persisted_and_read_by_name():
    """Staging, rejects, deletes and upserts, in the order the machine runs them."""

    spark, _result = _load(BUSY)

    assert [frame.role for frame in spark.persisted] == [
        "staging",
        "reject",
        "delete",
        "upsert",
    ]


@weaver_test()
def test_a_clean_load_still_clears_an_earlier_runs_evidence():
    """Stale evidence would read as evidence about the run that just succeeded.

    Attempted rather than looked up: a missing table is the ordinary case.
    """

    spark, _result = _load(NO_OP)

    assert spark.dropped == ["Customer_Reject", "Customer_Delete", "Customer_Staging"]


# --- a phase with nothing to do submits nothing -------------------------------


@weaver_test()
def test_no_delete_mutation_when_the_delete_set_is_empty():
    """A zero-row merge is a Delta commit and a scan for work that is not there."""

    spark, _result = _load(dict(NO_OP, delete=0, inserted=1, updated=0))

    assert not any("WHEN MATCHED THEN DELETE" in one for one in spark.mutations)


@weaver_test()
def test_no_upsert_mutation_when_nothing_is_new_or_changed():
    """The unchanged reload: every row's signature matched, so nothing moves."""

    spark, result = _load(NO_OP)

    assert spark.mutations == []
    assert result.rows_inserted == 0
    assert result.rows_updated == 0


@weaver_test()
def test_a_load_with_work_submits_exactly_the_mutations_it_decided_on():
    """Guards the two tests above from passing because the load stopped working."""

    spark, _result = _load(BUSY)

    assert len(spark.mutations) == 3
    assert "WHEN MATCHED THEN DELETE" in spark.mutations[0]
    assert spark.mutations[1].startswith("INSERT INTO")
    assert spark.mutations[2].startswith("MERGE INTO")


# --- the counts ---------------------------------------------------------------


@weaver_test()
def test_the_insert_and_update_counts_come_from_one_pass():
    """Membership already means new or changed, so the flag partitions the set."""

    spark, result = _load(BUSY)

    assert result.rows_inserted == 1
    assert result.rows_updated == 1
    assert spark.counted == ["staging", "reject", "delete"]


@weaver_test()
def test_rows_deleted_is_the_delete_set_and_needs_no_target_recount():
    """The delete set is already narrowed to keys the target holds.

    It is also disjoint from the upsert set, so its size *is* what the target
    lost. Counting the target afterwards asked Delta the same question twice.
    """

    spark, result = _load(BUSY)

    assert result.rows_deleted == 2
    # One target count, for the stability gate. Not a second one afterwards.
    assert len([one for one in spark.statements if "count(*) AS n" in one]) == 2


@weaver_test()
def test_the_target_is_not_counted_when_the_stability_gate_is_ignored():
    """The only reason to ask its size is the gate that reads it."""

    spark, _result = _load(BUSY, ignore_stability_threshold=True)

    assert not any(
        "FROM `lh`.`DWG`.`Customer`" in one and "count(*) AS n" in one
        for one in spark.statements
    )


# --- what a load gives back ---------------------------------------------------


@weaver_test()
def test_a_clean_load_gives_every_relation_back():
    spark, _result = _load(BUSY)

    assert spark.leaked == []
    assert set(spark.unpersisted) == {frame.view for frame in spark.persisted}


@weaver_test()
def test_a_refused_load_gives_every_relation_back_too():
    """Cleanup is in a finally, so the gate that stopped the load does not matter."""

    spark = _refused(dict(NO_OP, reject=2))

    assert spark.leaked == []
    assert set(spark.unpersisted) == {frame.view for frame in spark.persisted}


# --- evidence, and only for an outcome that owes one --------------------------


@weaver_test()
def test_an_intolerant_refusal_leaves_staging_and_the_rejects():
    """What the source proposed, and what Weaver refused. Nothing was written."""

    spark = _refused(dict(NO_OP, reject=2), match="fault_tolerant = 0")

    assert spark.created == ["Customer_Staging", "Customer_Reject"]
    assert spark.mutations == []


@weaver_test()
def test_a_tolerated_refusal_leaves_staging_the_rejects_and_the_deletes():
    """The survivors loaded, and the evidence explains what they were chosen from."""

    spark, result = _load(dict(BUSY, reject=2, clean=1), fault_tolerant=True)

    assert result.rows_rejected == 2
    assert result.rows_inserted == 1
    assert spark.created == ["Customer_Staging", "Customer_Reject", "Customer_Delete"]


@weaver_test()
def test_a_tolerated_refusal_that_deletes_nothing_leaves_no_delete_table():
    """Evidence is what a reader would use, not a full set for symmetry."""

    spark, result = _load(
        dict(NO_OP, reject=1, clean=2, inserted=1), fault_tolerant=True
    )

    assert result.rows_rejected == 1
    assert result.rows_deleted == 0
    assert spark.created == ["Customer_Staging", "Customer_Reject"]


@weaver_test()
def test_the_purge_supersedes_staging_and_gives_the_old_relation_back():
    """Clean staging replaces raw staging, whose evidence is already written."""

    spark, _result = _load(dict(BUSY, reject=1, clean=2), fault_tolerant=True)

    assert [frame.role for frame in spark.persisted] == [
        "staging",
        "reject",
        "clean",
        "delete",
        "upsert",
    ]
    # Released as soon as the survivors are materialised, not held to the end.
    assert spark.unpersisted[0].startswith("weaver_staging_")


@weaver_test()
def test_a_stability_breach_leaves_staging_and_what_it_would_have_removed():
    """Refused before writing, so the evidence is the proposal itself."""

    spark = _refused(
        dict(BUSY, delete=9, target=10),
        contract=_contract(delete_threshold=1, stability_rows=1),
    )

    assert spark.created == ["Customer_Staging", "Customer_Delete"]
    assert spark.mutations == []


@weaver_test()
def test_a_merge_conflict_leaves_staging_and_what_it_would_have_removed():
    """Fatal whatever fault_tolerant says, and it writes nothing to the target."""

    spark = _refused(
        dict(BUSY, conflicts=1),
        contract=_contract(incremental=True, unique_keys=(("Email",),)),
        deletes=_Staged(("Customer id",)),
        fault_tolerant=True,
        match="declared unique key",
    )

    assert spark.created == ["Customer_Staging", "Customer_Delete"]
    assert spark.mutations == []
    assert spark.leaked == []


# --- evidence a failure with no outcome of its own leaves ---------------------


@weaver_test()
def test_a_failure_after_staging_settles_leaves_what_the_source_proposed():
    """An engine error is not a refusal, and the load still owes an explanation.

    Staging has been materialised, so the proposal can be read afterwards even
    though no gate decided anything about it.
    """

    spark = _failed(dict(NO_OP, delete=0), fail_on=AT_UPSERT)

    assert spark.created == ["Customer_Staging"]
    assert spark.mutations == []


@weaver_test()
def test_the_staging_evidence_a_failure_leaves_is_the_raw_proposal():
    """Raw staging, which is what the question ``_Staging`` answers."""

    spark = _failed(dict(NO_OP, delete=0), fail_on=AT_UPSERT)

    written = [one for one in spark.statements if one.startswith("CREATE TABLE")]
    assert len(written) == 1
    assert "FROM weaver_staging_" in written[0]


@weaver_test()
def test_a_failure_after_the_deletes_are_derived_leaves_them_too():
    """Settled before anything moved, so it is what the load was proposing."""

    spark = _failed(BUSY, fail_on=AT_UPSERT)

    assert spark.created == ["Customer_Staging", "Customer_Delete"]


@weaver_test()
def test_a_failure_partway_through_the_target_leaves_the_same_evidence():
    """The deletes went in and the upserts did not, which is what to look at."""

    spark = _failed(BUSY, fail_on=AT_DELETE_MUTATION)

    assert spark.created == ["Customer_Staging", "Customer_Delete"]


@weaver_test()
def test_a_failure_after_a_tolerated_rejection_keeps_the_rejects_it_wrote():
    """The gate's own evidence stands, and the failure adds what it settled since.

    ``_Staging`` is still the raw proposal the gate wrote, not the survivors the
    purge left, and it is written once.
    """

    spark = _failed(
        dict(BUSY, reject=2, clean=1), fail_on=AT_UPSERT, fault_tolerant=True
    )

    assert spark.created == ["Customer_Staging", "Customer_Reject", "Customer_Delete"]
    staging = next(
        one
        for one in spark.statements
        if one.startswith("CREATE TABLE") and "Customer_Staging`" in one
    )
    assert "FROM weaver_staging_" in staging


@weaver_test()
def test_a_failure_never_leaves_the_upsert_set():
    """Evidence describes what was proposed, and the upsert set is work."""

    spark = _failed(BUSY, fail_on=AT_DELETE_MUTATION)

    assert not any("_Upsert" in one for one in spark.statements)


@weaver_test()
def test_evidence_that_cannot_be_written_does_not_replace_the_failure():
    """The engine error is the one worth reporting, so the write is attempted only.

    ``_failed`` asserts the type: an evidence failure reaching the caller would
    be reported instead of the reason the load stopped.
    """

    spark = _failed(BUSY, fail_on=AT_UPSERT, fail_creates=True)

    assert spark.created == []


@weaver_test()
def test_a_failed_load_gives_every_relation_back():
    """Release is in a finally, and the evidence write runs before it."""

    spark = _failed(BUSY, fail_on=AT_UPSERT)

    assert spark.leaked == []
    assert set(spark.unpersisted) == {frame.view for frame in spark.persisted}


@weaver_test()
def test_a_failed_load_whose_evidence_failed_gives_every_relation_back():
    spark = _failed(BUSY, fail_on=AT_UPSERT, fail_creates=True)

    assert spark.leaked == []
    assert set(spark.unpersisted) == {frame.view for frame in spark.persisted}
