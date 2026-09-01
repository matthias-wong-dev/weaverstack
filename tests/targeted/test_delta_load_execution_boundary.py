"""What a Delta keyed load submits, and what it no longer submits.

The reconciliation semantics are proved here, exhaustively and without a tenant,
and matched claim for claim with the Warehouse in
``tests/fabric/test_warehouse_load_primitive.py``. One representative transition
runs against a real Spark engine, in
``tests/fabric/test_delta_keyed_refusal_primitive.py``. What is proved here as
well is the other half, the physical shape of the execution.

Every phase of the state machine is a persisted Spark relation named by a
temporary view. So an ordinary load writes nothing durable, a phase that decided
on no rows submits no mutation for it, and whatever happens the relations are
given back. An outcome with something to troubleshoot writes durable Delta
evidence, and that is the only thing that does.

What the load decides is one relation: every insert, update and delete it
settled on, classified before anything moves. So the counts are one grouped
action, the writes are one merge, and the questions a load no longer has to ask
are asserted here as actions it does not submit.

The double records statements and answers cardinalities. It does not evaluate
anything: no SQL is parsed, no relation is modelled, and every count is one the
test set. What a statement means is a question for the Fabric file; what
Weaver submits is answered by reading the recording, and answering it without
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
WORKING = ("_Staging", "_Reject", "_Delete", "_Upsert", "_Change", "_StagingKeep")


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
        self.spark.register(name)

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


class _ViewNotFound(RuntimeError):
    """Spark's ``TABLE_OR_VIEW_NOT_FOUND``, for a temporary view."""


@dataclass
class _Spark:
    """A session that records what it was asked to run.

    ``counts`` is the whole of what a test configures: how many rows each phase
    decided on. ``inserted``, ``updated`` and ``deleted`` come back from the one
    grouped pass over the settled changes; ``staging``, ``reject``, ``clean`` and
    ``delete`` are answered as the count of the relation carrying that role.
    Nothing here derives one from another, so a flow that stopped asking would
    fail rather than agree.
    """

    counts: dict = field(default_factory=dict)
    #: A statement carrying this text fails, standing for an engine error the
    #: load has no outcome for. Raised before the statement is recorded, so what
    #: was recorded is what the session actually ran.
    fail_on: str | None = None
    #: Durable writes that fail, which is how the evidence a load leaves is made
    #: to fail in its turn. ``True`` for every one, or the name of a single table.
    fail_creates: bool | str = False
    statements: list = field(default_factory=list)
    #: Every durable write asked for, including the ones that failed, so a table
    #: written twice can be told from one written once.
    attempted: list = field(default_factory=list)
    counted: list = field(default_factory=list)
    persisted: list = field(default_factory=list)
    unpersisted: list = field(default_factory=list)
    views: list = field(default_factory=list)
    dropped_views: list = field(default_factory=list)
    identifier_case: list = field(default_factory=list)

    def __post_init__(self) -> None:
        self.catalog = _Catalog(self)
        self.conf = _Conf()
        #: Temporary views that are registered right now, by the key Spark holds
        #: each under. The one Spark rule this double models: a view is keyed by
        #: ``spark.sql.caseSensitive``'s normalisation of its name, so a name
        #: registered under one setting is looked up under the other.
        self.temporary: dict = {}

    def key(self, name: str) -> str:
        """The key a temporary view is held under, given the conf in force."""

        sensitive = str(self.conf.get("spark.sql.caseSensitive")).lower() == "true"
        return name if sensitive else name.lower()

    def register(self, name: str) -> None:
        self.views.append(name)
        self.temporary[self.key(name)] = name

    def resolve(self, text: str) -> None:
        """Refuse a statement naming a view that is not there to be found."""

        for written in self.views:
            if written in text and self.key(written) not in self.temporary:
                raise _ViewNotFound(f"[TABLE_OR_VIEW_NOT_FOUND] {written}")

    def table(self, name: str):
        return _Table(TARGET_COLUMNS)

    def sql(self, text: str) -> _Frame:
        self.resolve(text)
        if text.startswith(("CREATE TABLE", "DROP TABLE")):
            self.identifier_case.append(
                (text.split("`", 6)[5], self.conf.get("spark.sql.caseSensitive"))
            )
        if self.fail_on is not None and self.fail_on in text:
            raise _Boom(f"the engine failed on: {text.splitlines()[0]}")
        if text.startswith("CREATE TABLE"):
            self.attempted.append(text.split("`")[5])
            if self.fail_creates is True or (
                isinstance(self.fail_creates, str) and self.fail_creates in text
            ):
                raise _EvidenceRefused("the evidence could not be written")
        self.statements.append(text)
        return _Frame(self, text)

    def answer(self, text: str):
        if "GROUP BY `__weaver_operation`" in text:
            return [
                _Row(op="I", n=self.counts.get("inserted", 0)),
                _Row(op="U", n=self.counts.get("updated", 0)),
                _Row(op="D", n=self.counts.get("deleted", 0)),
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
        """Every statement that changes the target, whole and in order."""

        target = "`lh`.`DWG`.`Customer`"
        return [
            statement
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
        self._spark.temporary.pop(self._spark.key(name), None)


class _Conf:
    def __init__(self) -> None:
        self.values = {"spark.sql.caseSensitive": "false"}

    def get(self, key: str):
        return self.values[key]

    def set(self, key: str, value) -> None:
        self.values[key] = value


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
    """What ``read()`` handed over: a frame the load names but never persists.

    It answers no rows at all. A load that asked one of these what
    it held would be running a Spark job to learn what the contract already
    says, so being asked is a failure rather than an answer.
    """

    def __init__(self, columns=BUSINESS) -> None:
        self.columns = list(columns)

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark's name
        self.view = name

    def take(self, n: int):
        raise AssertionError("the load read a frame it was only meant to name")


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
AT_CHANGES = "weaver_proposed"
AT_DELETE_MUTATION = "WHEN MATCHED THEN DELETE"
#: The second mutation a busy load submits, so the deletes have already gone in
#: when it fails and the target is left halfway through the change.
AT_CHANGE_MUTATION = "WHEN MATCHED AND chg."


#: A load with rows to read, nothing refused and nothing to change.
NO_OP = {
    "staging": 3,
    "reject": 0,
    "inserted": 0,
    "updated": 0,
    "deleted": 0,
    "target": 3,
}

#: A load that inserts, updates and deletes.
BUSY = {
    "staging": 3,
    "reject": 0,
    "inserted": 1,
    "updated": 1,
    "deleted": 2,
    "target": 9,
}

#: An incremental load's own claim, whose size is the delete relation's own.
CLAIMED = {"staging": 3, "reject": 0, "inserted": 1, "updated": 1, "delete": 2}


def _incremental(**overrides) -> LoadContract:
    return _contract(incremental=True, **overrides)


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
    """Staging, the rejects, and everything the load decided to do.

    Three relations where there were four: the delete set and the upsert set
    asked the target the same question, so they are one classification now, and
    the keys to remove are a projection of it rather than a phase of their own.
    """

    spark, _result = _load(BUSY)

    assert [frame.role for frame in spark.persisted] == [
        "staging",
        "reject",
        "change",
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
def test_no_delete_mutation_when_nothing_is_being_removed():
    """A zero-row merge is a Delta commit and a scan for work that is not there."""

    spark, _result = _load(dict(NO_OP, inserted=1))

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
    """Guards the two tests above from passing because the load stopped working.

    Two statements, not three. The inserts and the updates were an insert and a
    merge over one relation; they are one merge whose clauses read the operation
    the classification already settled.
    """

    spark, _result = _load(BUSY)

    assert len(spark.mutations) == 2
    assert "WHEN MATCHED THEN DELETE" in spark.mutations[0]
    written = spark.mutations[1]
    assert written.startswith("MERGE INTO")
    assert "WHEN MATCHED AND chg.`__weaver_operation` = 'U' THEN UPDATE" in written
    assert "WHEN NOT MATCHED AND chg.`__weaver_operation` = 'I' THEN INSERT" in written


@weaver_test()
def test_the_one_merge_leaves_the_deletes_out_of_what_it_writes():
    """A delete row carries a key and no values, and this statement writes rows."""

    spark, _result = _load(BUSY)

    assert "WHERE `__weaver_operation` <> 'D'" in spark.mutations[1]


@weaver_test()
def test_an_insert_only_load_submits_the_same_one_merge():
    spark, result = _load(dict(NO_OP, inserted=2))

    assert result.rows_inserted == 2
    assert len(spark.mutations) == 1
    assert spark.mutations[0].startswith("MERGE INTO")


@weaver_test()
def test_an_update_only_load_submits_the_same_one_merge():
    spark, result = _load(dict(NO_OP, updated=2))

    assert result.rows_updated == 2
    assert len(spark.mutations) == 1
    assert spark.mutations[0].startswith("MERGE INTO")


@weaver_test()
def test_the_merge_stamps_an_insert_and_leaves_an_updated_rows_insert_time():
    """The audit contract, unchanged by the two statements becoming one.

    A new row is stamped inserted, updated and live. A changed row has its update
    and delete stamps refreshed and keeps the insert time it already had, so the
    insert column is absent from the update clause.
    """

    spark, _result = _load(BUSY)

    written = spark.mutations[1]
    update = written.split("THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "row_insert_datetime" not in update
    assert "`row_update_datetime` = current_timestamp()" in update
    assert "`row_signature` = chg.`row_signature`" in update
    assert "t.`Customer id` =" not in update, "the key is what was matched on"

    insert = written.split("THEN INSERT")[1]
    assert "`row_insert_datetime`, `row_update_datetime`, `row_delete_datetime`" in (
        insert
    )
    assert insert.count("current_timestamp()") == 2
    assert "`row_signature`" in insert


# --- the counts ---------------------------------------------------------------


@weaver_test()
def test_all_three_counts_come_from_one_pass():
    """The relation says what each row is, so the operation partitions it.

    Where a delete set was counted and an upsert set grouped, one grouped pass
    answers all three, and the relation it materialises is the one the gates and
    the mutations then read.
    """

    spark, result = _load(BUSY)

    assert (result.rows_inserted, result.rows_updated, result.rows_deleted) == (1, 1, 2)
    # Staging and the rejects. The classification is not counted separately: the
    # grouped pass is what materialises it.
    assert spark.counted == ["staging", "reject"]
    grouped = [
        one for one in spark.statements if "GROUP BY `__weaver_operation`" in one
    ]
    assert len(grouped) == 1


@weaver_test()
def test_rows_deleted_is_what_was_classified_and_needs_no_target_recount():
    """Those keys are already narrowed to ones the target holds.

    They are also disjoint from the rows written, so their number is what the
    target lost. Counting the target afterwards asked Delta the same question
    twice.
    """

    spark, result = _load(BUSY)

    assert result.rows_deleted == 2
    assert _target_counts(spark) == []


@weaver_test()
def test_the_target_is_not_counted_when_the_stability_gate_is_ignored():
    """The only reason to ask its size is the gate that reads it."""

    spark, _result = _load(BUSY, ignore_stability_threshold=True)

    assert not _target_counts(spark)


def _target_counts(spark) -> list[str]:
    """Every statement that asked the target how many rows it holds."""

    return [
        one
        for one in spark.statements
        if "FROM `lh`.`DWG`.`Customer`" in one and "count(*) AS n" in one
    ]


@weaver_test()
def test_a_change_too_small_to_breach_never_asks_the_target_its_size():
    """The gate cannot fire, so the action that feeds it is not submitted.

    At the declared defaults a delete has to pass 5% and an update 20% of a
    target of at least a million rows. Two deletes cannot do that to any target
    the gate applies to, so its size is not a question worth an action.
    """

    spark, result = _load(BUSY)

    assert result.succeeded
    assert _target_counts(spark) == []


@weaver_test()
def test_a_change_large_enough_to_breach_asks_and_then_the_gate_decides():
    """Past the precondition the existing calculation is what answers.

    Nine deletes against a threshold of 1% of one row could breach, so the size
    is read; the target turns out to hold ten rows, and 90% is over the limit.
    """

    spark = _refused(
        dict(BUSY, deleted=9, target=10),
        contract=_contract(delete_threshold=1, stability_rows=1),
    )

    assert len(_target_counts(spark)) == 1
    assert spark.mutations == []


@weaver_test()
def test_the_precondition_holds_the_boundary_the_gate_holds():
    """Strictly over, in both, so a count exactly on the line is not a breach."""

    contract = _contract(delete_threshold=10, update_threshold=10, stability_rows=100)

    assert not contract.may_breach(deleting=10, updating=0), "10% is not over 10%"
    assert contract.may_breach(deleting=11, updating=0)
    assert not contract.may_breach(deleting=0, updating=10)
    assert contract.may_breach(deleting=0, updating=11)
    # And what it lets through, the gate itself still refuses at the boundary.
    assert contract.breaches(target_rows=100, deleting=10, updating=0) is None
    assert contract.breaches(target_rows=100, deleting=11, updating=0)


@weaver_test()
def test_a_change_that_could_breach_a_small_target_is_still_asked_about():
    """The precondition is judged at the smallest target the gate acts on.

    A load whose object lowered ``Stability rows`` has a much smaller bar to
    clear, and the shortcut must not hide a breach it would have found.
    """

    spark, result = _load(
        dict(BUSY, deleted=2, target=3),
        contract=_contract(delete_threshold=5, stability_rows=10),
    )

    assert result.succeeded, "3 rows is under the stability floor"
    assert len(_target_counts(spark)) == 1, "but that is for the gate to say"


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
def test_mixed_case_runtime_tables_are_created_in_an_exact_case_scope():
    """Each physical create preserves its name, then restores the session."""

    spark, _result = _load(
        dict(BUSY, reject=2, clean=1),
        contract=_contract(object_id=ObjectId("DWG", "CustomerOrder")),
        fault_tolerant=True,
    )

    assert spark.identifier_case == [
        ("CustomerOrder_Reject", "false"),
        ("CustomerOrder_Delete", "false"),
        ("CustomerOrder_Staging", "false"),
        ("CustomerOrder_Staging", "true"),
        ("CustomerOrder_Reject", "true"),
        ("CustomerOrder_Delete", "true"),
    ]
    assert spark.conf.get("spark.sql.caseSensitive") == "false"


@weaver_test()
def test_a_mixed_case_evidence_write_still_finds_the_relation_it_reads():
    """The durable write is made inside the exact-case scope and reads a view.

    Spark holds a temporary view under ``spark.sql.caseSensitive``'s
    normalisation of its name, and this statement is the one place a load reads a
    view under a setting other than the one that registered it. Weaver names its
    working relations so the two settings make the same key.
    """

    spark = _refused(
        dict(NO_OP, reject=2),
        contract=_contract(object_id=ObjectId("DWG", "CustomerOrder")),
        match="fault_tolerant = 0",
    )

    assert spark.created == ["CustomerOrder_Staging", "CustomerOrder_Reject"]
    assert spark.views == [view.lower() for view in spark.views]


@weaver_test()
def test_the_delete_evidence_is_the_keys_and_not_the_change_relation():
    """``_Delete`` answers what the load was going to remove, as it always has.

    The classification carries an operation and the values a write needs. What a
    reader needs from ``_Delete`` is the keys, so it is written from a projection
    rather than from the relation the load happens to hold internally.
    """

    spark = _refused(
        dict(BUSY, deleted=9, target=10),
        contract=_contract(delete_threshold=1, stability_rows=1),
    )

    written = next(
        one
        for one in spark.statements
        if one.startswith("CREATE TABLE") and "Customer_Delete`" in one
    )
    source = next(
        one
        for one in spark.statements
        if one.startswith("SELECT `Customer id` FROM weaver_change_")
    )
    assert "FROM weaver_delete_" in written
    assert "`__weaver_operation` = 'D'" in source


@weaver_test()
def test_a_tolerated_refusal_that_deletes_nothing_leaves_no_delete_table():
    """Evidence is what gets used, not a full set for symmetry."""

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
        "change",
    ]
    # Released as soon as the survivors are materialised, not held to the end.
    assert spark.unpersisted[0].startswith("weaver_staging_")


@weaver_test()
def test_a_stability_breach_leaves_staging_and_what_it_would_have_removed():
    """Refused before writing, so the evidence is the proposal itself."""

    spark = _refused(
        dict(BUSY, deleted=9, target=10),
        contract=_contract(delete_threshold=1, stability_rows=1),
    )

    assert spark.created == ["Customer_Staging", "Customer_Delete"]
    assert spark.mutations == []


@weaver_test()
def test_a_merge_conflict_leaves_staging_and_what_it_would_have_removed():
    """Fatal whatever fault_tolerant says, and it writes nothing to the target."""

    spark = _refused(
        dict(CLAIMED, conflicts=1),
        contract=_incremental(unique_keys=(("Email",),)),
        deletes=_Staged(("Customer id",)),
        fault_tolerant=True,
        match="declared unique key",
    )

    assert spark.created == ["Customer_Staging", "Customer_Delete"]
    assert spark.mutations == []
    assert spark.leaked == []


# --- what an incremental load does not ask ------------------------------------


@weaver_test()
def test_an_incremental_load_with_no_claim_derives_no_delete_relation():
    """Absence proves nothing, so with no claim there is nothing to remove.

    An empty relation derived to say so was a Spark job answering a question the
    contract had already answered.
    """

    spark, result = _load(dict(NO_OP, inserted=1), contract=_incremental())

    assert result.rows_deleted == 0
    assert [frame.role for frame in spark.persisted] == ["staging", "reject", "change"]
    assert not any("weaver_delete_" in one for one in spark.statements)
    assert not any("WHEN MATCHED THEN DELETE" in one for one in spark.mutations)


@weaver_test()
def test_an_incremental_load_with_a_claim_settles_it_as_its_own_relation():
    """A claim is a statement about the target, and it is narrowed before it runs.

    Only keys the target holds, and only keys clean staging no longer carries.
    """

    spark, result = _load(
        CLAIMED, contract=_incremental(), deletes=_Staged(("Customer id",))
    )

    assert result.rows_deleted == 2
    assert [frame.role for frame in spark.persisted] == [
        "staging",
        "reject",
        "change",
        "delete",
    ]
    assert spark.counted == ["staging", "reject", "delete"]
    claim = next(
        one for one in spark.statements if one.startswith("SELECT t.`Customer")
    )
    assert "SELECT DISTINCT `Customer id` FROM weaver_delete_keys_" in claim
    assert "WHERE NOT EXISTS" in claim


@weaver_test()
def test_an_incremental_load_classifies_from_staging_alone():
    """Absence from an incremental source is not a deletion, so the join is left."""

    spark, _result = _load(dict(NO_OP, inserted=1), contract=_incremental())

    changes = next(one for one in spark.statements if "weaver_proposed AS (" in one)
    assert "LEFT JOIN `lh`.`DWG`.`Customer` AS t" in changes
    assert "FULL OUTER JOIN" not in changes
    assert "'D'" not in changes


@weaver_test()
def test_a_non_incremental_load_classifies_against_the_whole_target():
    """The target becomes clean staging, so one outer join answers all three."""

    spark, _result = _load(BUSY)

    changes = next(one for one in spark.statements if "weaver_proposed AS (" in one)
    assert "FULL OUTER JOIN `lh`.`DWG`.`Customer` AS t" in changes
    assert "THEN 'I'" in changes and "THEN 'D'" in changes and "ELSE 'U'" in changes
    # A key either side may be missing, so it comes from whichever side has it.
    assert "coalesce(q.`Customer id`, t.`Customer id`) AS `Customer id`" in changes


# --- what a table may return -------------------------------------------------


@weaver_test()
def test_a_non_incremental_load_refuses_a_delete_claim_before_anything_runs():
    """The rule the authoring surface holds, held again where a load starts.

    The source is the whole truth, so a second value states that twice. Refused
    on the claim being there: reading it to find out whether it held rows would
    be a Spark job run to learn that it was empty, and ``_Staged`` fails rather
    than answering one.
    """

    spark = _Spark(counts=NO_OP)

    with pytest.raises(LoadError, match="returns staging on its own"):
        load_table(
            spark,
            contract=_contract(),
            lakehouse=_Lakehouse(),
            staging_frame=_Staged(),
            deletes=_Staged(("Customer id",)),
        )

    assert spark.statements == [], "the load had already started work"


@weaver_test()
def test_an_incremental_load_takes_a_claim_without_reading_it():
    """A claim is a claim. What the target loses is settled by the load."""

    spark, result = _load(
        CLAIMED, contract=_incremental(), deletes=_Staged(("Customer id",))
    )

    assert result.succeeded


# --- evidence a failure with no outcome of its own leaves ---------------------


@weaver_test()
def test_a_failure_after_staging_settles_leaves_what_the_source_proposed():
    """An engine error is not a refusal, and the load still owes an explanation.

    Staging has been materialised, so the proposal can be read afterwards even
    though no gate decided anything about it.
    """

    spark = _failed(NO_OP, fail_on=AT_CHANGES)

    assert spark.created == ["Customer_Staging"]
    assert spark.mutations == []


@weaver_test()
def test_the_staging_evidence_a_failure_leaves_is_the_raw_proposal():
    """Raw staging, which is what the question ``_Staging`` answers."""

    spark = _failed(NO_OP, fail_on=AT_CHANGES)

    written = [one for one in spark.statements if one.startswith("CREATE TABLE")]
    assert len(written) == 1
    assert "FROM weaver_staging_" in written[0]


@weaver_test()
def test_a_failure_while_the_changes_settle_leaves_only_the_proposal():
    """Nothing was classified, so the proposal is all there is to leave.

    The deletions are settled by the same statement as the writes now, so a
    failure inside it has no delete set of its own to show. What the load was
    going to remove appears once that statement has answered, which is what
    :func:`test_a_failure_before_the_first_mutation_leaves_a_target_no_one_touched`
    reads.
    """

    spark = _failed(BUSY, fail_on=AT_CHANGES)

    assert spark.created == ["Customer_Staging"]


@weaver_test()
def test_a_failure_before_the_first_mutation_leaves_a_target_no_one_touched():
    """Nothing had moved, so the evidence is the whole of what was proposed."""

    spark = _failed(BUSY, fail_on=AT_DELETE_MUTATION)

    assert spark.mutations == []
    assert spark.created == ["Customer_Staging", "Customer_Delete"]


@weaver_test()
def test_a_failure_partway_through_the_target_leaves_the_same_evidence():
    """The deletes went in and the upserts did not, which is what to look at.

    A target halfway through a change is the case the evidence matters most for:
    the delete set says which rows are already gone.
    """

    spark = _failed(BUSY, fail_on=AT_CHANGE_MUTATION)

    # One mutation ran and the next did not, so the target is partway
    # through rather than untouched.
    assert len(spark.mutations) == 1
    assert "WHEN MATCHED THEN DELETE" in spark.mutations[0]
    assert spark.created == ["Customer_Staging", "Customer_Delete"]
    # Written on the way out, after the mutation that got through.
    written_at = next(
        index
        for index, one in enumerate(spark.statements)
        if one.startswith("CREATE TABLE") and "Customer_Delete`" in one
    )
    deleted_at = next(
        index
        for index, one in enumerate(spark.statements)
        if "WHEN MATCHED THEN DELETE" in one
    )
    assert written_at > deleted_at


@weaver_test()
def test_a_failure_after_a_tolerated_rejection_keeps_the_rejects_it_wrote():
    """The gate's own evidence stands, and the failure adds what it settled since.

    ``_Staging`` is still the raw proposal the gate wrote, not the survivors the
    purge left, and it is written once.
    """

    spark = _failed(
        dict(BUSY, reject=2, clean=1),
        fail_on=AT_DELETE_MUTATION,
        fault_tolerant=True,
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

    spark = _failed(BUSY, fail_on=AT_CHANGE_MUTATION)

    assert not any("_Upsert" in one for one in spark.statements)


@weaver_test()
def test_evidence_that_cannot_be_written_does_not_replace_the_failure():
    """The engine error is the one worth reporting, so the write is attempted only.

    ``_failed`` asserts the type: an evidence failure reaching the caller would
    be reported instead of the reason the load stopped.
    """

    spark = _failed(BUSY, fail_on=AT_CHANGES, fail_creates=True)

    assert spark.created == []


@weaver_test()
def test_a_failed_load_gives_every_relation_back():
    """Release is in a finally, and the evidence write runs before it."""

    spark = _failed(BUSY, fail_on=AT_CHANGES)

    assert spark.leaked == []
    assert set(spark.unpersisted) == {frame.view for frame in spark.persisted}


@weaver_test()
def test_a_failed_load_whose_evidence_failed_gives_every_relation_back():
    spark = _failed(BUSY, fail_on=AT_CHANGES, fail_creates=True)

    assert spark.leaked == []
    assert set(spark.unpersisted) == {frame.view for frame in spark.persisted}


@weaver_test()
def test_a_durable_write_that_failed_does_not_count_as_written():
    """A gate's write failing leaves the role to be attempted again on the way out.

    The load tracks what it has written so it does not write it twice. If that
    record were made before the write rather than after it, a table that never
    arrived would be treated as evidence that is already there.
    """

    spark = _Spark(
        counts=dict(BUSY, reject=2, clean=1), fail_creates="Customer_Staging"
    )
    with pytest.raises(_EvidenceRefused):
        load_table(
            spark,
            contract=_contract(),
            lakehouse=_Lakehouse(),
            staging_frame=_Staged(),
            fault_tolerant=True,
        )

    assert spark.attempted == [
        "Customer_Staging",
        "Customer_Staging",
        "Customer_Reject",
    ]
    assert spark.created == ["Customer_Reject"]
    assert spark.leaked == []
