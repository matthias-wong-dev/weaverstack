"""The ``spark_table`` executor, driven with a fake capability: no JVM.

The install-time behaviour (describe the query, validate, create the table) is
proven end to end against a real Lakehouse in
``tests/fabric/test_spark_table_lakehouse_boundary.py``. These tests pin the
executor's own logic cheaply: what it asks Spark, what SQL it
generates, and that it surfaces every column violation the plan lists, without
paying for a Spark session.

The executor reaches Spark twice and only twice: once to describe the query's
shape, once to create the table. Everything between is decided here, from the
frozen payload.
"""

from __future__ import annotations

import json

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.spark_table import SparkTableExecutor
from weaver.build_bundle.models import InstallAction
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import BuildError, InstallError
from weaver.spark import FabricSparkTarget
from weaver.targets import ItemRef

DESCRIBE = "DESCRIBE QUERY "


class _Capability:
    """The Session's Spark SQL capability, answering DESCRIBE QUERY from a shape.

    It records every call, the statements it carried and the identifier-case
    scope they travelled under, because both are claims the executor makes.
    """

    def __init__(
        self,
        query_fields: list[tuple[str, str]],
        *,
        describe_error: Exception | None = None,
        create_error: Exception | None = None,
    ) -> None:
        self._fields = list(query_fields)
        self._describe_error = describe_error
        self._create_error = create_error
        #: One entry per call: ``(statements, exact_case)``.
        self.calls: list[tuple[list[str], bool]] = []

    def one(self, statement: str, *, exact_case: bool = False):
        return self.many([statement], exact_case=exact_case)

    def many(self, statements, *, exact_case: bool = False):
        ordered = list(statements)
        self.calls.append((ordered, exact_case))
        last = ordered[-1].lstrip()
        if last.upper().startswith(DESCRIBE):
            if self._describe_error is not None:
                raise self._describe_error
            return [
                {"col_name": name, "data_type": simple, "comment": None}
                for name, simple in self._fields
            ]
        if last.upper().startswith("CREATE") and self._create_error is not None:
            raise self._create_error
        return []

    @property
    def statements(self) -> list[str]:
        return [one for statements, _case in self.calls for one in statements]

    @property
    def described(self) -> str:
        return next(one for one in self.statements if one.upper().startswith(DESCRIBE))

    @property
    def created(self) -> str:
        return next(
            one for one in self.statements if one.lstrip().upper().startswith("CREATE")
        )


AUDIT = [
    ["row_insert_datetime", "timestamp", True],
    ["row_update_datetime", "timestamp", True],
    ["row_delete_datetime", "timestamp", True],
]


#: The destination every case here builds into. The payload arrives already
#: addressed to it, and this executor discovers the query's shape, never
#: where the table goes.
DESTINATION = FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH")
FABRIC_DESTINATION = FabricSparkTarget(workspace="Analytics", lakehouse="Sales_LH")

#: What `Sales.Customer` is called there.
CUSTOMER = "`Demo`.`Sales_LH`.`Sales`.`Customer`"
RAW = "`Demo`.`Sales_LH`.`Sales`.`Raw`"


def _payload(**overrides) -> bytes:
    payload = {
        "object": CUSTOMER,
        "schema_mode": "inferred",
        "declared_columns": None,
        "source_query": f"select CustomerId, CustomerName from {RAW}",
        "references": [["Primary key", "CustomerId"]],
        "audit_columns": AUDIT,
        "column_mapping": True,
    }
    payload.update(overrides)
    return (json.dumps(payload) + "\n").encode("utf-8")


def _action() -> InstallAction:
    return InstallAction(
        id="build-delta-Sales.Customer",
        kind="build_table",
        resource_node_id="delta:Sales.Customer",
        executor="spark_table",
        payload="payload/x.spark-table.json",
        payload_sha256="x",
    )


def _context(capability, destination):
    target = ResolvedTarget(
        bound=BoundTarget(
            id="lakehouse-Sales_LH", kind="lakehouse", item_id="Sales_LH"
        ),
        lakehouse=ItemRef("Sales_LH"),
        destination=destination,
    )
    return InstallationContext(
        resolver=None,
        store=None,
        target=target,
        spark_sql=None if capability is None else capability.one,
        spark_sql_batch=None if capability is None else capability.many,
    )


def _run(capability, payload: bytes, *, destination=DESTINATION):
    return SparkTableExecutor().execute(
        _action(), payload, _context(capability, destination)
    )


# --- what reaches Spark, and how often ----------------------------------------


@weaver_test()
def test_the_shape_is_asked_for_rather_than_the_query_being_run():
    """``DESCRIBE QUERY`` answers the two things the executor takes from a query.

    The names in order and each type as ``simpleString`` spells it, without
    running the query, and without a ``DataFrame`` to hold.
    """

    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload())

    assert capability.described == (
        f"DESCRIBE QUERY select CustomerId, CustomerName from {RAW}"
    )


@weaver_test()
def test_setup_and_describe_travel_as_one_piece_of_work():
    """A temporary view registered in one session and read in another is not
    there, so the setup goes with the describe that depends on it."""

    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(
        capability,
        _payload(
            setup=[f"CREATE OR REPLACE TEMPORARY VIEW staged AS SELECT * FROM {RAW}"],
            source_query="select CustomerId, CustomerName from staged",
        ),
    )

    shape, _case = capability.calls[0]
    assert shape == [
        f"CREATE OR REPLACE TEMPORARY VIEW staged AS SELECT * FROM {RAW}",
        "DESCRIBE QUERY select CustomerId, CustomerName from staged",
    ]


@weaver_test()
def test_a_table_is_built_in_exactly_two_reaches_for_spark():
    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload())

    assert len(capability.calls) == 2
    assert capability.calls[0][0][-1].startswith(DESCRIBE)
    assert capability.calls[1][0] == [capability.created]


@pytest.mark.parametrize(
    "destination",
    [FABRIC_DESTINATION, DESTINATION],
    ids=["fabric", "local"],
)
@weaver_test()
def test_the_shape_and_the_create_share_one_case_scope(destination):
    """A table created as ``CustomerEnriched`` has to be readable by the next
    action in the same build, so both halves are analysed the same way."""

    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload(), destination=destination)

    assert [exact_case for _statements, exact_case in capability.calls] == [True, True]


@weaver_test()
def test_nothing_is_dropped_to_make_room_for_a_case_variant():
    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload(), destination=FABRIC_DESTINATION)

    assert not any(
        one.lstrip().upper().startswith("DROP") for one in capability.statements
    )


# --- generation -------------------------------------------------------------


@weaver_test()
def test_inferred_table_uses_query_types_and_appends_not_null_audit_columns():
    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    details = _run(capability, _payload())

    statement = capability.created
    assert statement.startswith(f"CREATE TABLE {CUSTOMER} (\n")
    # CustomerId is the primary key, so it is not null even when inferred;
    # CustomerName is not, so it stays nullable.
    assert "`CustomerId` int NOT NULL" in statement
    assert "`CustomerName` string,\n" in statement
    assert "`CustomerName` string NOT NULL" not in statement
    # Every audit column is not null.
    assert "`row_insert_datetime` timestamp NOT NULL" in statement
    assert "`row_update_datetime` timestamp NOT NULL" in statement
    assert "`row_delete_datetime` timestamp NOT NULL" in statement
    assert "USING delta" in statement
    assert "delta.columnMapping.mode" in statement
    assert details["columns"][:2] == ["CustomerId", "CustomerName"]


@weaver_test()
def test_creation_names_the_destination_the_payload_was_addressed_to():
    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload())

    assert capability.created.startswith(f"CREATE TABLE {CUSTOMER}")


@weaver_test()
def test_a_complex_query_type_reaches_the_created_table_unchanged():
    """Whatever ``DESCRIBE QUERY`` spells the type, that is the column's type."""

    capability = _Capability(
        [
            ("CustomerId", "int"),
            ("Balance", "decimal(18,2)"),
            ("SeenAt", "timestamp"),
            ("Lines", "array<struct<amount:decimal(9,3)>>"),
            ("Tags", "map<string,int>"),
        ]
    )
    _run(capability, _payload(references=[]))

    statement = capability.created
    assert "`Balance` decimal(18,2)" in statement
    assert "`SeenAt` timestamp" in statement
    assert "`Lines` array<struct<amount:decimal(9,3)>>" in statement
    assert "`Tags` map<string,int>" in statement


@weaver_test()
def test_the_not_null_header_marks_inferred_columns_not_null():
    capability = _Capability(
        [("CustomerId", "int"), ("CustomerName", "string"), ("Note", "string")]
    )
    _run(
        capability,
        _payload(
            references=[["Primary key", "CustomerId"], ["Not null", "CustomerName"]],
        ),
    )
    statement = capability.created
    # The primary key and the Not null column are not null; Note is nullable.
    assert "`CustomerId` int NOT NULL" in statement
    assert "`CustomerName` string NOT NULL" in statement
    assert "`Note` string,\n" in statement
    assert "`Note` string NOT NULL" not in statement


@weaver_test()
def test_a_delta_table_is_built_with_no_identity_column():
    """Identity is a Warehouse declaration, so nothing here materialises one.

    The parser refuses ``Identity`` on a Delta table
    (:data:`weaver.declaration.metadata.IDENTITY_LANGUAGES`), so the executor has
    no identity case to handle: the created table is the business columns and the
    audit columns, and nothing else.
    """

    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload())
    statement = capability.created
    assert statement.startswith(f"CREATE TABLE {CUSTOMER} (\n    `CustomerId` int")
    assert "identity" not in statement.lower()
    assert "generated" not in statement.lower()


@weaver_test()
def test_declared_table_uses_declared_types_and_nullability_not_the_query():
    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(
        capability,
        _payload(
            schema_mode="declared",
            declared_columns=[
                ["CustomerId", "bigint", True],
                ["CustomerName", "string", False],
            ],
        ),
    )
    statement = capability.created
    # The declaration asked for bigint NOT NULL; the query's int is ignored.
    assert "`CustomerId` bigint NOT NULL" in statement
    assert "`CustomerName` string,\n" in statement


@weaver_test()
def test_column_names_are_case_sensitive_against_the_declaration():
    capability = _Capability([("customerid", "int")])
    with pytest.raises(
        BuildError, match="not returned by the query under the same case"
    ):
        _run(
            capability,
            _payload(
                schema_mode="declared",
                declared_columns=[["CustomerId", "bigint", True]],
                references=[],
            ),
        )


# --- validation failures the plan enumerates --------------------------------


@weaver_test()
def test_a_declared_column_missing_from_the_query_fails_install():
    capability = _Capability([("CustomerId", "int")])
    with pytest.raises(
        BuildError, match="not returned by the query under the same case: CustomerName"
    ):
        _run(
            capability,
            _payload(
                schema_mode="declared",
                declared_columns=[
                    ["CustomerId", "bigint", True],
                    ["CustomerName", "string", False],
                ],
                references=[],
            ),
        )


@weaver_test()
def test_an_undeclared_extra_query_column_fails_install():
    capability = _Capability([("CustomerId", "int"), ("Extra", "string")])
    with pytest.raises(BuildError, match="not in the declared schema"):
        _run(
            capability,
            _payload(
                schema_mode="declared",
                declared_columns=[["CustomerId", "bigint", True]],
                references=[],
            ),
        )


@weaver_test()
def test_case_colliding_query_output_names_fail_install():
    capability = _Capability([("CustomerId", "int"), ("customerid", "bigint")])
    with pytest.raises(BuildError, match="collide by name"):
        _run(capability, _payload(references=[]))


@weaver_test()
def test_a_primary_key_naming_a_missing_column_fails_install():
    capability = _Capability([("CustomerName", "string")])
    with pytest.raises(BuildError, match="Primary key names column 'CustomerId'"):
        _run(capability, _payload())


@weaver_test()
def test_a_query_column_colliding_with_an_audit_column_is_refused():
    capability = _Capability([("CustomerId", "int"), ("row_insert_datetime", "string")])
    with pytest.raises(InstallError, match="reserved for Weaver's audit columns"):
        _run(capability, _payload(references=[]))


@weaver_test()
def test_a_query_that_does_not_resolve_names_the_action_and_carries_spark():
    """The failure moved from running the query to describing it, and it still
    has to say which action failed and what Spark said about it."""

    capability = _Capability(
        [], describe_error=RuntimeError("[UNRESOLVED_COLUMN] `NoSuchColumn`")
    )

    with pytest.raises(InstallError) as raised:
        _run(capability, _payload())

    message = str(raised.value)
    assert "build-delta-Sales.Customer" in message
    assert CUSTOMER in message
    assert "UNRESOLVED_COLUMN" in message
    assert not any(
        one.lstrip().upper().startswith("CREATE") for one in capability.statements
    )


@weaver_test()
def test_a_query_producing_no_columns_is_refused():
    capability = _Capability([])

    with pytest.raises(InstallError, match="produces no columns"):
        _run(capability, _payload())


@weaver_test()
def test_a_failing_create_is_not_swallowed():
    capability = _Capability(
        [("CustomerId", "int"), ("CustomerName", "string")],
        create_error=RuntimeError("create failed"),
    )

    with pytest.raises(RuntimeError, match="create failed"):
        _run(capability, _payload())


@weaver_test()
def test_no_way_to_run_a_statement_is_a_clear_install_error():
    with pytest.raises(InstallError, match="no Spark SQL capability"):
        _run(None, _payload())
