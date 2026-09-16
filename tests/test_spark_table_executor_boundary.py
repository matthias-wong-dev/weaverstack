"""The ``spark_table`` executor, driven with a fake capability: no JVM.

The install-time behaviour (describe the query, validate, create the table) is
proven end to end against a real Lakehouse in
``tests/fabric/test_spark_table_lakehouse_boundary.py``. These tests pin the
executor's own logic cheaply: what it asks Spark, what SQL it
generates, and that it surfaces every column violation the plan lists, without
paying for a Spark session.

The executor reaches Spark twice for a Spark SQL declaration: once to describe the
query's shape, then once through Session-owned TableBuilder creation. A Python
declaration already has its shape and reaches Spark once.
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
        self.creations: list[dict] = []

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
        return []

    def create(
        self,
        qualified_name,
        columns,
        *,
        identity_column=None,
        column_mapping=True,
        validate_only=False,
    ):
        specification = {
            "object": qualified_name,
            "columns": [tuple(column) for column in columns],
            "identity_column": identity_column,
            "column_mapping": column_mapping,
            "validate_only": validate_only,
        }
        self.creations.append(specification)
        if self._create_error is not None:
            raise self._create_error
        return {"created": qualified_name}

    @property
    def statements(self) -> list[str]:
        return [one for statements, _case in self.calls for one in statements]

    @property
    def described(self) -> str:
        return next(one for one in self.statements if one.upper().startswith(DESCRIBE))


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
        "identity_column": None,
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
        spark_sql_batch=None if capability is None else capability.many,
        create_delta_table=None if capability is None else capability.create,
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

    assert len(capability.calls) == 1
    assert capability.calls[0][0][-1].startswith(DESCRIBE)
    assert len(capability.creations) == 1


@pytest.mark.parametrize(
    "destination",
    [FABRIC_DESTINATION, DESTINATION],
    ids=["fabric", "local"],
)
@weaver_test()
def test_the_shape_uses_exact_case_and_creation_uses_the_session(destination):

    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload(), destination=destination)

    assert [exact_case for _statements, exact_case in capability.calls] == [True]
    assert capability.creations[0]["object"] == CUSTOMER


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

    created = capability.creations[0]
    assert created["columns"] == [
        ("CustomerId", "int", True),
        ("CustomerName", "string", False),
        ("row_insert_datetime", "timestamp", True),
        ("row_update_datetime", "timestamp", True),
        ("row_delete_datetime", "timestamp", True),
    ]
    assert created["column_mapping"] is True
    assert details["columns"][:2] == ["CustomerId", "CustomerName"]


@weaver_test()
def test_creation_names_the_destination_the_payload_was_addressed_to():
    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload())

    assert capability.creations[0]["object"] == CUSTOMER


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

    types = {
        name: type_ for name, type_, _not_null in capability.creations[0]["columns"]
    }
    assert types["Balance"] == "decimal(18,2)"
    assert types["SeenAt"] == "timestamp"
    assert types["Lines"] == "array<struct<amount:decimal(9,3)>>"
    assert types["Tags"] == "map<string,int>"


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
    columns = capability.creations[0]["columns"]
    assert columns[:3] == [
        ("CustomerId", "int", True),
        ("CustomerName", "string", True),
        ("Note", "string", False),
    ]


@weaver_test()
def test_a_delta_table_is_built_with_no_identity_column():
    capability = _Capability([("CustomerId", "int"), ("CustomerName", "string")])
    _run(capability, _payload())
    created = capability.creations[0]
    assert created["identity_column"] is None
    assert created["columns"][0] == ("CustomerId", "int", True)


@weaver_test()
def test_python_table_uses_declared_shape_without_query_inference():
    capability = _Capability([])

    _run(
        capability,
        _payload(
            schema_mode="declared",
            declared_columns=[
                ["CustomerId", "string", True],
                ["CustomerName", "string", False],
            ],
            source_query=None,
            setup=[],
        ),
    )

    assert capability.calls == []
    assert capability.creations[0]["columns"][:2] == [
        ("CustomerId", "string", True),
        ("CustomerName", "string", False),
    ]


@weaver_test()
def test_identity_leads_the_physical_shape_and_is_marked_for_creation():
    capability = _Capability([("CustomerId", "string"), ("CustomerName", "string")])

    details = _run(
        capability,
        _payload(identity_column=["CustomerKey", "bigint", True]),
    )

    created = capability.creations[0]
    assert created["identity_column"] == "CustomerKey"
    assert created["columns"][:3] == [
        ("CustomerKey", "bigint", True),
        ("CustomerId", "string", True),
        ("CustomerName", "string", False),
    ]
    assert details["columns"][0] == "CustomerKey"


@weaver_test()
def test_inferred_identity_is_available_to_column_metadata_validation():
    capability = _Capability([("CustomerId", "string"), ("CustomerName", "string")])

    _run(
        capability,
        _payload(
            identity_column=["CustomerKey", "bigint", True],
            references=[
                ["Primary key", "CustomerId"],
                ["Column notes", "CustomerKey"],
            ],
        ),
    )

    assert capability.creations[0]["identity_column"] == "CustomerKey"


@weaver_test()
def test_inferred_query_output_may_not_collide_with_identity():
    capability = _Capability([("CustomerId", "string"), ("CustomerKey", "bigint")])

    with pytest.raises(BuildError, match="Identity 'CustomerKey' duplicates"):
        _run(
            capability,
            _payload(identity_column=["CustomerKey", "bigint", True]),
        )

    assert capability.creations == []


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
    assert capability.creations[0]["columns"][:2] == [
        ("CustomerId", "bigint", True),
        ("CustomerName", "string", False),
    ]


@weaver_test()
def test_column_names_are_case_sensitive_against_the_declaration():
    capability = _Capability([("customerid", "int")])
    with pytest.raises(BuildError, match="does not return these declared columns"):
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
        BuildError, match="does not return these declared columns.*CustomerName"
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
    with pytest.raises(BuildError, match="ambiguous when compared case-insensitively"):
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
    assert capability.creations == []


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
def test_no_way_to_create_a_table_is_a_clear_install_error():
    with pytest.raises(InstallError, match="no Delta table creation capability"):
        _run(None, _payload())
