"""What a validation claims as an installed artefact, and how it is signed.

The sibling of ``test_load_artefacts_representation.py``, and the claims are the same
claims because the lifecycle is the same lifecycle: one artefact per validation,
a deterministic identity, a signature that changes when it should and only then,
and a role carried rather than inferred from a shape.

Nothing here builds or installs. An artefact is a claim about what *should* be
installed, derived from source alone, which is what lets a deleted validation
produce an ordinary prune instead of needing a scan to notice its module is
orphaned.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.etl import (
    ROLE_ASSUMPTION,
    ROLE_LOAD,
    ROLE_TEST,
    item_runtime_artefacts,
    item_validation_artefacts,
    load_schemas,
    runtime_artefacts,
)
from weaver.locations import Location
from weaver.spark import FabricSparkTarget

SALES = FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH")

LAKEHOUSE = WeaverItemId.parse("Lakehouse/Sales")
WAREHOUSE = WeaverItemId.parse("Warehouse/Reporting")

SCHEMA = "Schema ID: Sales\nDescription: Sales objects.\n"

ORDER = '''"""
Table ID: Sales.Order

Description: Orders.

Lineage: A source system.

Primary key: Id

Schema:
  Id: string
"""
from weaver import Table


class Sales__Order(Table):
    def read(self):
        return [], []
'''

PYTHON_TEST = '''"""
Test ID: Sales.OrdersReconcile

Description: Orders reconcile to the independent calculation.

Primary key: Id
"""
from weaver import Test


class Sales__OrdersReconcile(Test):
    def expected(self):
        return None

    def actual(self):
        return None
'''

SPARK_ASSUMPTION = """/*
Assumption ID: Sales.NoOrphans

Description: No orphan orders.
*/
select Id from Sales.Order where Id is null;
"""

WAREHOUSE_TABLE = """/*
Table ID: Sales.Report

Description: A report.

Lineage: A source system.
*/
select 1 as Id;
"""

WAREHOUSE_TEST = """/*
Test ID: Sales.Reconciles

Description: The report reconciles.

Primary key: Id
*/
select Id from Sales.Report;

select Id from Sales.Report;
"""


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="")


def _estate(root: Path, **replacements: str) -> Path:
    files = {
        "Lakehouse/Sales/schemas/Sales.yml": SCHEMA,
        "Lakehouse/Sales/Sales__Order.py": ORDER,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py": PYTHON_TEST,
        "Lakehouse/Sales/assumptions/Sales.NoOrphans.sql": SPARK_ASSUMPTION,
        "Warehouse/Reporting/schemas/Sales.yml": SCHEMA,
        "Warehouse/Reporting/Sales.Report.sql": WAREHOUSE_TABLE,
        "Warehouse/Reporting/tests/Sales.Reconciles.sql": WAREHOUSE_TEST,
    }
    files.update(replacements)
    for relative, text in files.items():
        _write(root, relative, text)
    return root


def _parse(root: Path):
    return parse_item_repository(Location(str(root)))


@pytest.fixture
def estate(tmp_path):
    return _parse(_estate(tmp_path))


def _by_role(artefacts):
    return {str(artefact.identity): artefact.role for artefact in artefacts}


# --- deterministic identity ---------------------------------------------------


@weaver_test()
def test_a_warehouse_validation_claims_a_procedure_in_the_generated_schema(estate):
    artefacts = item_validation_artefacts(estate, item=WAREHOUSE, destination=SALES)

    assert _by_role(artefacts) == {
        "Warehouse/Reporting/procedure:_/Test Sales.Reconciles": ROLE_TEST
    }
    assert artefacts[0].object_type == "stored_procedure"


@weaver_test()
def test_a_lakehouse_validation_claims_a_module_under_the_runtime_root(estate):
    """Under it, not beside it: that root is the item's Python import root."""

    assert _by_role(
        item_validation_artefacts(estate, item=LAKEHOUSE, destination=SALES)
    ) == {
        "Lakehouse/Sales/file:_/Load/assumptions/Sales__NoOrphans.py": ROLE_ASSUMPTION,
        "Lakehouse/Sales/file:_/Load/tests/Sales__OrdersReconcile.py": ROLE_TEST,
    }


@weaver_test()
def test_the_subdirectory_follows_the_kind(estate):
    paths = [
        artefact.identity.object_id.schema
        for artefact in item_validation_artefacts(
            estate, item=LAKEHOUSE, destination=SALES
        )
    ]

    assert sorted(paths) == ["_/Load/assumptions", "_/Load/tests"]


@weaver_test()
def test_a_python_validation_is_deployed_rather_than_generated(estate):
    """The module a developer wrote is already the primitive."""

    artefact = next(
        artefact
        for artefact in item_validation_artefacts(
            estate, item=LAKEHOUSE, destination=SALES
        )
        if artefact.role == ROLE_TEST
    )

    assert artefact.payload.decode("utf-8") == PYTHON_TEST


@weaver_test()
def test_a_spark_sql_validation_is_compiled_into_a_module(estate):
    artefact = next(
        artefact
        for artefact in item_validation_artefacts(
            estate, item=LAKEHOUSE, destination=SALES
        )
        if artefact.role == ROLE_ASSUMPTION
    )

    assert b"from weaver import SparkSqlAssumption" in artefact.payload


# --- one lifecycle ------------------------------------------------------------


@weaver_test()
def test_loads_and_validations_are_claimed_together(estate):
    roles = _by_role(item_runtime_artefacts(estate, item=LAKEHOUSE, destination=SALES))

    assert roles["Lakehouse/Sales/file:_/Load/Sales__Order.py"] == ROLE_LOAD
    assert (
        roles["Lakehouse/Sales/file:_/Load/tests/Sales__OrdersReconcile.py"]
        == ROLE_TEST
    )


@weaver_test()
def test_a_deployed_module_is_claimed_once(estate):
    """A Python validation is runtime source *and* a validation, not two files."""

    identities = [
        str(artefact.identity)
        for artefact in item_runtime_artefacts(
            estate, item=LAKEHOUSE, destination=SALES
        )
    ]

    assert len(identities) == len(set(identities))


@weaver_test()
def test_the_role_travels_with_the_artefact(estate):
    """Nothing downstream may recover it from a file or procedure shape.

    Four roles reach the Registry from here, and the shapes do not distinguish
    them: a load module and a Test module are both files, and a load procedure, a
    Test procedure and an entry point are all procedures.
    """

    from weaver.etl import ROLE_ENTRY

    for artefact in runtime_artefacts(estate):
        assert artefact.role in (ROLE_LOAD, ROLE_TEST, ROLE_ASSUMPTION, ROLE_ENTRY)
        assert artefact.is_validation == (artefact.role in (ROLE_TEST, ROLE_ASSUMPTION))


@weaver_test()
def test_a_validation_records_the_declaration_it_came_from(estate):
    artefact = next(
        artefact
        for artefact in item_validation_artefacts(
            estate, item=WAREHOUSE, destination=SALES
        )
        if artefact.role == ROLE_TEST
    )

    assert str(artefact.origin) == "Warehouse/Reporting/Sales.Reconciles"


# --- signatures ---------------------------------------------------------------


@weaver_test()
def test_an_edited_validation_changes_its_signature(tmp_path):
    before = item_validation_artefacts(_parse(_estate(tmp_path)), item=WAREHOUSE)
    after = item_validation_artefacts(
        _parse(
            _estate(
                tmp_path,
                **{
                    "Warehouse/Reporting/tests/Sales.Reconciles.sql": WAREHOUSE_TEST.replace(
                        "The report reconciles.", "The report reconciles exactly."
                    )
                },
            )
        ),
        item=WAREHOUSE,
    )

    assert before[0].signature != after[0].signature


@weaver_test()
def test_an_untouched_validation_keeps_its_signature(tmp_path):
    first = item_validation_artefacts(_parse(_estate(tmp_path)), item=WAREHOUSE)
    second = item_validation_artefacts(_parse(_estate(tmp_path)), item=WAREHOUSE)

    assert first[0].signature == second[0].signature


@weaver_test()
def test_the_generator_version_salts_a_generated_validation(tmp_path, monkeypatch):
    """Otherwise a changed generator leaves the old primitive installed, silently."""

    from weaver.declaration import validation

    before = item_validation_artefacts(_parse(_estate(tmp_path)), item=WAREHOUSE)
    monkeypatch.setattr(
        validation, "TSQL_VALIDATION_VERSION", validation.TSQL_VALIDATION_VERSION + 1
    )
    after = item_validation_artefacts(_parse(_estate(tmp_path)), item=WAREHOUSE)

    assert before[0].signature != after[0].signature


@weaver_test()
def test_a_deployed_python_validation_is_signed_by_its_own_bytes(tmp_path):
    """No salt: nothing generated it, so no generator version applies to it."""

    from weaver.declaration.source import content_hash

    artefact = next(
        artefact
        for artefact in item_validation_artefacts(
            _parse(_estate(tmp_path)), item=LAKEHOUSE
        )
        if artefact.role == ROLE_TEST
    )

    assert artefact.signature == content_hash(PYTHON_TEST.encode("utf-8"))


# --- the infrastructure a validation needs ------------------------------------


@weaver_test()
def test_a_validation_only_warehouse_still_gets_its_generated_schema(tmp_path):
    """Otherwise its procedure would have nowhere to be created."""

    _write(tmp_path, "Warehouse/Reporting/schemas/Sales.yml", SCHEMA)
    _write(
        tmp_path,
        "Warehouse/Reporting/tests/Sales.Reconciles.sql",
        # Reading nothing this item declares, which is the whole point: an item
        # that only validates still needs somewhere to put the procedure.
        WAREHOUSE_TEST.replace("Sales.Report", "Sales.Elsewhere").replace(
            "Primary key: Id", "Primary key: Id\n\nDependencies: []"
        ),
    )
    repository = _parse(tmp_path)

    artefacts = item_runtime_artefacts(repository, item=WAREHOUSE, destination=SALES)

    assert load_schemas(artefacts) == ("_",)


@weaver_test()
def test_a_validation_only_lakehouse_still_gets_its_runtime_tree(tmp_path):
    """Otherwise the folder its module lands in would not exist."""

    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", SCHEMA)
    _write(tmp_path, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", PYTHON_TEST)
    repository = _parse(tmp_path)
    item = next(model for model in repository.items if model.identity == LAKEHOUSE)

    assert "Lakehouse/Sales/Files/_.Load" in [
        str(identity) for identity in item.documents
    ]


@weaver_test()
def test_an_item_with_neither_gets_no_runtime_tree(tmp_path):
    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", SCHEMA)
    _write(
        tmp_path,
        "Lakehouse/Sales/Sales.View.sql",
        "/*\nView ID: Sales.View\nDescription: A view.\nLineage: A source.\n"
        "Dependencies: []\n*/\nselect 1 as Id;\n",
    )
    repository = _parse(tmp_path)
    item = next(model for model in repository.items if model.identity == LAKEHOUSE)

    assert "Lakehouse/Sales/Files/_.Load" not in [
        str(identity) for identity in item.documents
    ]


# --- deletion -----------------------------------------------------------------


@weaver_test()
def test_a_deleted_validation_stops_being_claimed(tmp_path):
    root = _estate(tmp_path)
    assert item_validation_artefacts(_parse(root), item=WAREHOUSE)

    (root / "Warehouse/Reporting/tests/Sales.Reconciles.sql").unlink()

    assert item_validation_artefacts(_parse(root), item=WAREHOUSE) == ()
