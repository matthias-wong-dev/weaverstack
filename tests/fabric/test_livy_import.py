"""Weaver running *inside* Fabric.

The other Fabric tests run Weaver on this machine and reach into a workspace
over HTTP. These run Weaver there — which is the product claim, and the only
thing that shows a notebook user could do the same.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.fabric


# --- importing ---------------------------------------------------------------


def test_weaver_imports_inside_a_fabric_session(livy_session):
    """The claim: a Fabric session imports the installed Weaver and uses it.

    The version is whatever ``weaver install`` published into the Environment —
    not necessarily this checkout's — so we assert a real version came back, not
    that it equals the laptop's.
    """

    result = livy_session.run(
        "from importlib.metadata import version\n"
        "emit({'attr': weaver.__version__, 'dist': version('weaverstack')})\n"
    )
    assert result.returned
    assert result.payload["attr"] == result.payload["dist"]
    assert result.payload["dist"]  # a real, non-empty version string


def test_the_environment_carries_weavers_dependencies(livy_session):
    """mssql-python and the rest resolve inside the session, from the Environment."""

    result = livy_session.run(
        "import yaml, sqlparse, mssql_python\n"
        "emit(sorted(['yaml', 'sqlparse', 'mssql_python']))\n"
    )
    assert result.payload == ["mssql_python", "sqlparse", "yaml"]


def test_the_core_public_surface_is_importable_there(livy_session):
    body = (
        "from weaver import FolderTarget, DeltaTarget, Location\n"
        "emit({\n"
        "  'folder': str(FolderTarget.parse('Sales_LH/Files/Extracts')),\n"
        "  'delta': str(DeltaTarget.parse('Sales_LH')),\n"
        "  'joined': (Location('abfss://ws@host/lh') / 'Files' / 'x').value,\n"
        "})\n"
    )
    result = livy_session.run(body)
    assert result.payload == {
        "folder": "Sales_LH/Files/Extracts",
        "delta": "Sales_LH",
        "joined": "abfss://ws@host/lh/Files/x",
    }


def test_the_ses_contract_parses_there(livy_session):
    """The heart of Weaver, running in Fabric rather than described to it."""

    body = (
        "from weaver.ses import parse_document\n"
        "doc = parse_document('''\n"
        "Table ID: Sales.Order\n\n"
        "Description: One row per order.\n\n"
        "Lineage: The order export.\n\n"
        "Primary key: Order id\n\n"
        "Schema:\n"
        "  Order id: string\n"
        "''', language='python')\n"
        "emit({'id': doc.qualified, 'columns': [c.name for c in doc.effective_schema]})\n"
    )
    result = livy_session.run(body)
    assert result.payload["id"] == "Sales.Order"
    assert result.payload["columns"] == [
        "Order id",
        "Row_insert_datetime",
        "Row_update_datetime",
        "Row_delete_datetime",
    ]


def test_a_failing_statement_reports_its_error(livy_session):
    from weaver.fabric import LivyError

    with pytest.raises(LivyError, match="ZeroDivisionError|division"):
        livy_session.run("1 / 0\n")


def test_printed_output_is_not_mistaken_for_a_result(livy_session):
    result = livy_session.run("print('just logging')\n")
    assert result.returned is False
    assert "just logging" in result.text
