"""Weaver running *inside* Fabric.

The other Fabric tests run Weaver on this machine and reach into a workspace
over HTTP. These run Weaver there — which is the product claim, and the only
thing that shows a notebook user could do the same.
"""

from __future__ import annotations

import pytest

import weaver
pytestmark = pytest.mark.fabric


# --- shipping ----------------------------------------------------------------


def test_the_runtime_syncs_into_the_workspace(synced_runtime):
    assert synced_runtime.total > 0
    shipped = set(synced_runtime.uploaded) | set(synced_runtime.unchanged)
    assert "__init__.py" in shipped
    assert "fabric/onelake.py" in shipped


def test_a_second_sync_uploads_nothing(fabric_host, synced_runtime):
    """Content hashes, so an unchanged package is not re-shipped."""
    from weaver.fabric import sync_runtime

    again = sync_runtime(fabric_host)
    assert again.uploaded == ()
    assert again.total == synced_runtime.total


# --- importing ---------------------------------------------------------------


def test_weaver_imports_inside_a_fabric_session(livy_session):
    """The claim: a Fabric session can import Weaver and use it."""

    result = livy_session.run("emit(weaver.__version__)\n")
    assert result.returned
    assert result.payload == weaver.__version__


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
