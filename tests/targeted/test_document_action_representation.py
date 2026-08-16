"""One authored document rendered into one build action.

The smallest step the build takes, and the one that used to be provable only by
generating a whole bundle. `source.create_ddl()` says *what statement*; this says
what action carries it — which executor runs it, under what id, against which
frozen bytes, and with what hash. Those are separate claims and they fail for
separate reasons, so they are asserted separately.

Nothing here needs a catalogue, an inventory, a bundle, an installer or Fabric.
"""

from __future__ import annotations

import hashlib

import pytest
from factories import (
    bound_target,
    document_id,
    folder_document,
    lakehouse_table,
    single_document_repository,
    spark_view,
    warehouse_table,
    warehouse_view,
)
from support.weaver_test import weaver_test

from weaver.build_bundle import render_document_build_action


def _render(repository, identity: str):
    document = document_id(identity)
    return render_document_build_action(
        document, repository.source_documents[document], target=bound_target()
    )


# --- a Lakehouse table --------------------------------------------------------


@pytest.fixture
def lakehouse_customer(tmp_path):
    return single_document_repository(
        tmp_path,
        documents={
            "DWG__Customer.py": lakehouse_table(
                "DWG.Customer",
                columns={"CustomerId": "string", "CustomerName": "string"},
            )
        },
    )


@weaver_test()
def test_a_lakehouse_table_renders_a_spark_sql_build_action(lakehouse_customer):
    rendered = _render(lakehouse_customer, "DWG.Customer")

    assert rendered.action.kind == "build_table"
    assert rendered.action.executor == "spark_sql"
    assert rendered.action.resource_node_id == "Lakehouse/Sales/DWG.Customer"


@weaver_test()
def test_the_action_id_is_derived_from_the_document_identity(lakehouse_customer):
    """Ids must be stable and unique per document: a bundle is keyed by them."""

    rendered = _render(lakehouse_customer, "DWG.Customer")

    assert rendered.action.id == "object-Lakehouse--Sales--DWG.Customer"


@weaver_test()
def test_the_payload_is_the_documents_own_ddl(lakehouse_customer):
    """The action carries what `create_ddl()` produced — not a re-rendering of it."""

    document = lakehouse_customer.source_documents[document_id("DWG.Customer")]
    rendered = _render(lakehouse_customer, "DWG.Customer")

    filename = rendered.action.payload
    assert rendered.payloads[filename] == (
        document.create_ddl(destination=bound_target().spark_target).content.encode(
            "utf-8"
        )
    )


@weaver_test()
def test_the_payload_filename_carries_the_ddls_extension(lakehouse_customer):
    """The extension comes from the DDL, so the payload says what kind it is."""

    rendered = _render(lakehouse_customer, "DWG.Customer")

    assert rendered.action.payload == "Lakehouse--Sales--DWG.Customer.spark.sql"


@weaver_test()
def test_the_hash_is_of_the_payload_the_action_carries(lakehouse_customer):
    """The installer verifies this before executing, so it must match exactly."""

    rendered = _render(lakehouse_customer, "DWG.Customer")
    content = rendered.payloads[rendered.action.payload]

    assert rendered.action.payload_sha256 == hashlib.sha256(content).hexdigest()


@weaver_test()
def test_rendering_the_same_document_twice_gives_identical_bytes(lakehouse_customer):
    """A bundle's identity is its bytes, so rendering must be deterministic.

    Anything time-dependent or set-ordered leaking into a payload would make the
    same repository produce a different bundle id on every run, and incremental
    selection would see a change that never happened.
    """

    first = _render(lakehouse_customer, "DWG.Customer")
    second = _render(lakehouse_customer, "DWG.Customer")

    assert first.action == second.action
    assert first.payloads == second.payloads


# --- the other declaration kinds ---------------------------------------------


@weaver_test()
def test_a_warehouse_table_renders_a_tsql_action(tmp_path):
    repository = single_document_repository(
        tmp_path,
        item="Warehouse/Reporting",
        documents={"DWG.Customer.sql": warehouse_table("DWG.Customer")},
    )

    rendered = render_document_build_action(
        document_id("Warehouse/Reporting/DWG.Customer"),
        repository.source_documents[document_id("Warehouse/Reporting/DWG.Customer")],
        target=bound_target(kind="warehouse", item_id="Reporting_WH"),
    )

    assert rendered.action.kind == "build_table"
    assert rendered.action.executor == "tsql"
    assert rendered.action.payload.endswith(".sql")


@weaver_test()
def test_a_spark_view_renders_a_build_view_action(tmp_path):
    repository = single_document_repository(
        tmp_path,
        documents={
            "DWG__Customer.py": lakehouse_table("DWG.Customer"),
            "DWG.ActiveCustomer.sql": spark_view(
                "DWG.ActiveCustomer", depends_on="DWG.Customer"
            ),
        },
    )

    rendered = _render(repository, "DWG.ActiveCustomer")

    assert rendered.action.kind == "build_view"
    assert rendered.action.executor == "spark_sql"


@weaver_test()
def test_a_warehouse_view_renders_a_tsql_build_view_action(tmp_path):
    repository = single_document_repository(
        tmp_path,
        item="Warehouse/Reporting",
        documents={
            "DWG.Customer.sql": warehouse_table("DWG.Customer"),
            "DWG.ActiveCustomer.sql": warehouse_view(
                "DWG.ActiveCustomer",
                select="select CustomerId from [DWG].[Customer]",
                depends_on="DWG.Customer",
            ),
        },
    )
    identity = document_id("Warehouse/Reporting/DWG.ActiveCustomer")

    rendered = render_document_build_action(
        identity, repository.source_documents[identity], target=bound_target()
    )

    assert rendered.action.kind == "build_view"
    assert rendered.action.executor == "tsql"


@weaver_test()
def test_a_folder_renders_an_action_with_no_payload_at_all(tmp_path):
    """A folder is created, not executed — there is nothing to freeze or hash.

    Worth its own assertion because a payload-less action is the one shape the
    installer's payload loading and hash verification must both skip, and an
    empty-string filename would satisfy neither.
    """

    repository = single_document_repository(
        tmp_path,
        schemas=("DWG", "Raw"),
        documents={"Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv")},
    )
    identity = document_id("Lakehouse/Sales/Files/Raw.CustomerCsv")

    rendered = render_document_build_action(
        identity, repository.source_documents[identity], target=bound_target()
    )

    assert rendered.action.kind == "build_folder"
    assert rendered.action.executor == "folder"
    assert rendered.action.payload is None
    assert rendered.action.payload_sha256 is None
    assert rendered.payloads == {}
