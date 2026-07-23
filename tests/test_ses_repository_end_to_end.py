"""The whole path over the example repository.

Filename classification -> metadata extraction -> structural checks -> SQL
analysis -> discovered references, asserted together rather than in pieces, so
a regression anywhere in the chain surfaces here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver import Location
from weaver.ses import PYTHON, SPARK_SQL, SQL, read_repository

FIXTURE = Location(str(Path(__file__).parent / "fixtures" / "sales-etl"))


@pytest.fixture(scope="module")
def repo():
    return read_repository(FIXTURE)


# --- classification ----------------------------------------------------------


def test_every_object_is_classified_from_its_filename(repo):
    assert {
        document.qualified: (document.language, document.kind) for document in repo
    } == {
        "Sales.OrderExport": (PYTHON, "Folder"),
        "Sales.Order": (PYTHON, "Table"),
        "Sales.Customer": (PYTHON, "Table"),
        "Sales.OrderSummary": (SPARK_SQL, "Table"),
        "Reporting.OrderReport": (SQL, "Table"),
        "Reporting.OrderView": (SQL, "View"),
    }


# --- metadata ----------------------------------------------------------------


def test_a_python_table_carries_its_full_contract(repo):
    document = repo["Sales.Order"].document
    assert document.primary_key == ("Order id",)
    assert document.not_null == ("Order id", "Order date")
    assert document.comparison_columns == ("Last modified",)
    assert document.lineage.reference.object_id.qualified == "Sales.OrderExport"
    assert [column.name for column in document.effective_schema][-3:] == [
        "Row_insert_datetime", "Row_update_datetime", "Row_delete_datetime",
    ]


def test_a_folder_carries_its_file_key_and_defaults(repo):
    document = repo["Sales.OrderExport"].document
    assert document.file_keys == ("*.csv",)
    assert document.is_incremental is True
    assert document.prohibit_rebuild is True


def test_a_warehouse_object_defers_column_validation(repo):
    report = repo["Reporting.OrderReport"].document
    assert report.defers_column_validation is True
    assert [column.name for column in report.audit_columns] == [
        "Row insert datetime", "Row update datetime", "Row delete datetime",
    ]


def test_a_view_prohibits_rebuild_with_its_reason(repo):
    document = repo["Reporting.OrderView"].document
    assert document.prohibit_rebuild is True
    assert "Row-level security" in document.notes


def test_a_spark_table_declares_schema_and_dependencies(repo):
    document = repo["Sales.OrderSummary"].document
    assert [column.name for column in document.schema] == [
        "Customer id", "Order count", "Total amount",
    ]
    assert [str(d) for d in document.dependencies] == ["Sales.Order"]
    assert [column.name for column in document.audit_columns][0] == "Row_insert_datetime"


# --- structural --------------------------------------------------------------


def test_python_objects_name_their_class_for_the_file(repo):
    assert repo["Sales.Order"].class_name == "Sales__Order"
    assert repo["Sales.OrderExport"].class_name == "Sales__OrderExport"


def test_support_files_are_not_objects(repo):
    assert repo.support_files == ("_helpers/dates.py",)


# --- sql analysis ------------------------------------------------------------


def test_the_spark_table_stages_through_a_temporary_view(repo):
    analysis = repo["Sales.OrderSummary"].sql_analysis
    assert analysis.statement_count == 2
    assert analysis.result_set_count == 1
    assert analysis.permanent_ddl == ()


def test_the_warehouse_table_stages_through_a_temp_table(repo):
    analysis = repo["Reporting.OrderReport"].sql_analysis
    assert analysis.statement_count == 2
    assert analysis.result_set_count == 1


def test_the_view_is_a_single_statement(repo):
    analysis = repo["Reporting.OrderView"].sql_analysis
    assert analysis.statement_count == 1
    assert analysis.result_set_count == 1


# --- discovered references ---------------------------------------------------


def test_references_across_every_language(repo):
    """Keyed by node id, because Sales.Customer exists in two targets."""
    assert {
        document.node_id: sorted(str(r) for r in document.discovered_references)
        for document in repo
    } == {
        "folder:Sales.OrderExport": [],
        "delta:Sales.Order": ["Sales.OrderExport"],
        "delta:Sales.Customer": ["Sales.OrderExport"],
        "delta:Sales.OrderSummary": ["Sales.Cancelled", "Sales.Order"],
        "sql:Sales.Customer": ["Sales_LH.Sales.Customer"],
        "sql:Reporting.OrderReport": [
            "Sales.Customer", "Sales.OrderLineCount", "Sales_LH.Sales.Order",
        ],
        "sql:Reporting.OrderView": ["Reporting.OrderReport"],
    }


def test_the_spark_temporary_view_is_not_a_reference(repo):
    assert "recent" not in str(repo["Sales.OrderSummary"].discovered_references)


def test_the_warehouse_temp_table_is_not_a_reference(repo):
    assert "#recent" not in str(repo["Reporting.OrderReport"].discovered_references)


def test_comments_in_the_fixtures_contribute_nothing(repo):
    """Both SQL fixtures mention a Legacy object in a comment."""
    for document in repo:
        assert not any(
            "Legacy" in str(reference) for reference in document.discovered_references
        )


def test_a_declaration_sits_beside_what_was_discovered(repo):
    """Declaration overrides at build; both are kept so a lint can compare."""
    summary = repo["Sales.OrderSummary"]
    assert [str(d) for d in summary.declared_dependencies] == ["Sales.Order"]
    assert "Sales.Cancelled" in [str(r) for r in summary.discovered_references]


def test_references_outside_the_repository_are_simply_recorded(repo):
    """Sales.Cancelled and Sales.OrderLineCount are not objects here.

    Whether they resolve — to a shortcut, or to nothing — needs the
    external-dependency configuration and is settled at build.
    """
    known = set(repo.by_id)
    referenced = {
        str(reference)
        for document in repo
        for reference in document.discovered_references
    }
    assert {"Sales.Cancelled", "Sales.OrderLineCount"} <= referenced - known


# --- the signature -----------------------------------------------------------


def test_the_signature_covers_every_file(repo, tmp_path):
    import shutil

    copy = tmp_path / "sales-etl"
    shutil.copytree(FIXTURE.value, copy)
    assert read_repository(Location(str(copy))).signature == repo.signature

    (copy / "_helpers" / "dates.py").write_text("# changed\n", encoding="utf-8")
    assert read_repository(Location(str(copy))).signature != repo.signature


# --- the internal graph ------------------------------------------------------
#
# Nodes are `target:Schema.Object`. An ID alone is not unique — the fixture has
# Sales.Customer as both a Delta table and a Warehouse table, which is two
# physical objects in two places and entirely legitimate.


def test_one_id_may_occupy_several_targets(repo):
    assert {document.node_id for document in repo.by_qualified["Sales.Customer"]} == {
        "delta:Sales.Customer",
        "sql:Sales.Customer",
    }


def test_asking_by_a_shared_id_says_which_are_meant(repo):
    from weaver.errors import DiscoveryError

    with pytest.raises(DiscoveryError, match="names more than one object"):
        repo["Sales.Customer"]


def test_an_unshared_id_still_resolves_on_its_own(repo):
    assert repo["Sales.Order"].node_id == "delta:Sales.Order"


def test_routing_is_inferred_from_language_and_kind(repo):
    assert {document.node_id for document in repo} == {
        "folder:Sales.OrderExport",
        "delta:Sales.Order",
        "delta:Sales.Customer",
        "delta:Sales.OrderSummary",
        "sql:Sales.Customer",
        "sql:Reporting.OrderReport",
        "sql:Reporting.OrderView",
    }


def test_the_repository_orders_upstream_before_downstream(repo):
    order = repo.graph.order()
    assert order.index("folder:Sales.OrderExport") < order.index("delta:Sales.Order")
    assert order.index("delta:Sales.Order") < order.index("sql:Reporting.OrderReport")
    assert order.index("sql:Reporting.OrderReport") < order.index("sql:Reporting.OrderView")


def test_the_layers_show_what_can_run_together(repo):
    assert repo.graph.layers() == (
        ("folder:Sales.OrderExport", "sql:Sales.Customer"),
        ("delta:Sales.Customer", "delta:Sales.Order", "sql:Reporting.OrderReport"),
        ("delta:Sales.OrderSummary", "sql:Reporting.OrderView"),
    )


def test_a_two_part_name_resolves_in_the_writers_own_namespace():
    """`join Sales.Customer` in a Warehouse query binds inside the Warehouse."""
    repo = read_repository(FIXTURE)
    assert "sql:Sales.Customer" in repo.graph.upstream_of("sql:Reporting.OrderReport")
    assert "delta:Sales.Customer" not in repo.graph.upstream_of("sql:Reporting.OrderReport")


def test_a_cross_boundary_read_is_written_in_three_parts():
    """A Warehouse reaches a Lakehouse table by naming it in full.

    That is the plain way and needs no configuration. It cannot resolve here,
    because whether `Sales_LH` is this repository's own Delta target is only
    known once the build is given its targets.
    """
    repo = read_repository(FIXTURE)
    assert "Sales_LH.Sales.Order" in repo.unresolved["sql:Reporting.OrderReport"]
    assert not any(
        node.startswith("delta:")
        for node in repo.graph.upstream_of("sql:Reporting.OrderReport")
    )


def test_an_object_sourcing_its_namesake_names_it_in_full():
    """A bare Sales.Customer inside sql:Sales.Customer would bind to itself."""
    repo = read_repository(FIXTURE)
    assert repo.graph.upstream_of("sql:Sales.Customer") == ()
    assert repo.unresolved["sql:Sales.Customer"] == ("Sales_LH.Sales.Customer",)


def test_a_name_declared_external_is_a_boundary_not_an_edge():
    """The seam the shortcut bindings will use once build supplies them."""
    from weaver.ses import build_internal_graph

    repo = read_repository(FIXTURE)
    graph = build_internal_graph(repo.documents, external_names=["Sales.Order"])
    assert graph.upstream_of("delta:Sales.OrderSummary") == ()


def test_the_boundary_stays_visible_in_node_identity(repo):
    """Node ids carry the target, so a delta -> sql edge is findable. That is
    where the SQL endpoint refresh belongs once the build resolves the
    three-part reads into edges."""
    assert all(node.split(":", 1)[0] in {"folder", "delta", "sql"} for node in repo.graph.nodes)
    crossing = [
        edge for edge in repo.graph.edges
        if edge.upstream.split(":")[0] != edge.downstream.split(":")[0]
    ]
    assert [str(edge) for edge in crossing] == [
        "folder:Sales.OrderExport -> delta:Sales.Customer",
        "folder:Sales.OrderExport -> delta:Sales.Order",
    ]


def test_descendants_are_what_a_rebuild_would_uncertify(repo):
    """Only what resolves today. The three-part reads join once the build
    knows whether Sales_LH is this repository's own Delta target."""
    assert repo.graph.descendants("folder:Sales.OrderExport") == (
        "delta:Sales.Customer",
        "delta:Sales.Order",
        "delta:Sales.OrderSummary",
    )


def test_a_declaration_replaces_discovery_in_the_graph(repo):
    """Sales.OrderSummary reads Sales.Cancelled but declares only Sales.Order."""
    assert repo.graph.upstream_of("delta:Sales.OrderSummary") == ("delta:Sales.Order",)


def test_references_outside_the_repository_are_not_edges(repo):
    assert "sql:Sales.OrderLineCount" not in repo.graph.nodes
    assert "Sales.OrderLineCount" in repo.unresolved["sql:Reporting.OrderReport"]


def test_what_remains_unresolved_is_pending_or_genuinely_outside(repo):
    """Three-part reads wait for the targets; Sales.OrderLineCount is a
    table-valued function nobody here defines and never will."""
    assert repo.unresolved == {
        "sql:Reporting.OrderReport": ("Sales.OrderLineCount", "Sales_LH.Sales.Order"),
        "sql:Sales.Customer": ("Sales_LH.Sales.Customer",),
    }


def test_a_cycle_in_a_repository_is_refused(tmp_path):
    import shutil

    copy = tmp_path / "cyclic"
    shutil.copytree(FIXTURE.value, copy)
    export = copy / "Sales__OrderExport.py"
    export.write_text(
        export.read_text(encoding="utf-8").replace(
            "from weaver import Folder",
            "from Sales__Order import Sales__Order\n\nfrom weaver import Folder",
        ),
        encoding="utf-8",
    )
    from weaver.errors import GraphError

    with pytest.raises(GraphError, match="dependency cycle"):
        read_repository(Location(str(copy)))


def test_two_objects_may_not_claim_the_same_physical_place(tmp_path):
    """A Python table and a Spark SQL table with one ID both claim Tables/…."""
    import shutil

    from weaver.errors import DiscoveryError

    copy = tmp_path / "clash"
    shutil.copytree(FIXTURE.value, copy)
    (copy / "Sales.Order.spark.sql").write_text(
        "/*\nTable ID: Sales.Order\n\nDescription: x\n\nLineage: y\n\n"
        "Dependencies:\n  - Sales.OrderExport\n\nSchema:\n  a: string\n*/\nselect 1 as a\n",
        encoding="utf-8",
    )
    with pytest.raises(DiscoveryError, match="declared twice for the delta target"):
        read_repository(Location(str(copy)))
