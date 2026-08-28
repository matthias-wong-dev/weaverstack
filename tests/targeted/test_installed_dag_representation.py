"""What the catalogue says the installed managed graph is.

Pure Python throughout: a `Catalogue` is rows, so these hand-write the rows a
real estate would hold and never open a repository, a session or a target. The
subject is one derivation, catalogue rows into nodes, edges and topology, which
load planning, validation planning and health all read.

Hand-written rows rather than a parsed repository, because a repository that
parses cannot produce a cycle, a duplicated identity or a dangling read, and a
fixture that could not express them would leave the refusals untested.
"""

from __future__ import annotations

import pytest
from factories import (
    dependency_row,
    document_id,
    installation_row,
    registry_row,
    shortcut_row,
    validation_row,
)
from support.weaver_test import weaver_test

from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    SHORTCUT,
    TEST_DICTIONARY,
)
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.errors import CatalogueStateError, GraphError
from weaver.etl import validation_artefact_id
from weaver.installed import PYTHON_TABLE, WAREHOUSE_PROCEDURE
from weaver.targets import PhysicalTargetRef

RAW = "Lakehouse/Raw"
CURATED = "Lakehouse/Curated"
REPORTING = "Warehouse/Reporting"

RAW_LH = PhysicalTargetRef("lakehouse", "Raw_LH")
CURATED_LH = PhysicalTargetRef("lakehouse", "Curated_LH")
REPORTING_WH = PhysicalTargetRef("warehouse", "Reporting_WH")

TARGET_FOR = {RAW: "Raw_LH", CURATED: "Curated_LH", REPORTING: "Reporting_WH"}


# --- building an estate out of rows -------------------------------------------


def _load_artefact(identity: WeaverDocumentId) -> dict:
    """The Registry row certifying one object's installed load primitive."""

    if identity.item.item_type == "Warehouse":
        return registry_row(
            WeaverDocumentId.parse(
                f"{identity.item}/procedure:_/Load {identity.object_id.qualified}"
            ),
            object_type="stored_procedure",
            object_role="load",
        )
    schema, name = identity.object_id.schema, identity.object_id.object
    return registry_row(
        WeaverDocumentId.parse(f"{identity.item}/file:_/Load/{schema}__{name}.py"),
        object_type="file",
        object_role="load",
    )


def _validation_artefact(logical: WeaverDocumentId, kind: str) -> dict:
    artefact = validation_artefact_id(logical.item, kind, logical.object_id)
    return registry_row(
        artefact,
        object_type="file" if artefact.shape == "file" else "stored_procedure",
        object_role=kind.casefold(),
    )


class _Estate:
    """Rows for one estate, gathered per item and handed to a `Catalogue`."""

    def __init__(self) -> None:
        self._rows: dict[WeaverItemId, dict[str, list[dict]]] = {}

    def _tables(self, item: WeaverItemId) -> dict[str, list[dict]]:
        tables = self._rows.setdefault(
            item,
            {
                INSTALLATION.name: [installation_row(item, TARGET_FOR[str(item)])],
                REGISTRY.name: [],
                DEPENDENCY.name: [],
                SHORTCUT.name: [],
                TEST_DICTIONARY.name: [],
            },
        )
        return tables

    def object(
        self,
        identity: str,
        *,
        object_type: str = "table",
        object_role: str = "data",
        loadable: bool = True,
    ) -> "_Estate":
        parsed = document_id(identity)
        tables = self._tables(parsed.item)
        tables[REGISTRY.name].append(
            registry_row(parsed, object_type=object_type, object_role=object_role)
        )
        if loadable and object_type in ("table", "folder") and object_role == "data":
            tables[REGISTRY.name].append(_load_artefact(parsed))
        return self

    def view(self, identity: str) -> "_Estate":
        return self.object(identity, object_type="view", loadable=False)

    def reads(self, consumer: str, reference: str, **referenced) -> "_Estate":
        parsed = document_id(consumer)
        self._tables(parsed.item)[DEPENDENCY.name].append(
            dependency_row(parsed, reference, **referenced)
        )
        return self

    def shortcut(self, destination: str, source: str, **how) -> "_Estate":
        parsed = document_id(destination)
        self._tables(parsed.item)[SHORTCUT.name].append(
            shortcut_row(parsed, source, **how)
        )
        self.object(
            destination,
            object_type="view" if parsed.item.item_type == "Warehouse" else "table",
            object_role="shortcut",
            loadable=False,
        )
        return self

    def validation(self, logical: str, *, kind: str = "Test", **declared) -> "_Estate":
        parsed = document_id(logical)
        tables = self._tables(parsed.item)
        tables[TEST_DICTIONARY.name].append(
            validation_row(parsed, test_type=kind.casefold(), **declared)
        )
        tables[REGISTRY.name].append(_validation_artefact(parsed, kind))
        return self

    def declared_only(self, logical: str, *, kind: str = "Test") -> "_Estate":
        """A validation whose runnable artefact was never installed."""

        parsed = document_id(logical)
        self._tables(parsed.item)[TEST_DICTIONARY.name].append(
            validation_row(parsed, test_type=kind.casefold())
        )
        return self

    def catalogue(self) -> Catalogue:
        return Catalogue(
            rows={
                item: {name: tuple(rows) for name, rows in tables.items()}
                for item, tables in self._rows.items()
            }
        )

    def dag(self):
        return self.catalogue().dag()


def _chain() -> _Estate:
    """``A -> B -> C`` within one Lakehouse."""

    return (
        _Estate()
        .object(f"{RAW}/Sales.A")
        .object(f"{RAW}/Sales.B")
        .object(f"{RAW}/Sales.C")
        .reads(f"{RAW}/Sales.B", "Sales.A")
        .reads(f"{RAW}/Sales.C", "Sales.B")
    )


def ids(nodes) -> tuple[str, ...]:
    return tuple(node.node_id for node in nodes)


# --- topology -----------------------------------------------------------------


@weaver_test()
def test_a_chain_orders_upstream_before_downstream():
    dag = _chain().dag()

    assert ids(dag.order()) == (
        f"{RAW}/Sales.A",
        f"{RAW}/Sales.B",
        f"{RAW}/Sales.C",
    )


@weaver_test()
def test_transitive_ancestry_reaches_through_the_middle():
    dag = _chain().dag()

    assert ids(dag.ancestors(f"{RAW}/Sales.C")) == (
        f"{RAW}/Sales.A",
        f"{RAW}/Sales.B",
    )
    assert ids(dag.parents(f"{RAW}/Sales.C")) == (f"{RAW}/Sales.B",)
    assert ids(dag.descendants(f"{RAW}/Sales.A")) == (
        f"{RAW}/Sales.B",
        f"{RAW}/Sales.C",
    )


@weaver_test()
def test_one_producer_branches_to_two_consumers():
    dag = (
        _Estate()
        .object(f"{RAW}/Sales.A")
        .object(f"{RAW}/Sales.B")
        .object(f"{RAW}/Sales.C")
        .reads(f"{RAW}/Sales.B", "Sales.A")
        .reads(f"{RAW}/Sales.C", "Sales.A")
        .dag()
    )

    assert ids(dag.children(f"{RAW}/Sales.A")) == (
        f"{RAW}/Sales.B",
        f"{RAW}/Sales.C",
    )


@weaver_test()
def test_two_producers_converge_on_one_consumer():
    dag = (
        _Estate()
        .object(f"{RAW}/Sales.A")
        .object(f"{RAW}/Sales.B")
        .object(f"{RAW}/Sales.C")
        .reads(f"{RAW}/Sales.C", "Sales.A")
        .reads(f"{RAW}/Sales.C", "Sales.B")
        .dag()
    )

    assert ids(dag.parents(f"{RAW}/Sales.C")) == (
        f"{RAW}/Sales.A",
        f"{RAW}/Sales.B",
    )


@weaver_test()
def test_a_view_is_a_conduit_that_owns_no_load():
    """A View is an ordinary node. Nothing installs a load primitive for it."""

    dag = (
        _Estate()
        .object(f"{RAW}/Sales.A")
        .view(f"{RAW}/Sales.Live")
        .object(f"{RAW}/Sales.C")
        .reads(f"{RAW}/Sales.Live", "Sales.A")
        .reads(f"{RAW}/Sales.C", "Sales.Live")
        .dag()
    )

    assert not dag.node(f"{RAW}/Sales.Live").is_loadable
    assert ids(dag.ancestors(f"{RAW}/Sales.C")) == (
        f"{RAW}/Sales.A",
        f"{RAW}/Sales.Live",
    )


@weaver_test()
def test_node_and_edge_order_is_the_estate_rather_than_iteration():
    """Rows given back to front still produce one order."""

    forwards = _chain().dag()
    backwards = (
        _Estate()
        .object(f"{RAW}/Sales.C")
        .object(f"{RAW}/Sales.B")
        .object(f"{RAW}/Sales.A")
        .reads(f"{RAW}/Sales.C", "Sales.B")
        .reads(f"{RAW}/Sales.B", "Sales.A")
        .dag()
    )

    assert ids(forwards.nodes) == ids(backwards.nodes)
    assert ids(forwards.order()) == ids(backwards.order())
    assert forwards.graph.edges == backwards.graph.edges


# --- crossing items -----------------------------------------------------------


def _crossing() -> _Estate:
    """A Lakehouse table read by a Warehouse consumer through a shortcut."""

    return (
        _Estate()
        .object(f"{RAW}/Sales.Order")
        .shortcut(f"{REPORTING}/Sales.Order", f"{RAW}/Sales.Order")
        .object(f"{REPORTING}/Sales.Summary")
        .reads(f"{REPORTING}/Sales.Summary", "Sales.Order")
    )


@weaver_test()
def test_a_logical_shortcut_connects_the_managed_source_to_the_consumer():
    dag = _crossing().dag()

    assert ids(dag.parents(f"{REPORTING}/Sales.Summary")) == (f"{RAW}/Sales.Order",)
    edge = dag.reads(f"{REPORTING}/Sales.Summary")[0]
    assert str(edge.through) == f"{REPORTING}/Sales.Order"


@weaver_test()
def test_a_shortcut_destination_is_ordered_behind_its_source():
    dag = _crossing().dag()

    assert ids(dag.parents(f"{REPORTING}/Sales.Order")) == (f"{RAW}/Sales.Order",)
    assert dag.reads(f"{REPORTING}/Sales.Order") == ()


@weaver_test()
def test_a_physical_shortcut_is_a_boundary_rather_than_a_producer():
    """A physical shortcut names an item Weaver does not manage.

    The destination is still an installed object the consumer reads. What stops
    at it is the managed graph: nothing behind it is Weaver's to order.
    """

    dag = (
        _Estate()
        .object(f"{RAW}/Sales.Order")
        .shortcut(
            f"{CURATED}/Sales.External",
            f"{RAW}/Sales.Order",
            target_type="physical",
        )
        .object(f"{CURATED}/Sales.Report")
        .reads(f"{CURATED}/Sales.Report", "Sales.External")
        .dag()
    )

    assert ids(dag.parents(f"{CURATED}/Sales.Report")) == (f"{CURATED}/Sales.External",)
    assert dag.parents(f"{CURATED}/Sales.External") == ()
    assert dag.ancestors(f"{CURATED}/Sales.Report") == (
        dag.node(f"{CURATED}/Sales.External"),
    )


@weaver_test()
def test_a_three_part_read_is_recorded_rather_than_ordered():
    """It names a physical object outside the managed estate."""

    consumer = document_id(f"{RAW}/Sales.Order")
    dag = (
        _Estate()
        .object(f"{RAW}/Sales.Order")
        .reads(f"{RAW}/Sales.Order", "other_ws.other_lh.Sales.Source")
        .dag()
    )

    assert dag.parents(consumer) == ()
    assert dag.external_references[consumer] == ("other_ws.other_lh.Sales.Source",)


# --- validations --------------------------------------------------------------


@weaver_test()
def test_a_test_is_a_terminal_node_that_reads_what_it_validates():
    dag = (
        _Estate()
        .object(f"{REPORTING}/Sales.Summary")
        .validation(f"{REPORTING}/Sales.Integrity")
        .reads(f"{REPORTING}/Sales.Integrity", "Sales.Summary")
        .dag()
    )

    node = dag.node(f"{REPORTING}/Sales.Integrity")
    assert node.is_validation
    assert not node.is_loadable
    assert ids(dag.parents(node.identity)) == (f"{REPORTING}/Sales.Summary",)
    assert dag.children(node.identity) == ()


@weaver_test()
def test_an_assumption_is_a_terminal_node_of_its_own_kind():
    dag = (
        _Estate()
        .object(f"{REPORTING}/Sales.Summary")
        .validation(f"{REPORTING}/Sales.Coverage", kind="Assumption")
        .reads(f"{REPORTING}/Sales.Coverage", "Sales.Summary")
        .dag()
    )

    node = dag.node(f"{REPORTING}/Sales.Coverage")
    assert node.role == "assumption"
    assert node.artefact_kind == "Assumption"
    assert node.is_validation


@weaver_test()
def test_a_validation_never_reads_another_validation():
    """Weaver has no test-on-test ordering: a Test declares data, not a Test."""

    estate = (
        _Estate()
        .object(f"{REPORTING}/Sales.Summary")
        .validation(f"{REPORTING}/Sales.Integrity")
        .validation(f"{REPORTING}/Sales.Coverage", kind="Assumption")
        .reads(f"{REPORTING}/Sales.Integrity", "Sales.Summary")
        .reads(f"{REPORTING}/Sales.Coverage", "Sales.Summary")
    )
    dag = estate.dag()

    for validation in dag.validations():
        assert dag.children(validation.identity) == ()


@weaver_test()
def test_a_declared_validation_with_no_artefact_is_a_node_that_is_not_installed():
    dag = (
        _Estate()
        .object(f"{REPORTING}/Sales.Summary")
        .declared_only(f"{REPORTING}/Sales.Integrity")
        .dag()
    )

    node = dag.node(f"{REPORTING}/Sales.Integrity")
    assert node.expects_artefact
    assert not node.is_installed


@weaver_test()
def test_a_validation_carries_its_declared_key_and_description():
    dag = (
        _Estate()
        .object(f"{REPORTING}/Sales.Summary")
        .validation(
            f"{REPORTING}/Sales.Integrity",
            primary_key="OrderId, LineId",
            description="Every line reconciles.",
        )
        .dag()
    )

    node = dag.node(f"{REPORTING}/Sales.Integrity")
    assert node.primary_key == ("OrderId", "LineId")
    assert node.description == "Every line reconciles."


# --- artefacts ----------------------------------------------------------------


@weaver_test()
def test_a_loadable_names_the_primitive_its_dispatch_runs():
    dag = (
        _Estate()
        .object(f"{RAW}/Sales.Order")
        .object(f"{REPORTING}/Sales.Summary")
        .dag()
    )

    assert dag.node(f"{RAW}/Sales.Order").artefact_kind == PYTHON_TABLE
    assert dag.node(f"{REPORTING}/Sales.Summary").artefact_kind == WAREHOUSE_PROCEDURE


@weaver_test()
def test_an_object_whose_load_primitive_is_absent_is_not_loadable():
    dag = _Estate().object(f"{RAW}/Sales.Order", loadable=False).dag()

    node = dag.node(f"{RAW}/Sales.Order")
    assert node.expects_artefact
    assert not node.is_installed
    assert not node.is_loadable


@weaver_test()
def test_a_runtime_artefact_is_not_a_node_of_the_graph():
    """A Registry row for a deployed module describes what runs, not what is read."""

    dag = _Estate().object(f"{RAW}/Sales.Order").dag()

    assert ids(dag.nodes) == (f"{RAW}/Sales.Order",)
    assert str(dag.node(f"{RAW}/Sales.Order").artefact) == (
        f"{RAW}/file:_/Load/Sales__Order.py"
    )


# --- selection ----------------------------------------------------------------


def _mixed() -> _Estate:
    """Two Lakehouses and a Warehouse, with a view and two validations."""

    return (
        _Estate()
        .object(f"{RAW}/Sales.Order")
        .object(f"{CURATED}/Sales.Customer")
        .object(f"{REPORTING}/Sales.Summary")
        .view(f"{REPORTING}/Sales.Live")
        .validation(f"{REPORTING}/Sales.Integrity")
        .validation(f"{CURATED}/Sales.Coverage", kind="Assumption")
    )


@weaver_test()
def test_selection_by_target_keeps_identity_order():
    dag = _mixed().dag()

    assert ids(dag.select(targets=(RAW_LH, REPORTING_WH))) == (
        f"{RAW}/Sales.Order",
        f"{REPORTING}/Sales.Integrity",
        f"{REPORTING}/Sales.Live",
        f"{REPORTING}/Sales.Summary",
    )


@weaver_test()
def test_selection_by_item():
    dag = _mixed().dag()

    assert ids(dag.nodes_for_item(WeaverItemId.parse(CURATED))) == (
        f"{CURATED}/Sales.Coverage",
        f"{CURATED}/Sales.Customer",
    )


@weaver_test()
def test_selection_by_object_type():
    dag = _mixed().dag()

    assert ids(dag.select(object_types=("view",))) == (f"{REPORTING}/Sales.Live",)


@weaver_test()
def test_selection_of_loadables_and_of_validations():
    dag = _mixed().dag()

    assert ids(dag.loadables()) == (
        f"{CURATED}/Sales.Customer",
        f"{RAW}/Sales.Order",
        f"{REPORTING}/Sales.Summary",
    )
    assert ids(dag.validations()) == (
        f"{CURATED}/Sales.Coverage",
        f"{REPORTING}/Sales.Integrity",
    )


@weaver_test()
def test_selection_by_load_name_folds_case():
    dag = _mixed().dag()

    assert ids(dag.select(load_names=("sales.summary",))) == (
        f"{REPORTING}/Sales.Summary",
    )


@weaver_test()
def test_filters_combine():
    dag = _mixed().dag()

    assert ids(dag.validations(targets=(CURATED_LH,))) == (f"{CURATED}/Sales.Coverage",)
    assert dag.loadables(targets=(CURATED_LH,), object_types=("folder",)) == ()


@weaver_test()
def test_a_filtered_subgraph_keeps_the_edges_between_what_it_kept():
    dag = _chain().dag()

    subgraph = dag.subgraph([f"{RAW}/Sales.C"], with_ancestors=True)

    assert subgraph.order() == (
        f"{RAW}/Sales.A",
        f"{RAW}/Sales.B",
        f"{RAW}/Sales.C",
    )


# --- what the catalogue may not say -------------------------------------------


@weaver_test()
def test_a_managed_cycle_is_refused_when_the_graph_is_built():
    estate = (
        _Estate()
        .object(f"{RAW}/Sales.A")
        .object(f"{RAW}/Sales.B")
        .reads(f"{RAW}/Sales.A", "Sales.B")
        .reads(f"{RAW}/Sales.B", "Sales.A")
    )

    with pytest.raises(GraphError, match="dependency cycle"):
        estate.dag()


@weaver_test()
def test_a_self_dependency_is_recorded_rather_than_ordered():
    consumer = document_id(f"{RAW}/Sales.A")
    dag = _Estate().object(f"{RAW}/Sales.A").reads(f"{RAW}/Sales.A", "Sales.A").dag()

    assert dag.parents(consumer) == ()
    assert "resolves to itself" in dag.unresolved_for(consumer)[0]


@weaver_test()
def test_a_read_that_names_nothing_installed_is_recorded_rather_than_raised():
    """An unrelated item's dangling read must not stop the whole graph."""

    consumer = document_id(f"{RAW}/Sales.A")
    dag = (
        _Estate()
        .object(f"{RAW}/Sales.A")
        .object(f"{CURATED}/Sales.Customer")
        .reads(f"{RAW}/Sales.A", "Sales.Nowhere")
        .dag()
    )

    assert ids(dag.nodes) == (f"{CURATED}/Sales.Customer", f"{RAW}/Sales.A")
    assert "resolves to neither an installed object" in dag.unresolved_for(consumer)[0]


@weaver_test()
def test_one_identity_may_not_be_both_an_object_and_a_validation():
    estate = (
        _Estate()
        .object(f"{REPORTING}/Sales.Summary")
        .validation(f"{REPORTING}/Sales.Summary")
    )

    with pytest.raises(
        CatalogueStateError, match="two installed nodes at one identity"
    ):
        estate.dag()


@weaver_test()
def test_a_registered_object_whose_item_has_no_installation_is_refused():
    catalogue = Catalogue(
        rows={
            WeaverItemId.parse(RAW): {
                REGISTRY.name: (registry_row(document_id(f"{RAW}/Sales.Order")),)
            }
        }
    )

    with pytest.raises(CatalogueStateError, match="has no installation row"):
        catalogue.dag()


@weaver_test()
def test_an_unknown_test_type_is_refused_rather_than_guessed():
    catalogue = Catalogue(
        rows={
            WeaverItemId.parse(RAW): {
                INSTALLATION.name: (installation_row(RAW, "Raw_LH"),),
                TEST_DICTIONARY.name: (
                    validation_row(f"{RAW}/Sales.Odd", test_type="probe"),
                ),
            }
        }
    )

    with pytest.raises(CatalogueStateError, match="unsupported test_type"):
        catalogue.dag()


@weaver_test()
def test_two_objects_at_one_physical_address_are_recorded_rather_than_refused():
    """An estate keeps Registry rows from every item ever bound to a target."""

    catalogue = Catalogue(
        rows={
            WeaverItemId.parse(RAW): {
                INSTALLATION.name: (installation_row(RAW, "Shared_LH"),),
                REGISTRY.name: (registry_row(document_id(f"{RAW}/Sales.Order")),),
            },
            WeaverItemId.parse(CURATED): {
                INSTALLATION.name: (installation_row(CURATED, "Shared_LH"),),
                REGISTRY.name: (registry_row(document_id(f"{CURATED}/Sales.Order")),),
            },
        }
    )

    dag = catalogue.dag()

    shared = PhysicalTargetRef("lakehouse", "Shared_LH")
    assert "both resolve to Sales.Order" in dag.ambiguous[shared][0]
    assert len(dag.nodes) == 2


@weaver_test()
def test_a_node_the_graph_does_not_hold_is_named_in_the_refusal():
    dag = _chain().dag()

    with pytest.raises(CatalogueStateError, match="is not a node of the installed"):
        dag.node(f"{RAW}/Sales.Nowhere")
