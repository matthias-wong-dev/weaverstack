"""What the installed catalogue says the physical load graph is.

Pure Python throughout: no Spark session, no SQL connection, no real target. The
subject is the arithmetic that turns *installed state* into *what runs and in
what order*, and that arithmetic reads nothing else.

Two kinds of fixture appear here, deliberately. Claims about a well-formed estate
use `installed_catalogue`, which is composed from the production projection — so
what is planned against is what a build actually publishes. Claims about a
*malformed* estate hand-write rows, because a repository that parses cannot
produce a cycle or an ambiguous binding, and a fixture that could not express
them would leave the refusals untested.
"""

from __future__ import annotations

import pytest
from factories import (
    LOAD_CONSUMER,
    LOAD_CONSUMER_TARGET,
    LOAD_PRODUCER,
    LOAD_PRODUCER_TARGET,
    alias_declaration,
    folder_document,
    installed_catalogue,
    item_bindings,
    lakehouse_table,
    load_estate,
    load_estate_bindings,
    schema_document,
    warehouse_table,
)

from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.errors import LoadError
from weaver.load_plan import (
    ENDPOINT_REFRESH,
    PYTHON_FOLDER,
    PYTHON_TABLE,
    WAREHOUSE_PROCEDURE,
    InstalledEstate,
    LoadDag,
    PhysicalTargetRef,
    load_dag,
)

RAW = PhysicalTargetRef("lakehouse", LOAD_PRODUCER_TARGET)
REPORTING = PhysicalTargetRef("warehouse", LOAD_CONSUMER_TARGET)


@pytest.fixture
def estate(tmp_path):
    repository = load_estate(tmp_path)
    return InstalledEstate.from_catalogue(
        installed_catalogue(repository, load_estate_bindings())
    )


def node_ids(dag) -> tuple[str, ...]:
    return tuple(node.node_id for node in dag.order())


# --- reversing the build's own binding ---------------------------------------


def test_load_dag_maps_physical_targets_back_to_logical_items(estate):
    assert estate.installations == {
        WeaverItemId.parse(LOAD_PRODUCER): RAW,
        WeaverItemId.parse(LOAD_CONSUMER): REPORTING,
    }
    assert (
        estate.objects[WeaverDocumentId.parse(f"{LOAD_PRODUCER}/Sales.Order")].target
        == RAW
    )


def test_load_dag_finds_the_installed_primitive_for_each_dispatch_kind(estate):
    dag = load_dag(estate, targets=(RAW, REPORTING))

    assert {node.node_id: node.primitive_kind for node in dag.nodes} == {
        "load:Lakehouse/Raw_LH/Sales.Export": PYTHON_FOLDER,
        "load:Lakehouse/Raw_LH/Sales.Order": PYTHON_TABLE,
        # A Spark-SQL-authored table dispatches as what it installs as.
        "load:Lakehouse/Raw_LH/Sales.Daily": PYTHON_TABLE,
        "refresh:Lakehouse/Raw_LH": ENDPOINT_REFRESH,
        "load:Warehouse/Reporting_WH/Sales.Summary": WAREHOUSE_PROCEDURE,
    }


# --- what one request selects -------------------------------------------------


def test_load_dag_loads_every_object_in_the_requested_targets(estate):
    dag = load_dag(estate, targets=(RAW,))

    assert node_ids(dag) == (
        "load:Lakehouse/Raw_LH/Sales.Export",
        "load:Lakehouse/Raw_LH/Sales.Order",
        "load:Lakehouse/Raw_LH/Sales.Daily",
    )


def test_load_dag_excludes_objects_that_own_no_load_primitive(estate):
    """A view and the generated runtime folder are installed and not loadable."""

    dag = load_dag(estate, targets=(RAW, REPORTING))
    logical = {str(node.logical_id) for node in dag.nodes if node.logical_id}

    assert f"{LOAD_CONSUMER}/Sales.Live" not in logical
    assert f"{LOAD_PRODUCER}/Files/_.Load" not in logical


def test_load_dag_keeps_a_single_target_as_a_hard_boundary(estate):
    dag = load_dag(estate, targets=(REPORTING,))

    assert node_ids(dag) == ("load:Warehouse/Reporting_WH/Sales.Summary",)
    assert dag.edges == ()


def test_load_dag_excludes_unrelated_downstream_objects(estate):
    dag = load_dag(estate, targets=(RAW,))

    assert "load:Warehouse/Reporting_WH/Sales.Summary" not in dag.by_id


def test_load_dag_crosses_targets_only_when_both_are_requested(estate):
    dag = load_dag(estate, targets=(RAW, REPORTING))

    assert "load:Lakehouse/Raw_LH/Sales.Order" in dag.by_id
    assert "refresh:Lakehouse/Raw_LH" in dag.by_id
    assert "load:Warehouse/Reporting_WH/Sales.Summary" in dag.by_id


def test_names_select_exact_nodes_without_dependencies_or_edges(estate):
    dag = load_dag(
        estate,
        targets=(RAW,),
        names=("Sales.Order", "Sales.Daily"),
    )

    assert set(node_ids(dag)) == {
        "load:Lakehouse/Raw_LH/Sales.Order",
        "load:Lakehouse/Raw_LH/Sales.Daily",
    }
    assert dag.edges == ()


def test_a_name_is_resolved_case_insensitively_within_the_requested_targets(estate):
    dag = load_dag(estate, targets=(RAW,), names=("sales.order",))

    assert node_ids(dag) == ("load:Lakehouse/Raw_LH/Sales.Order",)


def test_an_unknown_load_name_lists_the_installed_loadables(estate):
    with pytest.raises(LoadError, match="no loadable object named 'Sales.Missing'"):
        load_dag(estate, targets=(RAW,), names=("Sales.Missing",))


# --- ordering -----------------------------------------------------------------


def test_load_dag_orders_direct_dependencies(estate):
    dag = load_dag(estate, targets=(RAW,))

    assert (
        "load:Lakehouse/Raw_LH/Sales.Order",
        "load:Lakehouse/Raw_LH/Sales.Daily",
    ) in dag.edges


def test_load_dag_resolves_a_python_import_as_a_dependency(tmp_path):
    """A Python object declares its dependencies by importing them.

    And the catalogue records a dependency *exactly as its author wrote it* — so
    for a Python object the stored reference is an import path, ``Files.X__Y``,
    not a ``Schema.Object`` name. Reversing the graph means reapplying the rule
    that turned one into an identity.

    Worth its own test because the shape is easy to miss from a fixture: a
    repository whose dependencies are all declared in SQL never produces one, and
    an orchestrator that silently dropped these would build a graph with the
    right nodes and no edges between them.
    """

    for relative, text in {
        f"{LOAD_PRODUCER}/schemas/Sales.yml": schema_document("Sales"),
        f"{LOAD_PRODUCER}/Files/Sales__Drop.py": folder_document("Sales.Drop"),
        f"{LOAD_PRODUCER}/Sales__Customer.py": '''\
"""
Table ID: Sales.Customer

Description: Customers, read from the files the folder delivers.

Lineage: $Files/Sales.Drop

Primary key: CustomerId

Schema:
  CustomerId: string
"""
from Files.Sales__Drop import Sales__Drop

from weaver import Table


class Sales__Customer(Table):
    def read(self):
        return self.spark.read.csv(Sales__Drop(self).path()), None
''',
    }.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    repository = parse_item_repository(Location(str(tmp_path)))
    catalogue = installed_catalogue(
        repository, item_bindings((LOAD_PRODUCER, LOAD_PRODUCER_TARGET))
    )

    # The stored reference really is the import, not a two-part object name.
    estate = InstalledEstate.from_catalogue(catalogue)
    assert [edge.reference for edge in estate.dependencies] == ["Files.Sales__Drop"]

    dag = load_dag(estate, targets=(RAW,))
    assert dag.edges == (
        ("load:Lakehouse/Raw_LH/Sales.Drop", "load:Lakehouse/Raw_LH/Sales.Customer"),
    )


def test_load_dag_crosses_items_through_aliases(estate):
    """The Warehouse consumer's upstream is the Lakehouse table, not the alias."""

    dag = load_dag(estate, targets=(RAW, REPORTING))
    consumer = "load:Warehouse/Reporting_WH/Sales.Summary"

    assert "load:Lakehouse/Raw_LH/Sales.Order" in dag.by_id
    # And the crossing is represented physically: the consumer waits on the
    # barrier rather than on the producer directly.
    assert dag.upstream(consumer) == {"refresh:Lakehouse/Raw_LH"}


def test_load_dag_inserts_endpoint_refresh_before_alias_consumers(estate):
    dag = load_dag(estate, targets=(RAW, REPORTING))

    assert (
        "load:Lakehouse/Raw_LH/Sales.Order",
        "refresh:Lakehouse/Raw_LH",
    ) in dag.edges
    assert (
        "refresh:Lakehouse/Raw_LH",
        "load:Warehouse/Reporting_WH/Sales.Summary",
    ) in dag.edges


def test_load_dag_places_the_barrier_after_every_selected_load_in_that_lakehouse(
    estate,
):
    dag = load_dag(estate, targets=(RAW, REPORTING))

    assert dag.upstream("refresh:Lakehouse/Raw_LH") == {
        "load:Lakehouse/Raw_LH/Sales.Order",
        "load:Lakehouse/Raw_LH/Sales.Daily",
        "load:Lakehouse/Raw_LH/Sales.Export",
    }


def test_load_dag_coalesces_one_endpoint_refresh_per_lakehouse(tmp_path):
    """Two crossings out of one Lakehouse are one barrier, not two."""

    for relative, text in {
        f"{LOAD_PRODUCER}/schemas/Sales.yml": schema_document("Sales"),
        f"{LOAD_PRODUCER}/Sales__Order.py": lakehouse_table("Sales.Order"),
        f"{LOAD_PRODUCER}/Sales__Customer.py": lakehouse_table("Sales.Customer"),
        f"{LOAD_CONSUMER}/schemas/Sales.yml": schema_document("Sales"),
        f"{LOAD_CONSUMER}/alias.yml": alias_declaration(
            **{
                "Sales.Order": f"{LOAD_PRODUCER}/Sales.Order",
                "Sales.Customer": f"{LOAD_PRODUCER}/Sales.Customer",
            }
        ),
        f"{LOAD_CONSUMER}/Sales.Summary.sql": warehouse_table(
            "Sales.Summary",
            select=(
                "select o.CustomerId from [Sales].[Order] as o "
                "join [Sales].[Customer] as c on c.CustomerId = o.CustomerId"
            ),
        ),
    }.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    repository = parse_item_repository(Location(str(tmp_path)))
    dag = LoadDag.from_catalogue(
        installed_catalogue(repository, load_estate_bindings()),
        targets=(RAW, REPORTING),
    )

    refreshes = [node for node in dag.nodes if node.primitive_kind == ENDPOINT_REFRESH]
    assert [node.node_id for node in refreshes] == ["refresh:Lakehouse/Raw_LH"]
    assert dag.upstream("refresh:Lakehouse/Raw_LH") == {
        "load:Lakehouse/Raw_LH/Sales.Order",
        "load:Lakehouse/Raw_LH/Sales.Customer",
    }


def test_load_dag_is_deterministic(estate):
    once = load_dag(estate, targets=(RAW, REPORTING))
    again = load_dag(estate, targets=(REPORTING, RAW))

    assert node_ids(once) == node_ids(again)
    assert once.edges == again.edges


# --- refusals -----------------------------------------------------------------
#
# Hand-written rows, because a repository that parses cannot express these. The
# catalogue can: it is a record of what was installed, and an estate can be
# damaged after the fact.


def _rows(**tables):
    return {name: tuple(rows) for name, rows in tables.items()}


def _installation(item: str, target: str):
    identity = WeaverItemId.parse(item)
    return {
        "item_type": identity.item_type,
        "item_name": identity.item_name,
        "target_name": target,
        "weaver_version": "0.0.0+test",
        "signature": "installation-signature",
    }


def _registry(item: str, schema: str, name: str, *, object_type="table", role="data"):
    identity = WeaverItemId.parse(item)
    return {
        "item_type": identity.item_type,
        "item_name": identity.item_name,
        "schema_name": schema,
        "object_name": name,
        "object_type": object_type,
        "object_role": role,
        "signature": f"{schema}.{name}",
        "build_datetime": None,
    }


def _dependency(item: str, schema: str, name: str, reference: str, within=True):
    identity = WeaverItemId.parse(item)
    referenced = identity if within else WeaverItemId.parse("Lakehouse/Elsewhere")
    return {
        "item_type": identity.item_type,
        "item_name": identity.item_name,
        "referencing_schema_name": schema,
        "referencing_object_name": name,
        "dependency_reference": reference,
        "referenced_item_type": referenced.item_type,
        "referenced_item_name": referenced.item_name,
        "referenced_schema_name": schema,
        "referenced_object_name": reference.rpartition(".")[2],
        "signature": "dependency",
    }


def _catalogue(**by_item):
    return Catalogue(
        rows={WeaverItemId.parse(item): tables for item, tables in by_item.items()}
    )


def _colliding_catalogue(target: str = "Shared_LH"):
    """Two items that each installed ``Sales.Order`` into one physical target."""

    return _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", target)],
                Registry=[_registry("Lakehouse/Raw", "Sales", "Order")],
            ),
            "Lakehouse/Staging": _rows(
                Installation=[_installation("Lakehouse/Staging", target)],
                Registry=[_registry("Lakehouse/Staging", "Sales", "Order")],
            ),
        }
    )


def test_load_dag_rejects_ambiguous_physical_bindings():
    """Two logical objects cannot resolve to one physical object."""

    estate = InstalledEstate.from_catalogue(_colliding_catalogue())

    with pytest.raises(LoadError, match="two logical objects at one physical address"):
        load_dag(estate, targets=(PhysicalTargetRef("lakehouse", "Shared_LH"),))


def test_ambiguity_elsewhere_in_the_estate_does_not_stop_an_unrelated_load(estate):
    """A stale duplicate in one target is not a fault report about another.

    An estate accumulates a Registry row for every item ever bound to a target,
    so a rebound Warehouse can carry a duplicated address indefinitely. Refusing
    every load in the workspace because of it would name the wrong thing.
    """

    colliding = InstalledEstate.from_catalogue(_colliding_catalogue("Elsewhere_LH"))
    assert colliding.ambiguous

    # The canonical estate is untouched by a collision it does not contain.
    dag = load_dag(estate, targets=(RAW,))

    assert node_ids(dag) == (
        "load:Lakehouse/Raw_LH/Sales.Export",
        "load:Lakehouse/Raw_LH/Sales.Order",
        "load:Lakehouse/Raw_LH/Sales.Daily",
    )


def test_two_items_may_share_a_target_when_their_objects_do_not_collide():
    """A request names a target and means everything installed there.

    An estate accumulates an Installation row for every item ever bound to a
    target, so refusing the *item* overlap would stop a load of a target whose
    objects are perfectly unambiguous.
    """

    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Shared_LH")],
                Registry=[
                    _registry("Lakehouse/Raw", "Sales", "Order"),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load",
                        "Sales__Order.py",
                        object_type="file",
                        role="load",
                    ),
                ],
            ),
            "Lakehouse/Staging": _rows(
                Installation=[_installation("Lakehouse/Staging", "Shared_LH")],
                Registry=[
                    _registry("Lakehouse/Staging", "Sales", "Customer"),
                    _registry(
                        "Lakehouse/Staging",
                        "_/Load",
                        "Sales__Customer.py",
                        object_type="file",
                        role="load",
                    ),
                ],
            ),
        }
    )
    dag = load_dag(
        InstalledEstate.from_catalogue(catalogue),
        targets=(PhysicalTargetRef("lakehouse", "Shared_LH"),),
    )

    # Ordered by *logical* identity, so the two items' objects interleave by
    # item name rather than by the physical name they share.
    assert node_ids(dag) == (
        "load:Lakehouse/Shared_LH/Sales.Order",
        "load:Lakehouse/Shared_LH/Sales.Customer",
    )


def test_load_dag_rejects_missing_bindings():
    """A certified object whose item names no physical target stops planning."""

    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Registry=[_registry("Lakehouse/Raw", "Sales", "Order")],
            )
        }
    )

    with pytest.raises(LoadError, match="has no installation row"):
        InstalledEstate.from_catalogue(catalogue)


def test_load_dag_rejects_unresolved_dependencies():
    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Raw_LH")],
                Registry=[
                    _registry("Lakehouse/Raw", "Sales", "Order"),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load",
                        "Sales__Order.py",
                        object_type="file",
                        role="load",
                    ),
                ],
                Dependency=[
                    _dependency("Lakehouse/Raw", "Sales", "Order", "Sales.Nowhere")
                ],
            )
        }
    )
    estate = InstalledEstate.from_catalogue(catalogue)

    with pytest.raises(LoadError, match="resolves to neither an installed object"):
        load_dag(estate, targets=(RAW,))


def test_load_dag_rejects_cycles():
    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Raw_LH")],
                Registry=[
                    _registry("Lakehouse/Raw", "Sales", "Order"),
                    _registry("Lakehouse/Raw", "Sales", "Customer"),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load",
                        "Sales__Order.py",
                        object_type="file",
                        role="load",
                    ),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load",
                        "Sales__Customer.py",
                        object_type="file",
                        role="load",
                    ),
                ],
                Dependency=[
                    _dependency("Lakehouse/Raw", "Sales", "Order", "Sales.Customer"),
                    _dependency("Lakehouse/Raw", "Sales", "Customer", "Sales.Order"),
                ],
            )
        }
    )
    estate = InstalledEstate.from_catalogue(catalogue)

    with pytest.raises(LoadError, match="contains a cycle"):
        load_dag(estate, targets=(RAW,))


def test_load_dag_ignores_a_fully_qualified_physical_read():
    """A three-part read names something outside the estate's own graph."""

    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Raw_LH")],
                Registry=[
                    _registry("Lakehouse/Raw", "Sales", "Order"),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load",
                        "Sales__Order.py",
                        object_type="file",
                        role="load",
                    ),
                ],
                Dependency=[
                    _dependency(
                        "Lakehouse/Raw",
                        "Sales",
                        "Order",
                        "other_ws.other_lh.Sales.Source",
                        within=False,
                    )
                ],
            )
        }
    )
    dag = load_dag(InstalledEstate.from_catalogue(catalogue), targets=(RAW,))

    assert node_ids(dag) == ("load:Lakehouse/Raw_LH/Sales.Order",)
    assert [message.code for message in dag.messages] == ["dependency_external"]
