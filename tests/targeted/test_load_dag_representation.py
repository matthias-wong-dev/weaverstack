"""What the installed catalogue says the physical load graph is.

Pure Python throughout: no Spark session, no SQL connection, no real target. The
subject is the arithmetic that turns installed state into *what runs and in
what order*, and that arithmetic reads nothing else.

Two kinds of fixture appear here. Claims about a well-formed estate
use `installed_catalogue`, which is composed from the production projection, so
what is planned against is what a build actually publishes. Claims about a
malformed estate hand-write rows, because a repository that parses cannot
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
    folder_document,
    installed_catalogue,
    item_bindings,
    lakehouse_table,
    load_estate,
    load_estate_bindings,
    logical_shortcuts,
    schema_document,
    shortcut_row,
    warehouse_table,
)
from support.weaver_test import weaver_test

from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.errors import CatalogueStateError, GraphError, LoadError
from weaver.installed import (
    PYTHON_FOLDER,
    PYTHON_TABLE,
    WAREHOUSE_PROCEDURE,
)
from weaver.load_plan import ENDPOINT_REFRESH, LoadDag, load_dag
from weaver.targets import PhysicalTargetRef

RAW = PhysicalTargetRef("lakehouse", LOAD_PRODUCER_TARGET)
REPORTING = PhysicalTargetRef("warehouse", LOAD_CONSUMER_TARGET)

#: What a request names: the logical items, not the targets they are installed in.
PRODUCER = WeaverItemId.parse(LOAD_PRODUCER)
CONSUMER = WeaverItemId.parse(LOAD_CONSUMER)


@pytest.fixture
def estate(tmp_path):
    repository = load_estate(tmp_path)
    return installed_catalogue(repository, load_estate_bindings()).dag()


def node_ids(dag) -> tuple[str, ...]:
    return tuple(node.node_id for node in dag.order())


# --- reversing the build's own binding ---------------------------------------


@weaver_test()
def test_the_installed_graph_maps_logical_items_to_physical_targets(estate):
    assert estate.installations == {
        WeaverItemId.parse(LOAD_PRODUCER): RAW,
        WeaverItemId.parse(LOAD_CONSUMER): REPORTING,
    }
    assert estate.node(f"{LOAD_PRODUCER}/Tables/Sales.Order").target == RAW


@weaver_test()
def test_the_installed_graph_answers_where_one_logical_item_lives(estate):
    """The runtime authority a load and a test both read.

    One reading of ``_.Installation``, in one place. Nothing else interprets it.
    """

    assert estate.target_for(PRODUCER) == RAW
    assert estate.target_for(CONSUMER) == REPORTING

    with pytest.raises(CatalogueStateError, match="has no installation row"):
        estate.target_for(WeaverItemId.parse("Lakehouse/Absent"))


@weaver_test()
def test_two_logical_items_may_share_one_physical_target():
    """The estate this whole selection boundary exists for.

    Both items are installed in ``Shared_LH``, and the graph says so for each of
    them. What each item owns is a separate question, which the selection tests
    below answer.
    """

    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Shared_LH")],
                Registry=[_registry("Lakehouse/Raw", "Sales", "Order")],
            ),
            "Lakehouse/Staging": _rows(
                Installation=[_installation("Lakehouse/Staging", "Shared_LH")],
                Registry=[_registry("Lakehouse/Staging", "Sales", "Customer")],
            ),
        }
    )
    estate = catalogue.dag()
    shared = PhysicalTargetRef("lakehouse", "Shared_LH")

    assert estate.target_for(WeaverItemId.parse("Lakehouse/Raw")) == shared
    assert estate.target_for(WeaverItemId.parse("Lakehouse/Staging")) == shared
    # One target, named once, whichever items are bound to it.
    assert estate.targets == (shared,)


@weaver_test()
def test_load_dag_finds_the_installed_primitive_for_each_dispatch_kind(estate):
    dag = load_dag(estate, items=(PRODUCER, CONSUMER))

    assert {node.node_id: node.primitive_kind for node in dag.nodes} == {
        "load:Lakehouse/Raw_LH/Files/Sales.Export": PYTHON_FOLDER,
        "load:Lakehouse/Raw_LH/Tables/Sales.Order": PYTHON_TABLE,
        # A Spark-SQL-authored table dispatches as what it installs as.
        "load:Lakehouse/Raw_LH/Tables/Sales.Daily": PYTHON_TABLE,
        "refresh:Lakehouse/Raw_LH": ENDPOINT_REFRESH,
        "load:Warehouse/Reporting_WH/Sales.Summary": WAREHOUSE_PROCEDURE,
    }


# --- what one request selects -------------------------------------------------


@weaver_test()
def test_load_dag_loads_every_object_in_the_requested_targets(estate):
    dag = load_dag(estate, items=(PRODUCER,))

    assert node_ids(dag) == (
        "load:Lakehouse/Raw_LH/Files/Sales.Export",
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "load:Lakehouse/Raw_LH/Tables/Sales.Daily",
    )


@weaver_test()
def test_load_dag_excludes_objects_that_own_no_load_primitive(estate):
    """A view and the generated runtime folder are installed and not loadable."""

    dag = load_dag(estate, items=(PRODUCER, CONSUMER))
    logical = {str(node.logical_id) for node in dag.nodes if node.logical_id}

    assert f"{LOAD_CONSUMER}/Sales.Live" not in logical
    assert f"{LOAD_PRODUCER}/Files/_.Load" not in logical


@weaver_test()
def test_load_dag_keeps_a_single_target_as_a_hard_boundary(estate):
    dag = load_dag(estate, items=(CONSUMER,))

    assert node_ids(dag) == ("load:Warehouse/Reporting_WH/Sales.Summary",)
    assert dag.edges == ()


@weaver_test()
def test_load_dag_excludes_unrelated_downstream_objects(estate):
    dag = load_dag(estate, items=(PRODUCER,))

    assert "load:Warehouse/Reporting_WH/Sales.Summary" not in dag.by_id


@weaver_test()
def test_load_dag_crosses_targets_only_when_both_are_requested(estate):
    dag = load_dag(estate, items=(PRODUCER, CONSUMER))

    assert "load:Lakehouse/Raw_LH/Tables/Sales.Order" in dag.by_id
    assert "refresh:Lakehouse/Raw_LH" in dag.by_id
    assert "load:Warehouse/Reporting_WH/Sales.Summary" in dag.by_id


@weaver_test()
def test_names_select_exact_nodes_without_dependencies_or_edges(estate):
    dag = load_dag(
        estate,
        items=(PRODUCER,),
        names=("Sales.Order", "Sales.Daily"),
    )

    assert set(node_ids(dag)) == {
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "load:Lakehouse/Raw_LH/Tables/Sales.Daily",
    }
    assert dag.edges == ()


@weaver_test()
def test_a_name_is_resolved_case_insensitively_within_the_requested_targets(estate):
    dag = load_dag(estate, items=(PRODUCER,), names=("sales.order",))

    assert node_ids(dag) == ("load:Lakehouse/Raw_LH/Tables/Sales.Order",)


@weaver_test()
def test_an_unknown_load_name_lists_the_installed_loadables(estate):
    with pytest.raises(LoadError, match="no loadable object named 'Sales.Missing'"):
        load_dag(estate, items=(PRODUCER,), names=("Sales.Missing",))


# --- ordering -----------------------------------------------------------------


@weaver_test()
def test_load_dag_orders_direct_dependencies(estate):
    dag = load_dag(estate, items=(PRODUCER,))

    assert (
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "load:Lakehouse/Raw_LH/Tables/Sales.Daily",
    ) in dag.edges


@weaver_test()
def test_load_dag_resolves_a_python_import_as_a_dependency(tmp_path):
    """A Python object declares its dependencies by importing them.

    And the catalogue records a dependency *exactly as its author wrote it*, so
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
        f"{LOAD_PRODUCER}/Tables/Sales__Customer.py": '''\
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
    estate = catalogue.dag()
    assert [edge.reference for edge in estate.edges] == ["Files.Sales__Drop"]

    dag = load_dag(estate, items=(PRODUCER,))
    assert dag.edges == (
        (
            "load:Lakehouse/Raw_LH/Files/Sales.Drop",
            "load:Lakehouse/Raw_LH/Tables/Sales.Customer",
        ),
    )


@weaver_test()
def test_load_dag_crosses_items_through_shortcutes(estate):
    """The Warehouse consumer's upstream is the Lakehouse table, not the shortcut."""

    dag = load_dag(estate, items=(PRODUCER, CONSUMER))
    consumer = "load:Warehouse/Reporting_WH/Sales.Summary"

    assert "load:Lakehouse/Raw_LH/Tables/Sales.Order" in dag.by_id
    # And the crossing is represented physically: the consumer waits on the
    # barrier rather than on the producer directly.
    assert dag.upstream(consumer) == {"refresh:Lakehouse/Raw_LH"}


@weaver_test()
def test_load_dag_inserts_endpoint_refresh_before_shortcut_consumers(estate):
    dag = load_dag(estate, items=(PRODUCER, CONSUMER))

    assert (
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "refresh:Lakehouse/Raw_LH",
    ) in dag.edges
    assert (
        "refresh:Lakehouse/Raw_LH",
        "load:Warehouse/Reporting_WH/Sales.Summary",
    ) in dag.edges


@weaver_test()
def test_load_dag_places_the_barrier_after_every_selected_load_in_that_lakehouse(
    estate,
):
    dag = load_dag(estate, items=(PRODUCER, CONSUMER))

    assert dag.upstream("refresh:Lakehouse/Raw_LH") == {
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "load:Lakehouse/Raw_LH/Tables/Sales.Daily",
        "load:Lakehouse/Raw_LH/Files/Sales.Export",
    }


#: The consumer's two bound references to the producer, on the surface a
#: Warehouse declares them.
_CONSUMER_REFERENCES = logical_shortcuts(
    LOAD_CONSUMER,
    **{
        "Sales.Order": f"{LOAD_PRODUCER}/Tables/Sales.Order",
        "Sales.Customer": f"{LOAD_PRODUCER}/Tables/Sales.Customer",
    },
)


@weaver_test()
def test_load_dag_coalesces_one_endpoint_refresh_per_lakehouse(tmp_path):
    """Two crossings out of one Lakehouse are one barrier, not two."""

    for relative, text in {
        f"{LOAD_PRODUCER}/schemas/Sales.yml": schema_document("Sales"),
        f"{LOAD_PRODUCER}/Tables/Sales__Order.py": lakehouse_table("Sales.Order"),
        f"{LOAD_PRODUCER}/Tables/Sales__Customer.py": lakehouse_table("Sales.Customer"),
        f"{LOAD_CONSUMER}/schemas/Sales.yml": schema_document("Sales"),
        _CONSUMER_REFERENCES[0]: _CONSUMER_REFERENCES[1],
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
        items=(PRODUCER, CONSUMER),
    )

    refreshes = [node for node in dag.nodes if node.primitive_kind == ENDPOINT_REFRESH]
    assert [node.node_id for node in refreshes] == ["refresh:Lakehouse/Raw_LH"]
    assert dag.upstream("refresh:Lakehouse/Raw_LH") == {
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "load:Lakehouse/Raw_LH/Tables/Sales.Customer",
    }


@weaver_test()
def test_load_dag_is_deterministic(estate):
    once = load_dag(estate, items=(PRODUCER, CONSUMER))
    again = load_dag(estate, items=(CONSUMER, PRODUCER))

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
    """One Registry row, keyed as the catalogue keys it.

    A Lakehouse relation names ``Tables``; a Folder and a load artefact arrive
    with the schema they are stored under already spelled out.
    """

    identity = WeaverItemId.parse(item)
    if (
        identity.item_type == "Lakehouse"
        and object_type in ("table", "view")
        and "/" not in schema
    ):
        schema = f"Tables/{schema}"
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
    """One Dependency row, keyed as the declaring object is keyed."""

    identity = WeaverItemId.parse(item)
    referenced = identity if within else WeaverItemId.parse("Lakehouse/Elsewhere")
    if identity.item_type == "Lakehouse" and "/" not in schema:
        schema = f"Tables/{schema}"
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


@weaver_test()
def test_load_dag_rejects_ambiguous_physical_bindings():
    """Two logical objects cannot resolve to one physical object.

    Refused for the target, not for the item: the collision is physical, so a
    dispatch into ``Shared_LH`` is ambiguous even though only one of the two
    items claiming that address was requested.
    """

    estate = _colliding_catalogue().dag()

    with pytest.raises(LoadError, match="two logical objects at one physical address"):
        load_dag(estate, items=(WeaverItemId.parse("Lakehouse/Raw"),))


@weaver_test()
def test_ambiguity_elsewhere_in_the_estate_does_not_stop_an_unrelated_load(estate):
    """A stale duplicate in one target is not a fault report about another.

    An estate accumulates a Registry row for every item ever bound to a target,
    so a rebound Warehouse can carry a duplicated address indefinitely. Refusing
    every load in the workspace because of it would name the wrong thing.
    """

    colliding = _colliding_catalogue("Elsewhere_LH").dag()
    assert colliding.ambiguous

    # The canonical estate is untouched by a collision it does not contain.
    dag = load_dag(estate, items=(PRODUCER,))

    assert node_ids(dag) == (
        "load:Lakehouse/Raw_LH/Files/Sales.Export",
        "load:Lakehouse/Raw_LH/Tables/Sales.Order",
        "load:Lakehouse/Raw_LH/Tables/Sales.Daily",
    )


@weaver_test()
def test_a_request_for_one_of_two_items_sharing_a_target_loads_that_item_alone():
    """The execution boundary is the logical item, not the physical container.

    Two items are installed in ``Shared_LH`` and their objects do not collide.
    Naming one of them loads its object. The other's is in the same Lakehouse and
    is no part of what was asked for.
    """

    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Shared_LH")],
                Registry=[
                    _registry("Lakehouse/Raw", "Sales", "Order"),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load/Tables",
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
                        "_/Load/Tables",
                        "Sales__Customer.py",
                        object_type="file",
                        role="load",
                    ),
                ],
            ),
        }
    )
    dag = load_dag(catalogue.dag(), items=(WeaverItemId.parse("Lakehouse/Raw"),))

    # Raw's object, dispatched at the Lakehouse the two items share. Staging's
    # is installed there too and was not requested.
    assert node_ids(dag) == ("load:Lakehouse/Shared_LH/Tables/Sales.Order",)

    both = load_dag(
        catalogue.dag(),
        items=(
            WeaverItemId.parse("Lakehouse/Raw"),
            WeaverItemId.parse("Lakehouse/Staging"),
        ),
    )

    # Ordered by logical identity, so the two items' objects interleave by
    # item name rather than by the physical name they share.
    assert node_ids(both) == (
        "load:Lakehouse/Shared_LH/Tables/Sales.Order",
        "load:Lakehouse/Shared_LH/Tables/Sales.Customer",
    )


@weaver_test()
def test_load_dag_rejects_missing_bindings():
    """A certified object whose item names no physical target stops planning."""

    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Registry=[_registry("Lakehouse/Raw", "Sales", "Order")],
            )
        }
    )

    with pytest.raises(CatalogueStateError, match="has no installation row"):
        catalogue.dag()


@weaver_test()
def test_load_dag_rejects_unresolved_dependencies():
    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Raw_LH")],
                Registry=[
                    _registry("Lakehouse/Raw", "Sales", "Order"),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load/Tables",
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
    estate = catalogue.dag()

    with pytest.raises(LoadError, match="resolves to neither an installed object"):
        load_dag(estate, items=(PRODUCER,))


@weaver_test()
def test_a_stored_import_naming_no_area_is_refused():
    """An estate built before the areas were explicit recorded ``Sales__Order``.

    That names an object module and no area, and there is one of each in the
    item. Refused, because dropping the edge would lose an ordering silently and
    a build records the reference again.
    """

    catalogue = _catalogue(
        **{
            "Lakehouse/Raw": _rows(
                Installation=[_installation("Lakehouse/Raw", "Raw_LH")],
                Registry=[
                    _registry("Lakehouse/Raw", "Sales", "Order"),
                    _registry("Lakehouse/Raw", "Sales", "Customer"),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load/Tables",
                        "Sales__Order.py",
                        object_type="file",
                        role="load",
                    ),
                ],
                Dependency=[
                    _dependency("Lakehouse/Raw", "Sales", "Order", "Sales__Customer")
                ],
            )
        }
    )

    with pytest.raises(LoadError, match="names an object module"):
        load_dag(catalogue.dag(), items=(PRODUCER,))


@weaver_test()
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
                        "_/Load/Tables",
                        "Sales__Order.py",
                        object_type="file",
                        role="load",
                    ),
                    _registry(
                        "Lakehouse/Raw",
                        "_/Load/Tables",
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
    with pytest.raises(GraphError, match="dependency cycle"):
        catalogue.dag()


@weaver_test()
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
                        "_/Load/Tables",
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
    dag = load_dag(catalogue.dag(), items=(PRODUCER,))

    assert node_ids(dag) == ("load:Lakehouse/Raw_LH/Tables/Sales.Order",)
    assert [message.code for message in dag.messages] == ["dependency_external"]


# --- reconstructing what a shortcut import named -------------------------------
#
# The catalogue records a dependency as its author wrote it, so load resolution
# has to turn `shortcuts.<Name>` back into the object it named. Neither of these
# is Fabric behaviour: both are decisions about what an installed name means.


def _shortcut_estate(root):
    """One item importing each kind of shortcut it declares."""

    from factories import _write, lakehouse_table, schema_document

    item = "Lakehouse/Curated"
    _write(root, f"{item}/schemas/Sales.yml", schema_document("Sales"))
    _write(
        root,
        f"{item}/shortcuts.py",
        "from weaver import Shortcut\n\n"
        "Sales__External = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/Tables/Ref.Customer",\n'
        '    workspace="Shared Data",\n)\n\n'
        "Sales__Incoming = Shortcut(\n"
        '    shortcut_type="folder",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Landing/Files/Incoming",\n'
        '    workspace="Shared Data",\n)\n\n'
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/Ref",\n'
        '    workspace="Shared Data",\n)\n',
    )
    _write(
        root,
        f"{item}/Tables/Sales__Report.py",
        lakehouse_table("Sales.Report").replace(
            "from weaver import Table",
            "from shortcuts import Reference, Sales__External, Sales__Incoming\n\n"
            "from weaver import Table",
        ),
    )
    return item


def _resolved_producers(tmp_path):
    """What each written shortcut import resolves to, against the estate."""

    from factories import installed_catalogue, item_bindings

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    root = tmp_path / "shortcut-estate"
    item = _shortcut_estate(root)
    repository = parse_item_repository(Location(str(root)))
    bindings = item_bindings((item, "Curated_LH"))
    estate = installed_catalogue(repository, bindings).dag()

    consumer = WeaverDocumentId.parse(f"{item}/Tables/Sales.Report")
    dag = load_dag(estate, items=(WeaverItemId.parse(item),))
    return estate, consumer, dag


@weaver_test()
def test_a_folder_shortcut_import_resolves_beneath_files(tmp_path):
    """A shortcut import says nothing about which area its destination is in.

    Resolved as a table, a folder shortcut is not an installed object and the
    load refuses a name that is plainly there.
    """

    estate, _consumer, _dag = _resolved_producers(tmp_path)

    assert WeaverDocumentId.parse("Lakehouse/Curated/Files/Sales.Incoming") in estate
    assert (
        WeaverDocumentId.parse("Lakehouse/Curated/Tables/Sales.Incoming") not in estate
    )


@weaver_test()
def test_a_physical_shortcut_import_is_an_external_read(tmp_path):
    """Each of the three points outside the estate, so none of them orders.

    ``shortcuts.Reference`` carries no ``Schema__Object`` separator, and a
    schema shortcut is always physical: what appears inside the namespace
    belongs to the item it points at.
    """

    estate, consumer, _dag = _resolved_producers(tmp_path)

    assert set(estate.external_references[consumer]) == {
        "shortcuts.Reference",
        "shortcuts.Sales__External",
        "shortcuts.Sales__Incoming",
    }
    assert not [edge for edge in estate.edges if edge.downstream == consumer]


@weaver_test()
def test_a_program_importing_shortcuts_still_loads(tmp_path):
    """The composition: every kind resolves, so the item has a load DAG."""

    _estate, consumer, dag = _resolved_producers(tmp_path)

    assert any(node.logical_id == consumer for node in dag.nodes)


# --- one Schema.Object in two areas --------------------------------------------
#
# A Lakehouse holds `Files/Sales.Customer` as a physical folder shortcut and
# `Sales.Customer` as the Delta table built from what it points at. Two Weaver
# identities, both legitimate, and the table imports the shortcut. The
# declaration is what says which of the two `shortcuts.Sales__Customer` names.
# Derived from the symbol's spelling, it found the table and reported the table
# as reading itself.


def _same_name_estate(root):
    """A Folder shortcut and an owned table sharing one ``Schema.Object``."""

    from factories import _write, schema_document

    item = "Lakehouse/Curated"
    _write(root, f"{item}/schemas/Sales.yml", schema_document("Sales"))
    _write(
        root,
        f"{item}/shortcuts.py",
        "from weaver import Shortcut\n\n"
        "Sales__Customer = Shortcut(\n"
        '    shortcut_type="folder",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Landing/Files/Sales/Customer",\n'
        '    workspace="Shared Data",\n)\n',
    )
    # The import is aliased because the module already binds the name to its
    # own class. The Weaver identities are what collide; the Python names are
    # incidental.
    _write(
        root,
        f"{item}/Tables/Sales__Customer.py",
        '''\
"""
Table ID: Sales.Customer
Description: The table built from the folder shortcut of the same name.
Lineage: A source system.
Primary key: CustomerId
Schema:
  CustomerId: string
"""
from shortcuts import Sales__Customer as SourceFolder

from weaver import Table

class Sales__Customer(Table):
    def read(self):
        return SourceFolder(self).path()
''',
    )
    return item


def _same_name_estate_dag(tmp_path):
    """The whole round trip: repository, published catalogue, installed graph."""

    from factories import installed_catalogue, item_bindings

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    root = tmp_path / "same-name-estate"
    item = _same_name_estate(root)
    repository = parse_item_repository(Location(str(root)))
    catalogue = installed_catalogue(repository, item_bindings((item, "Curated_LH")))
    return item, catalogue.dag()


@weaver_test()
def test_a_folder_shortcut_and_a_table_of_one_name_are_two_installed_nodes(tmp_path):
    """The catalogue round trip keeps the two identities apart."""

    item, estate = _same_name_estate_dag(tmp_path)

    assert WeaverDocumentId.parse(f"{item}/Files/Sales.Customer") in estate
    assert WeaverDocumentId.parse(f"{item}/Tables/Sales.Customer") in estate


@weaver_test()
def test_a_table_importing_the_folder_shortcut_it_shares_a_name_with_is_external(
    tmp_path,
):
    """The declaration answers, so the import is the physical read it is."""

    item, estate = _same_name_estate_dag(tmp_path)
    table = WeaverDocumentId.parse(f"{item}/Tables/Sales.Customer")

    assert estate.external_references[table] == ("shortcuts.Sales__Customer",)
    assert not estate.unresolved
    assert not [edge for edge in estate.edges if edge.downstream == table]


@weaver_test()
def test_a_table_importing_the_folder_shortcut_it_shares_a_name_with_loads(tmp_path):
    """The composition: the item has a load DAG, and the table is in it."""

    item, estate = _same_name_estate_dag(tmp_path)
    dag = load_dag(estate, items=(WeaverItemId.parse(item),))

    assert node_ids(dag) == ("load:Lakehouse/Curated_LH/Tables/Sales.Customer",)


@weaver_test()
def test_a_python_shortcut_import_orders_a_warehouse_before_its_lakehouse_consumer(
    tmp_path,
):
    """The installed import must retain the logical shortcut's producer hop."""

    from factories import _write, logical_shortcuts

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    producer = "Warehouse/Serving"
    consumer = "Lakehouse/Published"
    shortcut_path, shortcut_text = logical_shortcuts(
        consumer, **{"WH.Reporting": f"{producer}/SERVE.Reporting"}
    )
    for relative, text in {
        f"{producer}/schemas/SERVE.yml": schema_document("SERVE"),
        f"{producer}/SERVE.Reporting.sql": warehouse_table("SERVE.Reporting"),
        f"{consumer}/schemas/PUB.yml": schema_document("PUB"),
        f"{consumer}/schemas/WH.yml": schema_document("WH"),
        shortcut_path: shortcut_text,
        f"{consumer}/Tables/PUB__Reporting.py": lakehouse_table(
            "PUB.Reporting"
        ).replace(
            "from weaver import Table",
            "from shortcuts import WH__Reporting\n\nfrom weaver import Table",
        ),
    }.items():
        _write(tmp_path, relative, text)

    repository = parse_item_repository(Location(str(tmp_path)))
    catalogue = installed_catalogue(
        repository,
        item_bindings(
            (producer, "Serving_WH"),
            (consumer, "Published_LH"),
        ),
    )
    dag = load_dag(
        catalogue.dag(),
        items=(WeaverItemId.parse(producer), WeaverItemId.parse(consumer)),
    )

    from weaver.load_plan import ONELAKE_PUBLICATION, OneLakeReadiness

    producer = "load:Warehouse/Serving_WH/SERVE.Reporting"
    consumer = "load:Lakehouse/Published_LH/Tables/PUB.Reporting"
    barrier = "publish:Warehouse/Serving_WH/SERVE.Reporting"

    # The barrier replaces the direct edge, as the endpoint refresh does in the
    # other direction, so the consumer cannot start until it has settled.
    assert (producer, consumer) not in dag.edges
    assert (producer, barrier) in dag.edges
    assert (barrier, consumer) in dag.edges

    waiting = dag.by_id[barrier]
    assert waiting.primitive_kind == ONELAKE_PUBLICATION
    # No logical identity, so it leaves no catalogue state of its own.
    assert waiting.logical_id is None
    assert waiting.produced_by == producer
    assert str(waiting.publication_of) == "Warehouse/Serving/SERVE.Reporting"
    assert waiting.publication_targets == (
        OneLakeReadiness(
            target=PhysicalTargetRef("lakehouse", "Published_LH"),
            schema="WH",
            object="Reporting",
        ),
    )


# --- one Schema.Object in two physical forms -----------------------------------
#
# A Lakehouse may legitimately own both `Files/Sales.Thing` and `Sales.Thing`,
# and the table may read the folder. These are two installed identities, and the
# load graph has to keep them apart: a node keyed on `Schema.Object` alone gave
# both the same id, so the dependency between them became a self-edge and the
# graph reported a cycle.
#
# The matrix, with the same `Schema.Object` throughout:
#
#     Files/Sales.Thing     Sales.Thing
#     -----------------     ---------------
#     real Folder           real Table
#     folder shortcut       real Table
#     real Folder           table shortcut

SAME_NAME = "Sales.Thing"
SOURCE_ITEM = "Lakehouse/Source"
SOURCE_TARGET = "Source_LH"
CURATED_ITEM = "Warehouse/Curated"
CURATED_TARGET = "Curated_WH"


def _folder(item: str, schema: str, name: str, *, role="data"):
    """Registry rows for one Folder and its deployed load module."""

    return [
        _registry(item, f"Files/{schema}", name, object_type="folder", role=role),
        _registry(
            item,
            "_/Load/Files",
            f"{schema}__{name}.py",
            object_type="file",
            role="load",
        ),
    ]


def _delta_table(item: str, schema: str, name: str, *, role="data"):
    """Registry rows for one Lakehouse table and its deployed load module."""

    return [
        _registry(item, schema, name, object_type="table", role=role),
        _registry(
            item,
            "_/Load/Tables",
            f"{schema}__{name}.py",
            object_type="file",
            role="load",
        ),
    ]


def _warehouse_table(item: str, schema: str, name: str):
    """Registry rows for one Warehouse table and its generated procedure."""

    return [
        _registry(item, schema, name, object_type="table"),
        _registry(
            item,
            "_",
            f"Load {schema}.{name}",
            object_type="stored_procedure",
            role="load",
        ),
    ]


def _folder_import(item: str, schema: str, name: str):
    """The dependency row a table reading its same-name Folder leaves.

    The spelling is the authored import, which is what ``_.Dependency`` keeps:
    ``from Files.Sales__Thing import Sales__Thing``.
    """

    row = _dependency(item, schema, name, f"Files.{schema}__{name}")
    row["referenced_schema_name"] = f"Files/{schema}"
    return row


def _shortcut_import(item: str, schema: str, name: str):
    """The dependency row a table reading a declared shortcut leaves.

    A shortcut is imported from the ``shortcuts`` module, which is a different
    authored shape from an owned Folder's ``from Files.Sales__Thing import``.
    """

    row = _dependency(item, schema, name, f"shortcuts.{schema}__{name}")
    for column in (
        "referenced_item_type",
        "referenced_item_name",
        "referenced_schema_name",
        "referenced_object_name",
    ):
        row[column] = None
    return row


@weaver_test()
def test_a_real_folder_and_a_real_table_of_one_name_are_two_load_nodes(tmp_path):
    """
    Intent: Case 1. A Lakehouse owns the Folder and the Delta table at one
    ``Schema.Object``, the table reads the folder, and a Warehouse table reads
    the Delta table.

    Proof: keyed on ``Schema.Object`` alone the Folder and the table shared one
    load node, so the edge between them closed on itself and LoadDag refused the
    graph as cyclic.
    """

    catalogue = _catalogue(
        **{
            SOURCE_ITEM: _rows(
                Installation=[_installation(SOURCE_ITEM, SOURCE_TARGET)],
                Registry=[
                    *_folder(SOURCE_ITEM, "Sales", "Thing"),
                    *_delta_table(SOURCE_ITEM, "Sales", "Thing"),
                ],
                Dependency=[_folder_import(SOURCE_ITEM, "Sales", "Thing")],
            ),
            # A Warehouse crosses into the Lakehouse through a view shortcut, as
            # a Weaver repository does. Its own table keeps the business name.
            CURATED_ITEM: _rows(
                Installation=[_installation(CURATED_ITEM, CURATED_TARGET)],
                Registry=[
                    *_warehouse_table(CURATED_ITEM, "Sales", "Thing"),
                    _registry(CURATED_ITEM, "Sales", "ThingRaw", object_type="view"),
                ],
                Shortcut=[
                    shortcut_row(
                        f"{CURATED_ITEM}/Sales.ThingRaw",
                        f"{SOURCE_ITEM}/Tables/{SAME_NAME}",
                        shortcut_type="view",
                    )
                ],
                Dependency=[
                    _dependency(CURATED_ITEM, "Sales", "Thing", "Sales.ThingRaw")
                ],
            ),
        }
    )
    estate = catalogue.dag()

    # The installed graph keeps them apart, which is what the planner must carry.
    assert estate.node(f"{SOURCE_ITEM}/Files/{SAME_NAME}").object_type == "folder"
    assert estate.node(f"{SOURCE_ITEM}/Tables/{SAME_NAME}").object_type == "table"

    dag = load_dag(
        estate,
        items=(WeaverItemId.parse(SOURCE_ITEM), WeaverItemId.parse(CURATED_ITEM)),
    )

    folder_node = f"load:Lakehouse/{SOURCE_TARGET}/Files/{SAME_NAME}"
    table_node = f"load:Lakehouse/{SOURCE_TARGET}/Tables/{SAME_NAME}"

    # Two nodes, not one.
    assert folder_node in dag.by_id
    assert table_node in dag.by_id
    assert folder_node != table_node
    assert dag.by_id[folder_node].primitive_kind == PYTHON_FOLDER
    assert dag.by_id[table_node].primitive_kind == PYTHON_TABLE

    # No node depends on itself.
    assert not [(a, b) for a, b in dag.edges if a == b]

    # The folder orders before the table, and the table before the Warehouse
    # consumer, which reaches it over TDS and so waits on the endpoint barrier.
    barrier = f"refresh:Lakehouse/{SOURCE_TARGET}"
    warehouse_node = f"load:Warehouse/{CURATED_TARGET}/{SAME_NAME}"
    assert (folder_node, table_node) in dag.edges
    assert (table_node, barrier) in dag.edges
    assert (barrier, warehouse_node) in dag.edges
    assert dag.by_id[barrier].primitive_kind == ENDPOINT_REFRESH

    # And the whole chain orders one way, at one business name throughout.
    order = node_ids(dag)
    assert order.index(folder_node) < order.index(table_node)
    assert order.index(table_node) < order.index(warehouse_node)


@weaver_test()
def test_a_folder_shortcut_and_a_real_table_of_one_name_stay_apart(tmp_path):
    """
    Intent: Case 2. ``Files/Sales.Thing`` is a physical folder shortcut and
    ``Sales.Thing`` is the owned Delta table that reads it.

    Proof: the import resolves through the shortcut declaration, so it names the
    Folder form and is an external read. Resolved by spelling it found the table
    and reported the table as reading itself.
    """

    catalogue = _catalogue(
        **{
            SOURCE_ITEM: _rows(
                Installation=[_installation(SOURCE_ITEM, SOURCE_TARGET)],
                Registry=[
                    _registry(
                        SOURCE_ITEM,
                        "Files/Sales",
                        "Thing",
                        object_type="folder",
                        role="shortcut",
                    ),
                    *_delta_table(SOURCE_ITEM, "Sales", "Thing"),
                ],
                Shortcut=[
                    shortcut_row(
                        f"{SOURCE_ITEM}/Files/{SAME_NAME}",
                        "Lakehouse/Landing/Files/Sales.Thing",
                        shortcut_type="folder",
                        target_type="physical",
                    )
                ],
                Dependency=[_shortcut_import(SOURCE_ITEM, "Sales", "Thing")],
            )
        }
    )
    estate = catalogue.dag()
    table = WeaverDocumentId.parse(f"{SOURCE_ITEM}/Tables/{SAME_NAME}")

    # The shortcut is what the import names, so the read is external and the
    # table is not made to depend on itself.
    assert estate.external_references[table] == ("shortcuts.Sales__Thing",)
    assert not estate.unresolved

    dag = load_dag(estate, items=(WeaverItemId.parse(SOURCE_ITEM),))
    table_node = f"load:Lakehouse/{SOURCE_TARGET}/Tables/{SAME_NAME}"

    # The pointer holds no load primitive, so the table is the only loadable.
    assert node_ids(dag) == (table_node,)
    assert not [(a, b) for a, b in dag.edges if a == b]
    assert dag.by_id[table_node].primitive_kind == PYTHON_TABLE


@weaver_test()
def test_a_logical_folder_shortcut_keeps_its_managed_producer(tmp_path):
    """Case 2, logical form: the crossing survives the shared spelling.

    The same ``Files/Sales.Thing`` pointer, now bound to a managed Folder in
    another item, so the consuming table orders behind that Folder's load.
    """

    upstream = "Lakehouse/Landing"
    catalogue = _catalogue(
        **{
            upstream: _rows(
                Installation=[_installation(upstream, "Landing_LH")],
                Registry=[*_folder(upstream, "Sales", "Thing")],
            ),
            SOURCE_ITEM: _rows(
                Installation=[_installation(SOURCE_ITEM, SOURCE_TARGET)],
                Registry=[
                    _registry(
                        SOURCE_ITEM,
                        "Files/Sales",
                        "Thing",
                        object_type="folder",
                        role="shortcut",
                    ),
                    *_delta_table(SOURCE_ITEM, "Sales", "Thing"),
                ],
                Shortcut=[
                    shortcut_row(
                        f"{SOURCE_ITEM}/Files/{SAME_NAME}",
                        f"{upstream}/Files/{SAME_NAME}",
                        shortcut_type="folder",
                    )
                ],
                Dependency=[_shortcut_import(SOURCE_ITEM, "Sales", "Thing")],
            ),
        }
    )
    dag = load_dag(
        catalogue.dag(),
        items=(WeaverItemId.parse(upstream), WeaverItemId.parse(SOURCE_ITEM)),
    )

    producer = f"load:Lakehouse/Landing_LH/Files/{SAME_NAME}"
    consumer = f"load:Lakehouse/{SOURCE_TARGET}/Tables/{SAME_NAME}"

    assert producer in dag.by_id
    assert consumer in dag.by_id
    assert (producer, consumer) in dag.edges
    assert not [(a, b) for a, b in dag.edges if a == b]


@weaver_test()
def test_a_real_folder_and_a_table_shortcut_of_one_name_stay_apart(tmp_path):
    """
    Intent: Case 3, the forms reversed. ``Files/Sales.Thing`` is the owned
    Folder and ``Sales.Thing`` is a table shortcut onto another item.

    Proof: a downstream table reads ``Sales.Thing``, so the graph has to resolve
    that spelling through the shortcut. Collapsed identities would order it
    behind the Folder, or behind itself.
    """

    upstream = "Lakehouse/Landing"
    catalogue = _catalogue(
        **{
            upstream: _rows(
                Installation=[_installation(upstream, "Landing_LH")],
                Registry=[*_delta_table(upstream, "Sales", "Thing")],
            ),
            SOURCE_ITEM: _rows(
                Installation=[_installation(SOURCE_ITEM, SOURCE_TARGET)],
                Registry=[
                    *_folder(SOURCE_ITEM, "Sales", "Thing"),
                    _registry(SOURCE_ITEM, "Sales", "Thing", role="shortcut"),
                    *_delta_table(SOURCE_ITEM, "Sales", "Report"),
                ],
                Shortcut=[
                    shortcut_row(
                        f"{SOURCE_ITEM}/Tables/{SAME_NAME}",
                        f"{upstream}/Tables/{SAME_NAME}",
                    )
                ],
                Dependency=[_dependency(SOURCE_ITEM, "Sales", "Report", SAME_NAME)],
            ),
        }
    )
    estate = catalogue.dag()

    # Three installed identities at two spellings, all distinct.
    assert estate.node(f"{SOURCE_ITEM}/Files/{SAME_NAME}").role == "data"
    assert estate.node(f"{SOURCE_ITEM}/Tables/{SAME_NAME}").role == "shortcut"

    dag = load_dag(
        estate,
        items=(WeaverItemId.parse(upstream), WeaverItemId.parse(SOURCE_ITEM)),
    )

    folder_node = f"load:Lakehouse/{SOURCE_TARGET}/Files/{SAME_NAME}"
    report_node = f"load:Lakehouse/{SOURCE_TARGET}/Tables/Sales.Report"
    producer_node = f"load:Lakehouse/Landing_LH/Tables/{SAME_NAME}"

    # The Folder is its own loadable and did not collapse into the pointer.
    assert folder_node in dag.by_id
    assert dag.by_id[folder_node].primitive_kind == PYTHON_FOLDER

    # The read resolved through the shortcut to its managed producer. The Folder
    # shares the spelling and is a different identity.
    assert (producer_node, report_node) in dag.edges
    assert (folder_node, report_node) not in dag.edges
    assert not [(a, b) for a, b in dag.edges if a == b]
