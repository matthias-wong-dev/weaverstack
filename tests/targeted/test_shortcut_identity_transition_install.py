"""Selecting and planning an identity whose declared form has changed.

Two claims, and they are the same claim on either side of the seam.

Impact propagates through the repository dependency graph, so a changed identity
carries impact only where the graph holds that identity. More is selectable than
the graph holds: a runtime artefact, a schema shortcut, and a physical shortcut
destination, which names a Fabric item this repository does not manage and so has
no producer here to order it against.

Desired state is reconciled to the form now declared. A native folder standing
where a folder shortcut is now declared is removed before the shortcut is
materialised, and a pointer standing where a document is now declared is unpicked
through the shortcut API. Both go through the managed drop, which is where a
table-to-view change already goes.

The existing propagation is asserted alongside, because the correction is that
graph membership is the whole rule and nothing else about the walk moved.
"""

from __future__ import annotations

import pytest
from factories import (
    _write,
    bound_target,
    catalogue_target,
    document_id,
    item_id,
    lakehouse_table,
    physical_folder_shortcut,
    registered_document,
    schema_document,
    shortcut_repository,
    single_document_repository,
    spark_view,
    target_inventory,
)
from support.weaver_test import weaver_test

from weaver.build_bundle import plan_item_build
from weaver.build_bundle.incremental import (
    declared_signatures,
    determine_impact,
    select_build,
)
from weaver.build_bundle.shortcuts import ResolvedShortcutSource
from weaver.declaration import parse_item_repository
from weaver.locations import Location

ITEM = "Lakehouse/Landing"
FOLDER = f"{ITEM}/Files/ACQSC.HarmSurveyXlsx"
CONSUMER = f"{ITEM}/ACQSC.Consumer"
TARGET = "Lakehouse/Drop/Files/ACQSC/HarmSurveyXlsx"


@pytest.fixture
def estate(tmp_path):
    """The repository after the transition: a shortcut where a folder was.

    One item declaring a physical folder shortcut, and one document that imports
    it. The consumer is what makes the graph claim testable: it reads the
    shortcut symbol, so if a physical destination were a node there would be an
    edge to walk down.
    """

    root = tmp_path / "repo"
    _write(root, f"{ITEM}/schemas/ACQSC.yml", schema_document("ACQSC"))
    _write(
        root,
        *physical_folder_shortcut(
            ITEM, name="ACQSC.HarmSurveyXlsx", target=TARGET, workspace="Upstream"
        ),
    )
    _write(
        root,
        f"{ITEM}/ACQSC__Consumer.py",
        '"""\n'
        "Table ID: ACQSC.Consumer\n"
        "Description: Reads the shortcut.\n"
        "Lineage: The shortcut.\n"
        "Schema:\n"
        "  Value: string\n"
        '"""\n'
        "from shortcuts import ACQSC__HarmSurveyXlsx\n"
        "from weaver import Table\n\n\n"
        "class ACQSC__Consumer(Table):\n"
        "    def read(self):\n"
        "        return self.staging_table(ACQSC__HarmSurveyXlsx)\n",
    )
    return parse_item_repository(Location(str(root)))


def impact_of(estate, registered, *, physical_types):
    return determine_impact(
        estate,
        registered,
        selected={document_id(FOLDER), document_id(CONSUMER)},
        physical_types=physical_types,
    )


def as_a_native_folder(estate):
    """The Registry as the build before the transition left it.

    A Folder document was installed here, so the row carries the ``data`` role
    and the signature that document had. The declaration standing there now is a
    shortcut, which signs itself differently, so the identity is changed.
    """

    return {
        document_id(FOLDER): registered_document(
            FOLDER,
            object_type="folder",
            object_role="data",
            signature="what the native folder was signed by",
        )
    }


def as_a_shortcut(estate):
    """The Registry as a build of this repository leaves it."""

    identity = document_id(FOLDER)
    return {
        identity: registered_document(
            FOLDER,
            object_type="folder",
            object_role="shortcut",
            signature=declared_signatures(estate, {identity})[identity],
        )
    }


# --- graph membership is the whole rule ----------------------------------------


@weaver_test()
def test_a_physical_shortcut_destination_is_not_a_graph_node(estate):
    """The fact the rest of this module rests on, stated once."""

    assert FOLDER not in estate.dependency_graph
    assert CONSUMER in estate.dependency_graph


@weaver_test()
def test_a_changed_physical_shortcut_destination_does_not_walk_the_graph(estate):
    """
    Intent: A build over an estate transitioning a native folder to a folder
    shortcut at the same identity selects work rather than failing.

    Proof: the destination is classified as changed, and impact expansion asks
    the graph whether it holds the identity instead of assuming it does. A
    `GraphError` here is the defect this covers.
    """

    impact = impact_of(
        estate,
        as_a_native_folder(estate),
        physical_types={document_id(FOLDER): "folder"},
    )

    assert impact.changed == (document_id(FOLDER),)
    assert impact.impacted_descendants == ()


@weaver_test()
def test_the_changed_destination_is_still_selected(estate):
    """Ending the walk is not being skipped. The identity is its own root."""

    impact = impact_of(
        estate,
        as_a_native_folder(estate),
        physical_types={document_id(FOLDER): "folder"},
    )

    assert document_id(FOLDER) in impact.impacted


@weaver_test()
def test_a_consumer_of_a_physical_shortcut_is_not_impacted_by_it(estate):
    """Current graph semantics, asserted rather than inherited.

    A physical target may have no producer in this repository at all, so
    importing one records a physical dependency and no graph edge. The consumer
    is therefore reached by nothing when the destination changes, and is
    classified on its own signature.
    """

    registered = dict(as_a_native_folder(estate))
    identity = document_id(CONSUMER)
    registered[identity] = registered_document(
        CONSUMER, signature=declared_signatures(estate, {identity})[identity]
    )

    impact = impact_of(
        estate,
        registered,
        physical_types={document_id(FOLDER): "folder", identity: "table"},
    )

    assert impact.changed == (document_id(FOLDER),)
    assert identity not in impact.impacted


# --- reconciling the installed form to the declared one -------------------------


def planned(estate, *, registered, folders=("ACQSC.HarmSurveyXlsx",)):
    """One item's plan, with the destination selected for both drop and build."""

    item = item_id(ITEM)
    target = bound_target(id="landing", item_id="Landing_LH")
    destination = document_id(FOLDER)
    return plan_item_build(
        estate,
        item=item,
        target=target,
        inventory=target_inventory(
            target_id="landing",
            target_name="Landing_LH",
            folder_schemas=("ACQSC",),
            folders=folders,
        ),
        target_by_item={item: target},
        selected_documents=set(),
        selected_shortcuts={destination},
        selected_for_drop={destination},
        selected_for_build={destination},
        registered=registered,
        catalogue_target=catalogue_target(),
        shortcut_sources={
            f"{ITEM}/ACQSC__HarmSurveyXlsx": ResolvedShortcutSource(
                workspace_id="workspace-1",
                item_id="drop-lakehouse",
                item_name="Drop",
                path="Files/ACQSC/HarmSurveyXlsx",
            )
        },
    )


def kinds_of(plan):
    return [
        action.kind
        for stage in plan.stages
        for batch in stage.batches
        for action in batch.actions
    ]


@weaver_test()
def test_a_native_folder_is_dropped_before_the_shortcut_replaces_it(estate):
    """
    Intent: A build transitions an identity from a native Folder to a folder
    shortcut without a person clearing the destination first.

    Proof: the catalogue records the installed role as data while the repository
    declares a shortcut, so the managed drop removes what is installed and the
    shortcut is created after it. Fabric refuses a shortcut over an occupied
    name, so the order is the claim.
    """

    kinds = kinds_of(planned(estate, registered=as_a_native_folder(estate)))

    assert kinds.index("drop_folder") < kinds.index("create_shortcut")


@weaver_test()
def test_the_drop_is_the_ordinary_managed_drop(estate):
    """The transition adds no mechanism. It reaches the one that exists.

    A folder comes off through the folder executor, addressed by its identity,
    which is what a rebuild drop of a declared Folder already does.
    """

    plan = planned(estate, registered=as_a_native_folder(estate))
    dropped = [
        action
        for stage in plan.stages
        for batch in stage.batches
        for action in batch.actions
        if action.kind == "drop_folder"
    ]

    assert [action.executor for action in dropped] == ["folder"]
    assert [action.resource_node_id for action in dropped] == [FOLDER]


@weaver_test()
def test_a_destination_already_installed_as_a_shortcut_is_not_dropped(estate):
    """Convergence. Re-running the same desired state changes nothing but the pointer.

    Materialising a shortcut replaces the pointer that is there, so a destination
    already installed as one is never dropped first. A build that dropped it
    would destroy a working pointer for nothing, and on a folder shortcut the
    removal would reach the item it points at.
    """

    kinds = kinds_of(planned(estate, registered=as_a_shortcut(estate)))

    assert "drop_folder" not in kinds
    assert "drop_shortcut" not in kinds


@weaver_test()
def test_the_transition_converges_after_one_build(estate):
    """
    Intent: The build after the transitioning one does nothing.

    Proof: selection is given the Registry the transition leaves, a shortcut role
    and the declaration's own signature, and the folder still in the inventory.
    The destination is then neither new nor changed, so nothing selects it and the
    shortcut is not recreated.
    """

    selection = select_build(
        estate,
        as_a_shortcut(estate),
        selected={document_id(FOLDER)},
        inventories={
            item_id(ITEM): target_inventory(
                target_id="landing",
                target_name="Landing_LH",
                folder_schemas=("ACQSC",),
                folders=("ACQSC.HarmSurveyXlsx",),
            )
        },
    )

    assert selection.impact.new == ()
    assert selection.impact.changed == ()
    assert selection.selected_for_build == ()


@weaver_test()
def test_an_uncertified_occupant_is_left_where_it_stands(estate):
    """No Registry row means nothing here is Weaver's to remove.

    What stands at the destination was not certified by any build, so removing it
    is not this build's decision. Shortcut creation reports the occupied name.
    """

    kinds = kinds_of(planned(estate, registered={}))

    assert "drop_folder" not in kinds
    assert "create_shortcut" in kinds


@weaver_test()
def test_a_pointer_standing_where_a_document_is_declared_is_unpicked(tmp_path):
    """
    Intent: A build transitions an identity the other way, from a shortcut to a
    document the item declares itself.

    Proof: the catalogue records the installed role as shortcut, so the drop is a
    shortcut removal through the workspace API rather than a Spark `DROP TABLE`.
    A OneLake shortcut is a read-write window into the item it points at, so the
    ordinary drop would reach that item's data.
    """

    repository = single_document_repository(
        tmp_path, documents={"DWG__Portable.py": lakehouse_table("DWG.Portable")}
    )
    item = next(model.identity for model in repository.items)
    identity = document_id(f"{item}/DWG.Portable")
    target = bound_target(id="landing", item_id="Landing_LH")

    plan = plan_item_build(
        repository,
        item=item,
        target=target,
        inventory=target_inventory(
            target_id="landing",
            target_name="Landing_LH",
            schemas=("DWG",),
            tables=("DWG.Portable",),
        ),
        target_by_item={item: target},
        selected_documents={identity},
        selected_shortcuts=set(),
        selected_for_drop={identity},
        selected_for_build={identity},
        registered={
            identity: registered_document(
                identity,
                object_type="table",
                object_role="shortcut",
                signature="what the shortcut declaration was signed by",
            )
        },
        catalogue_target=catalogue_target(),
    )

    dropped = [
        action
        for stage in plan.stages
        for batch in stage.batches
        for action in batch.actions
        if action.kind in ("drop_shortcut", "drop_table")
    ]

    assert [action.kind for action in dropped] == ["drop_shortcut"]
    assert [action.executor for action in dropped] == ["shortcut"]
    payload = next(
        stage.payloads[dropped[0].payload]
        for stage in plan.stages
        if dropped[0].payload in stage.payloads
    )
    assert b'"path": "Tables/DWG"' in payload
    assert b'"name": "Portable"' in payload


# --- what prohibit_rebuild protects -------------------------------------------
#
# A Folder holding retained source data declares `Prohibit rebuild: true` so a
# repository rebuild cannot destroy what has landed. A pointer holds none of
# Weaver's data, so the flag has nothing to protect while a shortcut is still
# what stands at the identity, and the shortcut-to-owned transition needs the
# pointer replaced.


def _protected_folder_repository(tmp_path):
    """One item declaring a Folder that forbids its own rebuild."""

    root = tmp_path / "repo"
    _write(root, f"{ITEM}/schemas/ACQSC.yml", schema_document("ACQSC"))
    _write(
        root,
        f"{ITEM}/Files/ACQSC__HarmSurveyXlsx.py",
        '"""\n'
        "Folder ID: ACQSC.HarmSurveyXlsx\n"
        "Description: Retained source workbooks.\n"
        "Lineage: A source system.\n"
        'File key: "*.xlsx"\n'
        "Incremental: true\n"
        "Prohibit rebuild: true\n"
        '"""\n'
        "from weaver import Folder\n\n\n"
        "class ACQSC__HarmSurveyXlsx(Folder):\n"
        "    def read(self):\n"
        "        return None\n",
    )
    return parse_item_repository(Location(str(root)))


def _selection(estate, *, installed_role):
    from weaver.build_bundle.incremental import select_build

    identity = document_id(FOLDER)
    return select_build(
        estate,
        {
            identity: registered_document(
                FOLDER,
                object_type="folder",
                object_role=installed_role,
                signature="what stood here before",
            )
        },
        selected={identity},
        inventories={
            identity.item: target_inventory(
                folder_schemas=("ACQSC",), folders=("ACQSC.HarmSurveyXlsx",)
            )
        },
    )


@weaver_test()
def test_a_protected_folder_installed_as_itself_is_not_rebuilt(tmp_path):
    """The flag does what it is for: landed data is never dropped."""

    estate = _protected_folder_repository(tmp_path)
    selection = _selection(estate, installed_role="data")

    assert document_id(FOLDER) in selection.prohibited
    assert document_id(FOLDER) not in selection.selected_for_drop


@weaver_test()
def test_a_protected_folder_installed_as_a_pointer_is_still_replaced(tmp_path):
    """
    Intent: A migration declares the owned Folder while the temporary shortcut
    is still installed, and the build has to unpick the pointer.

    Proof: the flag protects landed data, and a pointer holds none of Weaver's.
    Read from the declaration alone the identity was prohibited, so the pointer
    stayed and the catalogue certified an owned Folder that was never built.
    """

    estate = _protected_folder_repository(tmp_path)
    selection = _selection(estate, installed_role="shortcut")

    assert document_id(FOLDER) not in selection.prohibited
    assert document_id(FOLDER) in selection.selected_for_drop
    assert document_id(FOLDER) in selection.selected_for_build


# --- what the correction must not have moved -----------------------------------


@weaver_test()
def test_a_logical_shortcut_still_carries_impact_to_its_consumer(tmp_path):
    """``source → logical shortcut destination → consumer``, still three hops.

    A logical destination names a Weaver document, so it is a node and the walk
    reaches through it. This is the propagation the graph-membership rule must
    leave exactly as it was.
    """

    repository = shortcut_repository(tmp_path / "repo")
    source = document_id("Lakehouse/Raw/DWG.Customer")
    destination = document_id("Lakehouse/Curated/DWG.PortableCustomer")
    view = document_id("Lakehouse/Curated/DWG.CustomerName")
    selected = {source, destination, view}
    declared = declared_signatures(repository, selected)

    impact = determine_impact(
        repository,
        {
            source: registered_document(source, signature="an earlier declaration"),
            destination: registered_document(
                destination, object_role="shortcut", signature=declared[destination]
            ),
            view: registered_document(
                view, object_type="view", signature=declared[view]
            ),
        },
        selected=selected,
        physical_types={source: "table", destination: "table", view: "view"},
    )

    assert impact.changed == (source,)
    assert set(impact.impacted_descendants) == {destination, view}


@weaver_test()
def test_a_changed_runtime_artefact_still_ends_the_walk(tmp_path):
    """The exclusion the graph-membership rule replaced, held to the same answer.

    A deployed module is signed by its own content and nothing declares against
    it, so its identity is not a node. This asserted the named exclusion before;
    it asserts graph membership now, and the answer is the same.
    """

    from weaver.etl import runtime_artefacts

    repository = single_document_repository(
        tmp_path, documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )
    item = next(model.identity for model in repository.items)
    table = document_id(f"{item}/DWG.Customer")
    artefact = next(
        each
        for each in runtime_artefacts(repository)
        if each.identity.item == item and each.origin == table
    )

    assert str(artefact.identity) not in repository.dependency_graph

    impact = determine_impact(
        repository,
        {
            artefact.identity: registered_document(
                artefact.identity,
                object_type="file",
                object_role="load",
                signature="what an earlier module was signed by",
            ),
            table: registered_document(
                table, signature=declared_signatures(repository, {table})[table]
            ),
        },
        selected={artefact.identity, table},
        physical_types={artefact.identity: "file", table: "table"},
    )

    assert impact.changed == (artefact.identity,)
    assert impact.impacted_descendants == ()


@weaver_test()
def test_a_same_item_native_dependency_still_carries_impact(tmp_path):
    """The ordinary case, kept in view beside the exceptions."""

    table = "DWG.Customer"
    view = "DWG.ActiveCustomer"
    repository = single_document_repository(
        tmp_path,
        documents={
            "DWG__Customer.py": lakehouse_table(table),
            "DWG.ActiveCustomer.sql": spark_view(view, depends_on=table),
        },
    )
    item = next(model.identity for model in repository.items)
    producer = document_id(f"{item}/{table}")
    consumer = document_id(f"{item}/{view}")
    declared = declared_signatures(repository, {producer, consumer})

    impact = determine_impact(
        repository,
        {
            producer: registered_document(producer, signature="an earlier declaration"),
            consumer: registered_document(
                consumer, object_type="view", signature=declared[consumer]
            ),
        },
        selected={producer, consumer},
        physical_types={producer: "table", consumer: "view"},
    )

    assert impact.changed == (producer,)
    assert impact.impacted_descendants == (consumer,)
