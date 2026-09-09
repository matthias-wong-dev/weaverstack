"""What a build does about an object whose data is borrowed.

A mirror is a pointer: it holds none of Weaver's data, so an ordinary build may
replace one with an owned object. The row saying it was borrowed goes last, once
the build that gives it its own rows has run.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.build_bundle.catalogue_actions import (
    DEREGISTER_MIRROR_SLUG,
    render_mirror_deregistration,
)
from weaver.catalogue.state import Catalogue, InstalledMirror
from weaver.catalogue.tables import MIRROR, REGISTRY, ROLE_DATA
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.installed import InstalledNode
from weaver.targets import PhysicalTargetRef

ITEM = WeaverItemId.parse("Warehouse/Model")
TARGET = PhysicalTargetRef(kind="Warehouse", name="Model_Dev")


def _id(name: str) -> WeaverDocumentId:
    return WeaverDocumentId(ITEM, ObjectId("Core", name))


def _mirror(name: str) -> InstalledMirror:
    return InstalledMirror(
        identity=_id(name),
        source_workspace="Analytics",
        source_target="PROD",
        source_schema="Core",
        source_object=name,
        physical_type="view",
    )


def _catalogue(*names: str) -> Catalogue:
    rows = {
        ITEM: {
            MIRROR.name: tuple(
                {
                    "item_type": ITEM.item_type,
                    "item_name": ITEM.item_name,
                    "schema_name": "Core",
                    "object_name": name,
                    "source_workspace_name": "Analytics",
                    "source_target_name": "PROD",
                    "source_schema_name": "Core",
                    "source_object_name": name,
                    "physical_type": "view",
                }
                for name in names
            ),
            REGISTRY.name: tuple(
                {
                    "item_type": ITEM.item_type,
                    "item_name": ITEM.item_name,
                    "schema_name": "Core",
                    "object_name": name,
                    "object_type": "table",
                    "object_role": ROLE_DATA,
                    "signature": "sig",
                }
                for name in names
            ),
        }
    }
    return Catalogue(rows=rows)


# --- what the catalogue says --------------------------------------------------


@weaver_test()
def test_registry_still_says_what_the_object_logically_is():
    """A mirror is an overlay. It does not change the type or the role."""

    catalogue = _catalogue("Customer")
    document = catalogue.registered[_id("Customer")]

    assert document.object_type == "table"
    assert document.object_role == ROLE_DATA
    assert catalogue.is_mirrored(_id("Customer"))


@weaver_test()
def test_what_stands_at_the_address_is_what_mirror_says():
    """Registry Table plus a borrowed View is valid, and not a mismatch."""

    catalogue = _catalogue("Customer")

    assert catalogue.effective_physical_type(_id("Customer")) == "view"


@weaver_test()
def test_an_object_that_is_not_borrowed_is_what_registry_says():
    catalogue = _catalogue()

    assert catalogue.mirrors == {}
    assert catalogue.effective_physical_type(_id("Customer")) is None


# --- what a run may write -----------------------------------------------------


@weaver_test()
def test_a_borrowed_node_is_not_loadable():
    """The rows belong to the target it borrows from."""

    node = InstalledNode(
        identity=_id("Customer"),
        target=TARGET,
        role=ROLE_DATA,
        object_type="table",
        artefact=_id("LoadCustomer"),
        artefact_type="stored_procedure",
        mirror=_mirror("Customer"),
    )

    assert node.is_installed
    assert node.is_mirrored
    assert not node.is_loadable


@weaver_test()
def test_the_same_node_holding_its_own_rows_is_loadable():
    node = InstalledNode(
        identity=_id("Customer"),
        target=TARGET,
        role=ROLE_DATA,
        object_type="table",
        artefact=_id("LoadCustomer"),
        artefact_type="stored_procedure",
    )

    assert node.is_loadable


@weaver_test()
def test_a_borrowed_node_is_addressed_as_what_stands_there():
    node = InstalledNode(
        identity=_id("Customer"),
        target=TARGET,
        role=ROLE_DATA,
        object_type="table",
        mirror=_mirror("Customer"),
    )

    assert node.effective_object_type == "view"
    assert node.physical.object_type == "view"


# --- when the row goes --------------------------------------------------------


@weaver_test()
def test_only_what_this_build_materialised_stops_being_borrowed():
    stage = render_mirror_deregistration(
        _catalogue("Customer", "Region"),
        [_id("Customer")],
        catalogue_target=_target(),
    )

    statement = _statements(stage)
    assert "N'Customer'" in statement
    assert "N'Region'" not in statement


@weaver_test()
def test_one_batch_for_the_whole_build():
    """A row removed per object would spread the transition across the work."""

    stage = render_mirror_deregistration(
        _catalogue("Customer", "Region"),
        [_id("Customer"), _id("Region")],
        catalogue_target=_target(),
    )

    assert stage.slug == DEREGISTER_MIRROR_SLUG
    assert len(stage.batches) == 1


@weaver_test()
def test_a_build_that_materialised_nothing_borrowed_writes_nothing():
    assert (
        render_mirror_deregistration(
            _catalogue("Customer"), [_id("Elsewhere")], catalogue_target=_target()
        )
        is None
    )
    assert (
        render_mirror_deregistration(
            _catalogue(), [_id("Customer")], catalogue_target=_target()
        )
        is None
    )


def _target():
    from weaver.build_bundle.targets import BoundTarget

    return BoundTarget(
        id="Warehouse-_weaver--warehouse-Weaver",
        kind="warehouse",
        item_id="Weaver",
        item_name="Weaver",
    )


def _statements(stage) -> str:
    import json

    ((_name, content),) = stage.payloads.items()
    return "\n".join(json.loads(content.decode("utf-8")))
