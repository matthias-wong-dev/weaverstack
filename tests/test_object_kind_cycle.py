"""One object's kind changing, and changing back, generation by generation.

A view becomes a table under the same name, and then becomes a view again. Each
generation plans against the catalogue and the inventory the previous one left,
so what a build renders for a kind change is asserted rather than assumed.
"""

from __future__ import annotations

import json
from pathlib import Path

from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    WarehouseBinding,
    generate_item_build_bundle,
)
from weaver.build_bundle.models import (
    BUILD_TABLE,
    BUILD_VIEW,
    DROP_TABLE,
    DROP_VIEW,
    PRUNE_TABLE,
    PRUNE_VIEW,
)
from weaver.build_bundle.prune import TargetInventory
from weaver.catalogue.projection import project_item_catalogue
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.catalogue.tables import REGISTRY
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import TABLE, VIEW
from weaver.declaration.model import WeaverItemId
from weaver.etl import item_runtime_artefacts
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

ITEM = WeaverItemId.parse("Lakehouse/Sales")
SUMMARY = "Lakehouse/Sales/Sales.Summary"
TARGET = "Sales_LH"
#: The four-part name every generated statement spells the object with.
QUALIFIED = f"`{WORKSPACE}`.`{TARGET}`.`Sales`.`Summary`"

_SCHEMA = "Schema ID: Sales\nDescription: Sales objects.\n"

_ORDER = '''"""
Table ID: Sales.Order

Description: One row per order.

Lineage: A source system.

Primary key: Id

Schema:
  Id: string
  Amount: decimal(18,2)
"""
from weaver import Table


class Sales__Order(Table):
    def read(self):
        return [], []
'''

#: The same body under two headers: only the kind changes across a generation.
_BODY = "select Id, Amount from Sales.Order"

_AS_VIEW = f"""/*
View ID: Sales.Summary

Description: Order amounts.

Lineage: $Sales.Order

Dependencies:
  - Sales.Order
*/
{_BODY}
"""

_AS_TABLE = f"""/*
Table ID: Sales.Summary

Description: Order amounts.

Lineage: $Sales.Order

Primary key: Id

Dependencies:
  - Sales.Order

Schema:
  Id: string
  Amount: decimal(18,2)
*/
{_BODY};
"""


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _estate(tmp_path: Path, summary: str) -> Path:
    """A one-item Lakehouse whose Sales.Summary is declared as ``summary``."""

    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Sales/schemas/Sales.yml", _SCHEMA)
    _write(root, "Lakehouse/Sales/Sales__Order.py", _ORDER)
    _write(root, "Lakehouse/Sales/Sales.Summary.sql", summary)
    return root


def _repository(root: Path):
    return parse_item_repository(Location(str(root)))


def _bindings() -> ItemBindings:
    return ItemBindings(
        (
            ItemBinding(
                ITEM, LakehouseBinding(ItemRef(TARGET), workspace_name=WORKSPACE)
            ),
        )
    )


def _target():
    return _bindings().by_item[ITEM].to_bound_target()


def _empty_state():
    """A workspace where nothing has been built yet."""

    target = _target()
    return Catalogue({}), {
        ITEM: TargetInventory(
            target_id=target.id, kind=target.kind, target_name=target.name
        )
    }


def _installed_state(repository):
    """The catalogue and the inventory a completed build of this source leaves.

    Projected from the declaration and then reconciled against the inventory, as
    a build reads them: the Registry says what each object is, and the inventory
    is where it says so.
    """

    target = _target()
    artefacts = item_runtime_artefacts(repository, item=ITEM)
    documents = [
        (identity, repository.source_documents[identity])
        for identity in repository.source_documents
        if identity.item == ITEM
    ]
    retained = [identity for identity, _source in documents]
    retained.extend(artefact.identity for artefact in artefacts)
    inventory = TargetInventory(
        target_id=target.id,
        kind=target.kind,
        target_name=target.name,
        schemas=("Sales",),
        tables=tuple(
            source.qualified
            for identity, source in documents
            if not identity.is_files and source.kind == TABLE
        ),
        views=tuple(
            source.qualified
            for identity, source in documents
            if not identity.is_files and source.kind == VIEW
        ),
        files=tuple(artefact.target_path for artefact in artefacts if artefact.is_file),
        procedures=tuple(
            artefact.identity.object_id.qualified
            for artefact in artefacts
            if not artefact.is_file
        ),
    )
    catalogue = Catalogue(
        {ITEM: project_item_catalogue(repository, item=ITEM, retained=retained).rows},
        materialised=frozenset({REGISTRY.name}),
    )
    reconciled = reconcile_catalogue_state(catalogue, inventories={ITEM: inventory})
    return reconciled.catalogue, {ITEM: inventory}


def _generate(tmp_path: Path, repository, *, state, name: str):
    catalogue, inventories = state
    return generate_item_build_bundle(
        repository,
        bindings=_bindings(),
        output=Location(str(tmp_path / name)),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=catalogue,
        catalogue_binding=WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE),
    )


def _rendered(bundle) -> tuple[tuple[str, str | None, str | None], ...]:
    """Every action in install order, with the code the installer would run."""

    store = FilesystemStore()
    rendered = []
    for _sequence, _batch, action in bundle.plan.actions():
        payload = None
        if action.payload is not None:
            payload = store.read(
                bundle.location.join(*action.payload.split("/"))
            ).decode()
        rendered.append((action.kind, action.resource_node_id, payload))
    return tuple(rendered)


def _summary_code(bundle, kind: str) -> str:
    """The one payload this bundle renders for Sales.Summary under ``kind``."""

    matched = [
        payload
        for action_kind, node, payload in _rendered(bundle)
        if action_kind == kind and node == SUMMARY
    ]
    assert len(matched) == 1, f"{kind} for {SUMMARY}: {len(matched)} actions"
    return matched[0]


def _kinds(bundle) -> tuple[str, ...]:
    """The physical object kinds this bundle acts on, in install order."""

    physical = {BUILD_TABLE, BUILD_VIEW, DROP_TABLE, DROP_VIEW, PRUNE_TABLE, PRUNE_VIEW}
    return tuple(
        kind for kind, _node, _payload in _rendered(bundle) if kind in physical
    )


def _cycle(tmp_path: Path):
    """The three generations: build the view, replace it with a table, restore it.

    Returned together because each is planned against the state the previous one
    left, which is the claim these tests are making.
    """

    root = _estate(tmp_path, _AS_VIEW)
    first_source = _repository(root)
    first = _generate(tmp_path, first_source, state=_empty_state(), name="first")

    _write(root, "Lakehouse/Sales/Sales.Summary.sql", _AS_TABLE)
    second_source = _repository(root)
    second = _generate(
        tmp_path, second_source, state=_installed_state(first_source), name="second"
    )

    _write(root, "Lakehouse/Sales/Sales.Summary.sql", _AS_VIEW)
    third = _generate(
        tmp_path,
        _repository(root),
        state=_installed_state(second_source),
        name="third",
    )
    return first, second, third


@weaver_test()
def test_a_first_build_creates_the_view_and_drops_nothing(tmp_path):
    first, _second, _third = _cycle(tmp_path)

    assert _kinds(first) == (BUILD_TABLE, BUILD_VIEW)
    assert _summary_code(first, BUILD_VIEW).startswith(f"CREATE VIEW {QUALIFIED} AS\n")


@weaver_test()
def test_a_view_becoming_a_table_drops_a_view_and_builds_a_table(tmp_path):
    """The installed kind decides the drop, and the declared kind the build.

    One drop, not two: the item still declares the name, so prune leaves it to
    the managed drop, which removes it strictly by its registered type.
    """

    _first, second, _third = _cycle(tmp_path)

    assert _kinds(second) == (DROP_VIEW, BUILD_TABLE)
    assert _summary_code(second, DROP_VIEW) == f"DROP VIEW {QUALIFIED}\n"
    payload = json.loads(_summary_code(second, BUILD_TABLE))
    assert payload["object"] == QUALIFIED
    assert payload["schema_mode"] == "declared"


@weaver_test()
def test_a_table_becoming_a_view_drops_a_table_and_builds_a_view(tmp_path):
    """And back again: the drop follows the table the previous generation left."""

    _first, _second, third = _cycle(tmp_path)

    assert _kinds(third) == (DROP_TABLE, BUILD_VIEW)
    assert _summary_code(third, DROP_TABLE) == f"DROP TABLE {QUALIFIED}\n"
    assert _summary_code(third, BUILD_VIEW).startswith(f"CREATE VIEW {QUALIFIED} AS\n")


@weaver_test()
def test_the_round_trip_restores_the_code_the_first_build_generated(tmp_path):
    """A view, a table and the same view again render the same view a second time."""

    first, _second, third = _cycle(tmp_path)

    assert _summary_code(third, BUILD_VIEW) == _summary_code(first, BUILD_VIEW)
