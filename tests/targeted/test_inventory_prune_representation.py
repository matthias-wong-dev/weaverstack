"""The prune diff itself: desired state plus actual inventory, into removals.

`test_prune.py` exercises this through `item_prune_stage`, which also does item
scoping, target binding and stage packaging. That is the right test for those
things and the wrong one for this: a defect in the diff and a defect in the
scoping fail it identically.

So the renderer is called directly here, with the keep-set handed in rather than
derived. Both sides are plain data, so what is being asserted is arithmetic on
two sets and the statements that fall out of it.

Prune is the destructive direction, so most of what is asserted is what it
*spares*.
"""

from __future__ import annotations

from factories import bound_target, target_inventory
from support.weaver_test import weaver_test

#: What `managed_sets` produces: the keep-set, folded for comparison. Built by
#: hand here so the diff is tested against a stated desired state rather than
#: against whatever a repository happened to declare.
from weaver.build_bundle.prune import _Managed, render_inventory_prune


def keep(
    *,
    tables=(),
    views=(),
    folders=(),
    schemas=(),
    folder_schemas=(),
    declared_objects=None,
):
    """The keep-set, with the document-declared names defaulting to all of them.

    ``declared_objects`` is stated only where the difference is the subject: an
    shortcut destination is in ``tables`` or ``views`` and *not* here, because no
    managed drop can remove one.
    """

    return _Managed(
        schemas=frozenset(schemas),
        folder_schemas=frozenset(folder_schemas),
        folders=frozenset(folders),
        tables=frozenset(tables),
        views=frozenset(views),
        declared_objects=frozenset(
            set(tables) | set(views) if declared_objects is None else declared_objects
        ),
    )


def prune(inventory, managed, *, target=None):
    payloads: dict[str, bytes] = {}
    actions, _changes = render_inventory_prune(
        target or bound_target(), inventory, managed, payloads
    )
    return actions, payloads


def kinds(actions):
    return sorted(action.kind for action in actions)


# --- what it spares -----------------------------------------------------------


@weaver_test()
def test_an_inventory_matching_the_keep_set_is_left_alone():
    actions, _ = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
        keep(schemas={"dwg"}, tables={"dwg.customer"}),
    )

    assert actions == ()


@weaver_test()
def test_an_empty_inventory_prunes_nothing_rather_than_everything():
    """Nothing there is nothing to remove — not everything to remove.

    The direction matters: computed the wrong way round, a fresh target would
    produce a removal for every declared object, against objects that do not
    exist.
    """

    actions, _ = prune(
        target_inventory(), keep(schemas={"dwg"}, tables={"dwg.customer"})
    )

    assert actions == ()


@weaver_test()
def test_an_object_declared_under_a_new_kind_is_spared_for_the_managed_drop():
    """A view the item now declares as a table is a kind change, not an orphan.

    The managed drop reads the installed type from the Registry and removes it
    strictly, so a prune of the same name would leave that drop with nothing to
    remove and fail the install.
    """

    actions, _ = prune(
        target_inventory(schemas=("DWG",), views=("DWG.Customer",)),
        keep(schemas={"dwg"}, tables={"dwg.customer"}),
    )

    assert actions == ()


@weaver_test()
def test_a_table_the_item_now_declares_as_a_view_is_spared_the_same_way():
    actions, _ = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
        keep(schemas={"dwg"}, views={"dwg.customer"}),
    )

    assert actions == ()


@weaver_test()
def test_the_comparison_folds_case():
    """The physical name's case is the workspace's to choose, so a keep-set that
    compared exactly would delete the very object it meant to spare."""

    actions, _ = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
        keep(schemas={"dwg"}, tables={"dwg.customer"}),
    )

    assert actions == ()


# --- what it removes ----------------------------------------------------------


@weaver_test()
def test_an_unmanaged_table_is_dropped():
    actions, payloads = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.OldTable",)),
        keep(schemas={"dwg"}),
    )

    assert kinds(actions) == ["prune_table"]
    assert payloads
    assert "DROP TABLE" in next(iter(payloads.values())).decode().upper()


@weaver_test()
def test_an_unmanaged_view_is_dropped():
    actions, _ = prune(
        target_inventory(schemas=("DWG",), views=("DWG.OldView",)),
        keep(schemas={"dwg"}),
    )

    assert kinds(actions) == ["prune_view"]


@weaver_test()
def test_an_unmanaged_folder_is_removed():
    actions, _ = prune(
        target_inventory(folder_schemas=("Raw",), folders=("Raw.Stale",)),
        keep(folder_schemas={"raw"}),
    )

    assert kinds(actions) == ["prune_folder"]


@weaver_test()
def test_an_unmanaged_schema_is_dropped():
    actions, _ = prune(target_inventory(schemas=("Legacy",)), keep())

    assert kinds(actions) == ["prune_schema"]


@weaver_test()
def test_a_schema_that_is_going_takes_its_contents_with_it():
    """One drop, not three.

    Dropping the schema removes what is in it, so emitting a drop per object as
    well would issue statements against objects the first one already took —
    which succeeds only because the drops are `IF EXISTS`, and reads as a build
    doing three times the work it needs to.
    """

    actions, _ = prune(
        target_inventory(
            schemas=("Legacy",),
            tables=("Legacy.Thing",),
            views=("Legacy.Report",),
        ),
        keep(),
    )

    assert kinds(actions) == ["prune_schema"]


@weaver_test()
def test_an_object_in_a_surviving_schema_is_dropped_individually():
    """The other side of the same rule: the schema stays, so the object must go
    by name."""

    actions, _ = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.OldTable",)),
        keep(schemas={"dwg"}),
    )

    assert kinds(actions) == ["prune_table"]


# --- the Warehouse side -------------------------------------------------------


def warehouse():
    return bound_target(kind="warehouse")


@weaver_test()
def test_a_warehouse_prune_is_t_sql():
    actions, payloads = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.OldTable",)),
        keep(schemas={"dwg"}),
        target=warehouse(),
    )

    (action,) = actions
    assert action.executor == "tsql"
    assert payloads[action.payload].decode().startswith("drop table if exists")


@weaver_test()
def test_an_shortcut_destination_installed_as_the_other_kind_is_still_dropped():
    """The limit of the kind-change rule, and why it names *documents*.

    A Warehouse shortcut is materialised by `create or alter view`, which cannot
    replace a table, and no managed drop covers a shortcut. So a table standing
    where the shortcut belongs is removed here or the install fails on it.
    """

    actions, _ = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.PortableCustomer",)),
        keep(schemas={"dwg"}, views={"dwg.portablecustomer"}, declared_objects=()),
        target=warehouse(),
    )

    assert kinds(actions) == ["prune_table"]


@weaver_test()
def test_a_warehouse_drops_every_orphan_by_name_including_in_a_doomed_schema():
    """The Warehouse side does *not* fold objects into their schema's drop.

    T-SQL will not drop a schema that still holds objects, so each has to go by
    name first — the opposite of the Lakehouse rule, and worth pinning because
    the two look interchangeable from a distance.
    """

    actions, _ = prune(
        target_inventory(
            schemas=("Legacy",),
            tables=("Legacy.Thing",),
            views=("Legacy.Report",),
        ),
        keep(),
        target=warehouse(),
    )

    assert kinds(actions) == ["prune_schema", "prune_table", "prune_view"]


@weaver_test()
def test_warehouse_identifiers_are_bracket_escaped():
    actions, payloads = prune(
        target_inventory(schemas=("Od]d",)), keep(), target=warehouse()
    )

    (action,) = actions
    assert payloads[action.payload].decode() == "drop schema if exists [Od]]d];\n"


# --- the frozen payloads ------------------------------------------------------


@weaver_test()
def test_every_action_carries_a_frozen_statement_and_its_hash():
    import hashlib

    actions, payloads = prune(
        target_inventory(schemas=("DWG",), tables=("DWG.OldTable",)),
        keep(schemas={"dwg"}),
    )

    for action in actions:
        if action.payload is None:  # a folder is removed, not executed
            continue
        content = payloads[action.payload]
        assert action.payload_sha256 == hashlib.sha256(content).hexdigest()


@weaver_test()
def test_rendering_the_same_diff_twice_is_identical():
    """A bundle's identity is its bytes, and both inputs here are sets."""

    inventory = target_inventory(
        schemas=("DWG", "Legacy"), tables=("DWG.OldTable", "DWG.Other")
    )
    managed = keep(schemas={"dwg"})

    first, first_payloads = prune(inventory, managed)
    second, second_payloads = prune(inventory, managed)

    assert [a.id for a in first] == [a.id for a in second]
    assert first_payloads == second_payloads
