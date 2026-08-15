"""Explicit unbind is target-directed and never inspects target existence."""

from __future__ import annotations

from weaver.catalogue.tables import INSTALLATION
from weaver.spark import FabricSparkTarget
from weaver.unbind import plan_unbind, unbind_targets

#: The Weaver Lakehouse every catalogue statement is addressed to.
WEAVER = FabricSparkTarget(workspace="Demo", lakehouse="Weaver")


class _Catalogue:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []
        self.destination = WEAVER

    def columns_of(self, _name):
        return tuple(INSTALLATION.physical_columns)

    def rows(self, _statement):
        return [dict(row) for row in self._rows]

    def sql(self, statement):
        self.executed.append(statement)


ROWS = (
    {
        "item_type": "Lakehouse",
        "item_name": "Sales",
        "target_name": "Sales_Dev",
        "weaver_version": "1",
        "signature": "a",
    },
    {
        "item_type": "Lakehouse",
        "item_name": "Inventory",
        "target_name": "Inventory_Dev",
        "weaver_version": "1",
        "signature": "b",
    },
)


def test_plan_unbind_selects_by_physical_target_and_orders_dependent_deletes():
    catalogue = _Catalogue(ROWS)
    result = plan_unbind(catalogue, lakehouses=("Sales_Dev",))

    assert result.targets == ("Lakehouse/Sales_Dev",)
    assert result.logical_items == ("Lakehouse/Sales",)
    assert "Registry" in result.statements[0]
    assert all("Inventory" not in statement for statement in result.statements)
    assert catalogue.executed == []


def test_unbind_executes_only_catalogue_dml_without_a_physical_target_client():
    catalogue = _Catalogue(ROWS)
    result = unbind_targets(catalogue, lakehouses=("Sales_Dev",))

    assert tuple(catalogue.executed) == result.statements
    assert len(catalogue.executed) > 2


# --- what an unbind costs -----------------------------------------------------
#
# A DELETE is a Delta transaction that rewrites files, and it costs seconds
# whether it removes one row or a thousand. So the statement *count* is the
# cost, and against a real Fabric workspace it was the largest single thing in
# the development loop: a wipe of the Weaver Example estate spent about 105
# seconds in one Livy call, almost all of it transaction overhead on thirty-odd
# tiny deletes.


def test_every_installation_is_removed_in_one_pass_over_the_tables():
    """One statement per table, not one per table per installation."""

    from weaver.catalogue.reconcile import prune_installation
    from weaver.catalogue.render import InstallationScope, InstallationScopes

    scopes = tuple(
        InstallationScope(item_type, name)
        for item_type, name in (
            ("Lakehouse", "Sales"),
            ("Warehouse", "Reporting"),
            ("Lakehouse", "_weaver"),
        )
    )

    one_at_a_time = sum(
        len(prune_installation(scope, destination=WEAVER)) for scope in scopes
    )
    together = prune_installation(InstallationScopes(scopes), destination=WEAVER)

    assert len(together) == one_at_a_time // len(scopes)
    assert len(together) == len(set(together)), "a table was addressed twice"


def test_the_combined_delete_names_every_installation_and_no_others():
    """The predicate is a *bounded* address. `WHERE` with no predicate — or one
    that reassociates — is every row in the catalogue."""

    from weaver.catalogue.reconcile import prune_installation
    from weaver.catalogue.render import InstallationScope, InstallationScopes

    scopes = InstallationScopes(
        (
            InstallationScope("Lakehouse", "Sales"),
            InstallationScope("Warehouse", "Reporting"),
        )
    )

    for statement in prune_installation(scopes, destination=WEAVER):
        where = statement.split("WHERE", 1)[1]
        assert "'Sales'" in where and "'Reporting'" in where
        # Outer parentheses around the OR: without them a later `AND` would
        # reassociate and the delete would widen.
        assert where.strip().startswith("((")


def test_removing_nothing_renders_nothing():
    """An empty selection must not become a `DELETE` with no predicate."""

    from weaver.unbind import plan_unbind

    result = plan_unbind(_Catalogue(()), lakehouses=(), warehouses=())

    assert result.statements == ()
