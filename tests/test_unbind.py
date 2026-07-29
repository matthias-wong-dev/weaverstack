"""Explicit unbind is target-directed and never inspects target existence."""

from __future__ import annotations

from weaver.catalogue.tables import INSTALLATION
from weaver.unbind import plan_unbind, unbind_targets


class _Row(dict):
    def asDict(self):
        return dict(self)


class _Frame:
    columns = list(INSTALLATION.column_names)


class _Query:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return [_Row(row) for row in self._rows]


class _Spark:
    def __init__(self, rows):
        self.rows = rows

    def table(self, _name):
        return _Frame()

    def sql(self, _statement):
        return _Query(self.rows)


class _Catalogue:
    def __init__(self, rows):
        self.spark = _Spark(rows)
        self.executed = []

    def expand(self, name):
        return name

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

    assert result.targets == ("Lakehouses/Sales_Dev",)
    assert result.logical_items == ("Lakehouse/Sales",)
    assert "Registry" in result.statements[0]
    assert all("Inventory" not in statement for statement in result.statements)
    assert catalogue.executed == []


def test_unbind_executes_only_catalogue_dml_without_a_physical_target_client():
    catalogue = _Catalogue(ROWS)
    result = unbind_targets(catalogue, lakehouses=("Sales_Dev",))

    assert tuple(catalogue.executed) == result.statements
    assert len(catalogue.executed) > 2
