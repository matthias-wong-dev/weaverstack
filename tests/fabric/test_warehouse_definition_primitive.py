"""The physical Warehouse shape produced from a Weaver declaration."""

from __future__ import annotations

from support.weaver_test import weaver_test

AUDIT = {"row insert datetime", "row update datetime", "row delete datetime"}


@weaver_test(remote=True)
def test_a_built_table_uses_the_declared_types(warehouse_primitive_estate):
    columns = warehouse_primitive_estate.columns("DWG", "Customer")

    assert columns["customerid"]["type_name"] == "int"
    assert columns["customername"]["type_name"] == "varchar"
    assert columns["score"]["type_name"] == "decimal"


@weaver_test(remote=True)
def test_a_built_table_makes_the_primary_key_not_nullable(
    warehouse_primitive_estate,
):
    columns = warehouse_primitive_estate.columns("DWG", "Customer")

    assert columns["customerid"]["is_nullable"] is False
    for audit in AUDIT:
        assert columns[audit]["is_nullable"] is False, audit


@weaver_test(remote=True)
def test_a_built_table_adds_the_declared_identity_column(
    warehouse_primitive_estate,
):
    columns = warehouse_primitive_estate.columns("DWG", "CustomerDim")

    assert columns["customerkey"]["type_name"] == "bigint"
    assert bool(columns["customerkey"]["is_identity"]) is True
    assert {"customerid", "customername"} <= set(columns)
