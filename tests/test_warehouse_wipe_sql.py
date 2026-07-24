"""The Warehouse wipe is inspectable SQL ported from legacy Weaver."""

from __future__ import annotations

from weaver import generate_warehouse_wipe_sql


def test_the_wipe_sql_is_deterministic_and_carries_the_proven_object_scope():
    first = generate_warehouse_wipe_sql()
    second = generate_warehouse_wipe_sql()
    lowered = first.lower()

    assert first == second
    for object_type in (
        "drop constraint",
        "drop view",
        "drop procedure",
        "drop function",
        "drop table",
        "drop schema",
    ):
        assert object_type in lowered


def test_objects_are_removed_before_schemas_in_dependency_safe_broad_order():
    sql = generate_warehouse_wipe_sql().lower()

    assert sql.index("drop constraint") < sql.index("drop view")
    assert sql.index("drop view") < sql.index("drop procedure")
    assert sql.index("drop procedure") < sql.index("drop function")
    assert sql.index("drop function") < sql.index("drop table")
    assert sql.index("drop table") < sql.index("drop schema")


def test_system_schemas_are_excluded_and_no_selected_warehouse_is_interpolated():
    sql = generate_warehouse_wipe_sql().lower()

    for schema in ("dbo", "guest", "information_schema", "sys", "queryinsights"):
        assert f"n'{schema}'" in sql
    assert "reporting_wh" not in sql
