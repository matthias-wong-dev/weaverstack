"""Mirroring one Warehouse item, against a real workspace.

The claim: ``weaver mirror --item Warehouse/X=Warehouse/Y`` empties Y, stands a
View over each of X's data relations in it, records them in ``_.Mirror``, and
binds the item to Y. The data stays where it was.

A tenant is needed because the reading-through is Fabric's: whether a
three-part View resolves across Warehouses, and whether a row written at the
source is visible through it. What the mirror decides is settled in
``tests/test_mirror_boundary.py`` and ``tests/test_borrow_declaration.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from support.build_envs import WAREHOUSE_ESTATE_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import CATALOGUE_SCHEMA, MIRROR
from weaver.targets import ItemRef, WarehouseTarget

ITEM = "Warehouse/Reporting"

#: A row the source holds and the mirror has no copy of.
SENTINEL = (99, "Borrowed")

_OBJECTS = (
    "select schema_name(o.schema_id) as s, o.name as n, o.type as t "
    "from sys.objects as o "
    "where o.is_ms_shipped = 0 and o.type in (N'U', N'V', N'P') "
    "order by s, o.name"
)


@dataclass(frozen=True)
class Mirrored:
    """One mirror, and a way to read either side of it."""

    result: Any
    source_sql: Any
    target_sql: Any
    catalogue_sql: Any
    source_name: str
    target_name: str
    catalogue_name: str


@pytest.fixture(scope="module")
def mirrored(
    fabric_workspace,
    fabric_catalogue,
    fabric_fork_catalogue,
    fabric_mirror_warehouse,
    session_disposable_warehouse,
    warehouse_session,
    tmp_path_factory,
):
    """A built source estate, forked, then one item mirrored into its own target."""

    register_session(warehouse_session)
    source_name = session_disposable_warehouse.item.name
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("mirror"))
    built = weaver.build(
        str(estate.path),
        items=[f"{ITEM}=Warehouse/{source_name}"],
        session=warehouse_session,
    )
    assert built.succeeded, [failure.describe() for failure in built.errors]

    source_sql = _sql(warehouse_session, fabric_workspace, source_name)
    _seed(source_sql)

    result = weaver.mirror(
        [f"{ITEM}=Warehouse/{fabric_mirror_warehouse.name}"],
        session=warehouse_session,
        workspace=fabric_workspace.workspace,
        catalogue=f"Warehouse/{fabric_fork_catalogue.name}",
        mirror=f"Warehouse/{fabric_catalogue.name}",
    )
    return Mirrored(
        result=result,
        source_sql=source_sql,
        target_sql=_sql(
            warehouse_session, fabric_workspace, fabric_mirror_warehouse.name
        ),
        catalogue_sql=_sql(
            warehouse_session, fabric_workspace, fabric_fork_catalogue.name
        ),
        source_name=source_name,
        target_name=fabric_mirror_warehouse.name,
        catalogue_name=fabric_fork_catalogue.name,
    )


def _sql(session, workspace, name: str):
    return session.sql_executor(WarehouseTarget(ItemRef(name)), workspace=workspace)


def _seed(sql) -> None:
    """One row at the source, so reading through the mirror proves something."""

    sql.execute("delete from [Wh].[Product];")
    sql.execute(
        "insert into [Wh].[Product] ([ProductId], [ProductName], [Row signature], "
        "[Row insert datetime], [Row update datetime], [Row delete datetime]) "
        f"values ({SENTINEL[0]}, N'{SENTINEL[1]}', "
        f"hashbytes('SHA2_256', N'{SENTINEL[1]}'), sysdatetime(), sysdatetime(), "
        "convert(datetime2(6), '9999-12-31 23:59:59.999999'));"
    )


def _objects(sql) -> dict[str, str]:
    return {
        f"{row['s']}.{row['n']}": str(row["t"]).strip() for row in sql.query(_OBJECTS)
    }


@weaver_test(remote=True, resources={"tds"})
def test_every_data_relation_becomes_a_view(mirrored):
    """A table and a view both read the same way through one."""

    found = _objects(mirrored.target_sql)
    borrowed = {name: kind for name, kind in found.items() if "." in name}

    assert borrowed["Wh.Product"] == "V"
    assert borrowed["Wh.Customer"] == "V"
    # The source's own view is a view here too, over the source's view.
    assert borrowed["Rpt.CustomerSummary"] == "V"
    assert not [name for name, kind in borrowed.items() if kind == "U"], (
        "a mirrored Warehouse holds no table of its own"
    )


@weaver_test(remote=True, resources={"tds"})
def test_the_rows_stay_where_they_were(mirrored):
    """Zero copy: what the source holds is what the mirror reads."""

    seen = mirrored.target_sql.query(
        "select [ProductId], [ProductName] from [Wh].[Product]"
    )

    assert [(int(row["ProductId"]), str(row["ProductName"])) for row in seen] == [
        SENTINEL
    ]


@weaver_test(remote=True, resources={"tds"})
def test_a_row_written_at_the_source_is_seen_through_the_mirror(mirrored):
    """Nothing was copied, so the source is still the only place the rows are."""

    mirrored.source_sql.execute(
        "update [Wh].[Product] set [ProductName] = N'Changed' "
        f"where [ProductId] = {SENTINEL[0]};"
    )

    seen = mirrored.target_sql.query(
        f"select [ProductName] from [Wh].[Product] where [ProductId] = {SENTINEL[0]}"
    )

    assert [str(row["ProductName"]) for row in seen] == ["Changed"]


@weaver_test(remote=True, resources={"tds"})
def test_the_catalogue_records_what_is_borrowed(mirrored):
    rows = mirrored.catalogue_sql.query(
        "select [Schema name], [Object name], [Source target name], "
        f"[Physical type] from {_q(MIRROR.name)} order by [Schema name], [Object name]"
    )

    assert {str(row["Object name"]) for row in rows} == {
        "Customer",
        "CustomerDim",
        "CustomerOrder",
        "Product",
        "CustomerSummary",
    }
    assert {str(row["Source target name"]) for row in rows} == {mirrored.source_name}
    assert {str(row["Physical type"]) for row in rows} == {"View"}


@weaver_test(remote=True, resources={"tds"})
def test_the_catalogues_own_tables_are_never_borrowed(mirrored):
    """``_.*`` is Weaver's own state, so a mirror gives the target real views
    over the catalogue rather than recording them as borrowed."""

    rows = mirrored.catalogue_sql.query(f"select [Schema name] from {_q(MIRROR.name)}")

    assert CATALOGUE_SCHEMA not in {str(row["Schema name"]) for row in rows}
    # And the target has the ordinary surface a build would have given it.
    found = _objects(mirrored.target_sql)
    assert f"{CATALOGUE_SCHEMA}.Installation" in found
    assert f"{CATALOGUE_SCHEMA}.{MIRROR.name}" not in found


@weaver_test(remote=True, resources={"tds"})
def test_the_item_is_bound_to_its_new_target(mirrored):
    """Installation moves last, once the Views are there."""

    rows = mirrored.catalogue_sql.query(
        "select [Item name], [Target name] from [_].[Installation] order by [Item name]"
    )
    bound = {str(row["Item name"]): str(row["Target name"]) for row in rows}

    assert bound["Reporting"] == mirrored.target_name
    # The catalogue still says its own tables are where they are.
    assert bound["_weaver"] != mirrored.target_name


@weaver_test(remote=True, resources={"tds", "rest"})
def test_a_borrowed_object_is_not_loadable(
    mirrored, fabric_workspace, warehouse_session
):
    """The rows belong to the target it borrows from, so Weaver does not write
    them: a load into a mirrored relation would write through to the source."""

    from dataclasses import replace

    from weaver.catalogue.state import catalogue_for

    forked = replace(fabric_workspace, catalogue=f"Warehouse/{mirrored.catalogue_name}")
    with catalogue_for(warehouse_session, forked) as catalogue:
        dag = catalogue.dag()

    borrowed = [node for node in dag.nodes if node.is_mirrored]
    assert borrowed, "the mirror recorded nothing"
    assert not any(node.is_loadable for node in borrowed)
    # Still real nodes: readable, and something a test can assert against.
    assert all(node.object_type for node in borrowed)


@weaver_test(remote=True, resources={"tds"})
def test_mirroring_again_converges(
    mirrored, warehouse_session, fabric_workspace, fabric_catalogue
):
    """A mirror is reconstruction, so a second one leaves the same estate."""

    before = _objects(mirrored.target_sql)

    weaver.mirror(
        [f"{ITEM}=Warehouse/{mirrored.target_name}"],
        session=warehouse_session,
        workspace=fabric_workspace.workspace,
        catalogue=f"Warehouse/{mirrored.catalogue_name}",
        mirror=f"Warehouse/{fabric_catalogue.name}",
    )

    assert _objects(mirrored.target_sql) == before
    rows = mirrored.catalogue_sql.query(f"select count(*) as n from {_q(MIRROR.name)}")
    assert int(rows[0]["n"]) == len(before) - _SURFACE


def _q(name: str) -> str:
    return f"[{CATALOGUE_SCHEMA}].[{name}]"


#: How many views the standard ``_`` surface adds to a mirrored Warehouse.
_SURFACE = 6
