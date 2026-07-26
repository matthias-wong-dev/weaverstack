"""Warehouse reconciliation — frozen T-SQL drops, planned from the catalogue.

Prune is part of the build (build-philosophy §5): the planner inspects the
Warehouse catalogue *now* and compiles each unmanaged table, view and schema into
an explicit drop, so a reviewer sees exactly what an install will remove and the
installer enumerates nothing.

Order matters more than on the Lakehouse: T-SQL has no ``DROP SCHEMA … CASCADE``,
so views go before the tables they read, and a schema only once it is empty.
The catalogue here is a fake executor, so these run locally; the behavioural
counterpart runs against a real Warehouse in ``tests/fabric``.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, Location, RepositoryRef
from weaver.build_bundle import (
    TargetBindings,
    WarehouseBinding,
    generate_build_bundle,
)
from weaver.errors import BuildError

FIXTURE = Path(__file__).parent / "fixtures" / "warehouse-estate"

#: What the fixture manages: Wh.Customer/Product/CustomerOrder/CustomerDim and
#: the Rpt.CustomerSummary view. Everything else below is an orphan.
ORPHANS = [
    {"schema_name": "Wh", "object_name": "OldTable", "object_type": "U "},
    {"schema_name": "Rpt", "object_name": "OldView", "object_type": "V "},
    {"schema_name": "Legacy", "object_name": "Thing", "object_type": "U "},
    {"schema_name": "Legacy", "object_name": "ThingView", "object_type": "V "},
]
MANAGED = [
    {"schema_name": "Wh", "object_name": "Customer", "object_type": "U "},
    {"schema_name": "Rpt", "object_name": "CustomerSummary", "object_type": "V "},
]
RESERVED = [{"schema_name": "dbo", "object_name": "Leave", "object_type": "U "}]


#: Every Fabric Warehouse carries a schema per fixed database role. They are not
#: Weaver's to drop, and not anyone's — `DROP SCHEMA` on one fails.
FIXED_ROLE_SCHEMAS = (
    "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin",
    "db_backupoperator", "db_datareader", "db_datawriter",
    "db_denydatareader", "db_denydatawriter",
)


class FakeSql:
    """A Warehouse catalogue, as the planner reads it.

    The schema query models the server rather than returning a flat list: the
    fixed-role schemas are always present, and are filtered out *by the query* if
    it asks the server to. A fake that answered every schema query identically
    could not tell a planner that excludes them from one that does not.
    """

    def __init__(self, objects, schemas):
        self.objects = objects
        self.schemas = schemas

    def query(self, statement: str):
        if "sys.schemas" in statement and "sys.objects" not in statement:
            names = list(self.schemas)
            if "is_fixed_role" not in statement:
                names += list(FIXED_ROLE_SCHEMAS)
            return [{"name": name} for name in names]
        return list(self.objects)


@pytest.fixture
def estate(tmp_path):
    host = LocalHost(root=tmp_path, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(host)
    store.make_directory(resolver.files_root(ItemRef("Weaver")))
    store.make_directory(resolver.tables_root(ItemRef("Weaver")))
    store.make_directory(resolver.repos_root)
    shutil.copytree(FIXTURE, resolver.repository(RepositoryRef("WhEstate")).path)
    return host, store, tmp_path


def _generate(estate, sql, *, prune=True):
    host, store, tmp_path = estate
    return generate_build_bundle(
        weaver_lakehouse=ItemRef("Weaver"),
        repository_name="WhEstate",
        targets=TargetBindings(warehouse=WarehouseBinding(warehouse=ItemRef("Play_WH"))),
        output=Location(str(tmp_path / "bundle")),
        host=host,
        store=store,
        prune=prune,
        sql=sql,
    )


def _prune_scripts(estate, bundle):
    """The frozen prune statements, in the order the installer will run them."""

    store = estate[1]
    return [
        (action.kind, store.read(bundle.location.join(*action.payload.split("/"))).decode().strip())
        for _, _, action in bundle.plan.actions()
        if action.kind.startswith("prune")
    ]


def test_unmanaged_tables_views_and_schemas_are_dropped(estate):
    sql = FakeSql(ORPHANS + MANAGED + RESERVED, ["Wh", "Rpt", "Legacy", "dbo", "sys"])
    scripts = _prune_scripts(estate, _generate(estate, sql))
    statements = [statement for _, statement in scripts]

    assert "drop view if exists [Rpt].[OldView];" in statements
    assert "drop view if exists [Legacy].[ThingView];" in statements
    assert "drop table if exists [Wh].[OldTable];" in statements
    assert "drop table if exists [Legacy].[Thing];" in statements
    assert "drop schema if exists [Legacy];" in statements
    # Every drop runs through the T-SQL executor.
    assert {kind for kind, _ in scripts} == {"prune_view", "prune_table", "prune_schema"}


def test_managed_and_reserved_objects_are_never_dropped(estate):
    sql = FakeSql(ORPHANS + MANAGED + RESERVED, ["Wh", "Rpt", "Legacy", "dbo", "sys"])
    statements = " ".join(s for _, s in _prune_scripts(estate, _generate(estate, sql)))

    # The bundle's own objects survive.
    assert "[Customer]" not in statements
    assert "[CustomerSummary]" not in statements
    # A schema the bundle manages survives, and system schemas are never touched.
    assert "[Wh]);" not in statements and "drop schema if exists [Wh]" not in statements
    assert "drop schema if exists [Rpt]" not in statements
    assert "[dbo]" not in statements and "[sys]" not in statements


def test_views_are_dropped_before_tables_and_schemas_last(estate):
    sql = FakeSql(ORPHANS + MANAGED, ["Wh", "Rpt", "Legacy"])
    kinds = [kind for kind, _ in _prune_scripts(estate, _generate(estate, sql))]

    # T-SQL has no DROP SCHEMA CASCADE, so the order has to be dependency-safe.
    assert kinds == sorted(
        kinds, key=lambda k: ("prune_view", "prune_table", "prune_schema").index(k)
    )
    assert kinds[0] == "prune_view"
    assert kinds[-1] == "prune_schema"


def test_a_clean_warehouse_needs_no_prune_sequence(estate):
    sql = FakeSql(MANAGED, ["Wh", "Rpt"])
    assert _prune_scripts(estate, _generate(estate, sql)) == []


def test_pruning_off_a_fabric_session_fails_closed(estate):
    """Reading the target is Fabric-native by default, like wipe_sql_target.

    Off a Fabric session there is no session identity to read the catalogue with,
    so generation raises rather than emitting drops from an inventory nobody
    could read. A desktop caller injects ``desktop_sql_executor`` explicitly.
    """

    from weaver.errors import CommandError

    with pytest.raises((CommandError, BuildError)):
        _generate(estate, None)


def test_prune_false_skips_reconciliation_without_a_catalogue(estate):
    bundle = _generate(estate, None, prune=False)
    assert _prune_scripts(estate, bundle) == []


def test_a_fixed_role_schema_is_never_dropped(estate):
    """Every Fabric Warehouse has nine of them, and none can be dropped.

    Excluded by *ownership*, asked of the server, rather than by adding nine more
    names to Weaver's reserved list — the reserved list says what Weaver declines
    to manage, and this says what SQL will not let anyone touch.
    """

    sql = FakeSql(ORPHANS + MANAGED, ["Wh", "Rpt", "Legacy"])
    statements = " ".join(s for _, s in _prune_scripts(estate, _generate(estate, sql)))

    # The one genuine orphan still goes.
    assert "drop schema if exists [Legacy];" in statements
    for name in FIXED_ROLE_SCHEMAS:
        assert f"[{name}]" not in statements
