"""Trial this Weaver version against an estate a previous release installed.

The previous release, installed from PyPI into a venv of its own, builds, loads
and tests a Warehouse item and a Lakehouse item into a catalogue of its own.
This version then mirrors that catalogue, first alone and then with both items,
builds the mirrored items, changes a declaration, builds, loads and tests. The
previous release's catalogue is never upgraded, and is read back unchanged at
the end. A damaged source is refused before the destination is touched.

Spark for the previous release runs that release too: its own Livy session
imports the released wheel, staged the way the suite stages a checkout. That
session needs the capacity's slot before the suite's shared session takes it,
so the installation is its own exclusive test.

Steps run in file order and do not cascade.
"""

from __future__ import annotations

import json
import os
import subprocess
import venv
from dataclasses import dataclass, replace
from pathlib import Path

import pytest
from mirror_support import forked_config
from support.acceptance import Acceptance
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE, WAREHOUSE_ESTATE_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import CATALOGUE_TABLES, SEMANTIC_TABLES
from weaver.targets import ItemRef, WarehouseTarget

#: The release this version upgrades from.
PREVIOUS_RELEASE = os.environ.get("WEAVER_PYTEST_PREVIOUS_RELEASE", "0.9.0")

WAREHOUSE_ITEM = "Warehouse/Reporting"
LAKEHOUSE_ITEM = "Lakehouse/Sales"

#: Each item's changed declaration: ``(path in the item, before, after)``.
CHANGES = (
    (WAREHOUSE_ITEM, "Wh.Product.sql", "'Widget'", "'Widget II'"),
    (
        LAKEHOUSE_ITEM,
        "Tables/DWG.NamedCustomer.sql",
        "where CustomerName is not null;",
        "where CustomerName is not null and CustomerId >= 0;",
    ),
)

_SHAPE = (
    "select c.TABLE_NAME as [Table], c.COLUMN_NAME as [Column], "
    "c.IS_NULLABLE as [Nullable], c.DATA_TYPE as [Type] "
    "from INFORMATION_SCHEMA.COLUMNS as c "
    "join INFORMATION_SCHEMA.TABLES as t "
    "on t.TABLE_SCHEMA = c.TABLE_SCHEMA and t.TABLE_NAME = c.TABLE_NAME "
    "where c.TABLE_SCHEMA = N'_' and t.TABLE_TYPE = N'BASE TABLE'"
)

_OBJECTS = (
    "select schema_name(o.schema_id) as [Schema], o.name as [Name], "
    "o.type as [Type], o.modify_date as [Modified] "
    "from sys.objects as o where o.is_ms_shipped = 0 "
    "and o.type in (N'U', N'V', N'P') and schema_name(o.schema_id) <> N'_'"
)


@dataclass(frozen=True)
class Snapshot:
    """A catalogue Warehouse at one moment: its shape, content and other objects."""

    shape: frozenset
    content: dict
    objects: frozenset


# --- the previous release -----------------------------------------------------


@pytest.fixture(scope="session")
def previous_weaver(request):
    """A venv holding the previous release, and its wheel. Reused across runs."""

    root = Path(request.config.cache.mkdir(f"weaverstack-{PREVIOUS_RELEASE}"))
    python = root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    wheels = root / "wheel"
    if not python.exists() or not list(wheels.glob("*.whl")):
        venv.create(root, with_pip=True, clear=True)
        try:
            for arguments in (
                ["install", f"weaverstack=={PREVIOUS_RELEASE}"],
                ["download", f"weaverstack=={PREVIOUS_RELEASE}", "--no-deps"]
                + ["-d", str(wheels)],
            ):
                subprocess.run(
                    [str(python), "-m", "pip", "-q", *arguments],
                    check=True,
                    capture_output=True,
                    text=True,
                )
        except subprocess.CalledProcessError as exc:
            pytest.skip(
                f"could not install weaverstack {PREVIOUS_RELEASE}: {exc.stderr}"
            )
    (wheel,) = wheels.glob("*.whl")
    return python, wheel


@pytest.fixture(scope="session")
def previous_estate(
    previous_weaver,
    fabric_workspace,
    fabric_previous_catalogue,
    fabric_previous_lakehouse,
    fabric_staging_lakehouse,
    environment_name,
    warehouse_session,
    tmp_path_factory,
):
    """Both items installed, loaded and tested by the previous release.

    Session-scoped, and first acquired by the exclusive test below, because the
    previous release's Livy session has to finish before the shared one starts.
    """

    from weaver.physical_wipe import wipe_lakehouse, wipe_sql_target

    python, wheel = previous_weaver
    catalogue = fabric_previous_catalogue.name
    lakehouse = fabric_previous_lakehouse.name
    sql = warehouse_session.sql_executor(
        WarehouseTarget(ItemRef(catalogue)), workspace=fabric_workspace
    )
    # Every run starts from empty items and a catalogue the release creates.
    wipe_sql_target(WarehouseTarget(ItemRef(catalogue)), fabric_workspace, sql=sql)
    sql.execute_script(_FORGET_CATALOGUE_SQL)
    wipe_lakehouse(ItemRef(lakehouse), fabric_workspace, session=warehouse_session)

    root = tmp_path_factory.mktemp("previous") / "Upgrade"
    estate = _estate(root)
    request = {
        "workspace": fabric_workspace.workspace,
        "catalogue": catalogue,
        "environment": environment_name,
        "staging": fabric_staging_lakehouse.name,
        "wheel": str(wheel),
        "repository": str(estate),
        "result": str(root.parent / "result.json"),
        "steps": [
            [
                "build",
                [
                    f"{WAREHOUSE_ITEM}=Warehouse/{catalogue}",
                    f"{LAKEHOUSE_ITEM}=Lakehouse/{lakehouse}",
                ],
            ],
            ["load", [WAREHOUSE_ITEM, LAKEHOUSE_ITEM]],
            ["test", [WAREHOUSE_ITEM, LAKEHOUSE_ITEM]],
        ],
    }
    path = root.parent / "request.json"
    path.write_text(json.dumps(request), encoding="utf-8")
    driver = Path(__file__).with_name("previous_release.py")
    completed = subprocess.run(
        [str(python), "-I", str(driver), str(path)],
        capture_output=True,
        text=True,
        timeout=3600,
    )
    outcome = json.loads(Path(request["result"]).read_text(encoding="utf-8"))
    outcome["stderr"] = completed.stderr[-4000:]
    return {
        "outcome": outcome,
        "catalogue": catalogue,
        "lakehouse": lakehouse,
        "estate": estate,
        "before": _snapshot(sql),
    }


def _estate(root: Path) -> Path:
    """One repository holding the Warehouse and Lakehouse items."""

    import shutil

    for fixture, item in (
        (WAREHOUSE_ESTATE_FIXTURE, WAREHOUSE_ITEM),
        (LAKEHOUSE_JOURNEY_FIXTURE, LAKEHOUSE_ITEM),
    ):
        shutil.copytree(fixture.path / item, root / item)
    return root


@pytest.mark.slow
@weaver_test(remote=True)
def test_the_previous_release_installs_an_estate_this_version_has_not_seen(
    exclusive_livy_slot, previous_estate
):
    """The release built, loaded and tested both items, in a catalogue of its own.

    Its catalogue lacks what this version introduced: Installation records no
    Fabric GUIDs and there are no semantic catalogue tables.
    """

    outcome = previous_estate["outcome"]
    assert "error" not in outcome, outcome.get("traceback") or outcome
    assert outcome["version"] == PREVIOUS_RELEASE
    assert [step["status"] for step in outcome["steps"]] == ["succeeded"] * 3, outcome

    shape = previous_estate["before"].shape
    tables = {table for table, _column, _null, _type in shape}
    installation = {
        column for table, column, _null, _type in shape if table == "Installation"
    }
    assert "Target item GUID" not in installation
    assert not tables & {table.name for table in SEMANTIC_TABLES}


# --- this version -------------------------------------------------------------


@pytest.fixture(scope="module")
def journey(
    previous_estate,
    fabric_workspace,
    fabric_fork_catalogue,
    fabric_mirror_warehouse,
    fabric_mirror_lakehouse,
    weaver_session,
    tmp_path_factory,
):
    """Mirror the previous release's catalogue, then build, load and test."""

    register_session(weaver_session)
    run = Acceptance(name="mirror-upgrade")
    run.workspace = fabric_workspace
    run.session = weaver_session
    run.source_catalogue_name = previous_estate["catalogue"]
    run.catalogue_name = fabric_fork_catalogue.name
    run.source_sql = _sql(run, run.source_catalogue_name)
    run.catalogue_sql = _sql(run, run.catalogue_name)
    run.target_sql = _sql(run, fabric_mirror_warehouse.name)
    run.forked = replace(fabric_workspace, catalogue=f"Warehouse/{run.catalogue_name}")
    estate = previous_estate["estate"]
    into_mirror = [
        f"{WAREHOUSE_ITEM}=Warehouse/{fabric_mirror_warehouse.name}",
        f"{LAKEHOUSE_ITEM}=Lakehouse/{fabric_mirror_lakehouse.name}",
    ]
    previous = previous_estate["outcome"]
    run.step(
        "the previous release installed the estate",
        lambda: _require(previous),
    )
    run.step(
        "mirror the catalogue alone",
        lambda: _mirror(run, no_item=True),
        observe=lambda: _catalogue(run.catalogue_sql),
    )
    run.step(
        "mirror the items",
        lambda: _mirror(run, into_mirror),
        observe=lambda: _catalogue(run.catalogue_sql),
    )
    run.step(
        "build with nothing changed",
        lambda: weaver.build(
            str(estate), items=into_mirror, session=weaver_session,
            catalogue=f"Warehouse/{run.catalogue_name}",
        ),
    )  # fmt: skip
    run.step("change a declaration in each item", lambda: _change(estate))
    run.step(
        "build the changes",
        lambda: weaver.build(
            str(estate), items=into_mirror, session=weaver_session,
            catalogue=f"Warehouse/{run.catalogue_name}",
        ),
        observe=lambda: _catalogue(run.catalogue_sql),
    )  # fmt: skip
    config = _forked_config(run, tmp_path_factory.mktemp("upgrade-load"))
    run.step(
        "load what the builds materialised",
        lambda: weaver.load(
            [WAREHOUSE_ITEM, LAKEHOUSE_ITEM], stale=True, session=weaver_session,
            workspace_config=config,
        ),
    )  # fmt: skip
    run.step(
        "test the mirrored items",
        lambda: weaver.test(
            [WAREHOUSE_ITEM, LAKEHOUSE_ITEM], session=weaver_session,
            workspace_config=config,
        ),
    )  # fmt: skip
    run.step("read the source catalogue again", lambda: _snapshot(run.source_sql))
    run.step("damage the source", lambda: _damage(run.source_sql))
    run.step(
        "mirror the damaged source",
        lambda: _refused(run, into_mirror),
    )
    yield run
    run.close()


def _forked_config(run, directory: Path) -> Path:
    """The fork, the catalogue it mirrors and the Environment a load runs in."""

    path = forked_config(run, directory)
    path.write_text(
        path.read_text(encoding="utf-8")
        + f"\nenvironment: {run.workspace.environment}\n",
        encoding="utf-8",
    )
    return path


def _sql(run, name: str):
    return run.session.sql_executor(
        WarehouseTarget(ItemRef(name)), workspace=run.workspace
    )


def _require(outcome) -> None:
    if "error" in outcome or any(
        step["status"] != "succeeded" for step in outcome["steps"]
    ):
        raise AssertionError(f"the previous release did not install: {outcome}")


def _mirror(run, items=None, *, no_item=False):
    return weaver.mirror(
        items,
        no_item=no_item,
        session=run.session,
        workspace=run.workspace.workspace,
        environment=str(run.workspace.environment),
        catalogue=f"Warehouse/{run.catalogue_name}",
        mirror=f"Warehouse/{run.source_catalogue_name}",
    )


def _change(estate: Path) -> None:
    for item, relative, before, after in CHANGES:
        path = estate / item / relative
        text = path.read_text(encoding="utf-8")
        changed = text.replace(before, after, 1)
        assert changed != text, f"{relative}: the declaration to change was not found"
        path.write_text(changed, encoding="utf-8")


def _damage(sql) -> None:
    """Remove a table every Weaver version's catalogue holds.

    The next run's installation starts from an empty Warehouse, so nothing
    restores it.
    """

    sql.execute_script("drop table [_].[Shortcut];")


def _refused(run, items):
    """The mirror's refusal, with the destination read before and after it."""

    before = (_snapshot(run.catalogue_sql), _objects(run.target_sql))
    try:
        _mirror(run, items)
    except Exception as exc:  # noqa: BLE001 - the refusal is the result
        refusal = exc
    else:
        raise AssertionError("a damaged source catalogue was mirrored")
    after = (_snapshot(run.catalogue_sql), _objects(run.target_sql))
    return {"refusal": refusal, "before": before, "after": after}


def _snapshot(sql) -> Snapshot:
    """The ``_`` schema, a checksum of every table in it, and the other objects."""

    shape = frozenset(
        (str(r["Table"]), str(r["Column"]), str(r["Nullable"]), str(r["Type"]))
        for r in sql.query(_SHAPE)
    )
    tables = sorted({table for table, *_ in shape})
    content = {}
    if tables:
        content = {
            str(r["Table"]): (int(r["Rows"]), r["Checksum"])
            for r in sql.query(
                "\nunion all\n".join(
                    f"select N'{table}' as [Table], count_big(*) as [Rows], "
                    f"checksum_agg(binary_checksum(*)) as [Checksum] "
                    f"from [_].[{table}]"
                    for table in tables
                )
            )
        }
    return Snapshot(shape=shape, content=content, objects=_objects(sql))


def _objects(sql) -> frozenset:
    return frozenset(
        (str(r["Schema"]), str(r["Name"]), str(r["Type"]).strip(), str(r["Modified"]))
        for r in sql.query(_OBJECTS)
    )


def _catalogue(sql) -> dict:
    """What the destination catalogue holds, in one round trip."""

    queries = {
        "installation": "select [Item type], [Item name], [Workspace name], "
        "[Workspace GUID], [Target name], [Target item GUID], [Weaver version] "
        "from [_].[Installation]",
        "registry": "select [Item type], [Item name], [Schema name], [Object name], "
        "[Signature] from [_].[Registry] "
        "where not ([Item type] = N'Warehouse' and [Item name] = N'_weaver')",
        "tables": "select t.TABLE_NAME as [Table] from INFORMATION_SCHEMA.TABLES as t "
        "where t.TABLE_SCHEMA = N'_' and t.TABLE_TYPE = N'BASE TABLE'",
        "semantic": "\nunion all\n".join(
            f"select N'{table.name}' as [Table], count_big(*) as [Rows] "
            f"from [_].[{table.name}]"
            for table in SEMANTIC_TABLES
        ),
    }
    sets = sql.query_result_sets(";\n".join(queries.values()) + ";")
    return {name: [dict(row) for row in rows] for name, rows in zip(queries, sets)}


# --- the claims ---------------------------------------------------------------


def _source_registry(journey) -> set:
    return {(r[0], r[1], r[2], r[3], r[4]) for r in _registry_rows(journey.source_sql)}


def _registry_rows(sql):
    return [
        (
            str(r["Item type"]),
            str(r["Item name"]),
            str(r["Schema name"]),
            str(r["Object name"]),
            str(r["Signature"]),
        )
        for r in sql.query(
            "select [Item type], [Item name], [Schema name], [Object name], "
            "[Signature] from [_].[Registry] "
            "where not ([Item type] = N'Warehouse' and [Item name] = N'_weaver')"
        )
    ]


@pytest.mark.slow
@weaver_test(remote=True, resources={"rest", "tds"})
def test_the_catalogue_alone_upgrades_into_this_versions_shape(
    journey, previous_estate
):
    """``--no-item``: every installation keeps its target and gains its GUIDs."""

    journey.require("mirror the catalogue alone")
    seen = journey["mirror the catalogue alone"].observation

    assert {table.name for table in CATALOGUE_TABLES} <= {
        str(row["Table"]) for row in seen["tables"]
    }
    assert {str(row["Table"]): int(row["Rows"]) for row in seen["semantic"]} == {
        table.name: 0 for table in SEMANTIC_TABLES
    }
    installed = {
        (str(row["Item type"]), str(row["Item name"])): row
        for row in seen["installation"]
    }
    resolver = journey.session.resolver(journey.workspace)
    for item, kind, target in (
        ("Reporting", "Warehouse", previous_estate["catalogue"]),
        ("Sales", "Lakehouse", previous_estate["lakehouse"]),
    ):
        row = installed[(kind, item)]
        physical = resolver.resolve(ItemRef(target), item_type=kind)
        assert row["Target name"] == target
        assert str(row["Target item GUID"]).casefold() == physical.id.casefold()
        assert str(row["Workspace GUID"]).casefold() == physical.workspace_id.casefold()
        # The release that reconciled the installation is still the one named.
        assert row["Weaver version"] == PREVIOUS_RELEASE
    assert {
        (r[0], r[1], r[2], r[3], r[4])
        for r in (
            (
                str(row["Item type"]),
                str(row["Item name"]),
                str(row["Schema name"]),
                str(row["Object name"]),
                str(row["Signature"]),
            )
            for row in seen["registry"]
        )
    } == _source_registry(journey)


@pytest.mark.slow
@weaver_test(remote=True)
def test_both_items_mirror_from_the_previous_release(journey, fabric_mirror_warehouse):
    journey.require("mirror the items")
    seen = journey["mirror the items"].observation
    installed = {str(row["Item name"]): row for row in seen["installation"]}

    assert installed["Reporting"]["Target name"] == fabric_mirror_warehouse.name
    assert installed["Reporting"]["Weaver version"] == weaver.__version__
    assert installed["Sales"]["Weaver version"] == weaver.__version__


@pytest.mark.slow
@weaver_test(remote=True, resources={"tds"})
def test_this_version_builds_loads_and_tests_the_mirrored_items(journey):
    """The trial itself: what production would do after the upgrade."""

    journey.require(
        "build with nothing changed",
        "build the changes",
        "load what the builds materialised",
        "test the mirrored items",
    )
    seen = journey["build the changes"].observation
    signatures = {
        (str(row["Item name"]), f"{row['Schema name']}.{row['Object name']}"): str(
            row["Signature"]
        )
        for row in seen["registry"]
    }
    source = {(r[1], f"{r[2]}.{r[3]}"): r[4] for r in _source_registry(journey)}
    assert (
        signatures[("Reporting", "Wh.Product")] != source[("Reporting", "Wh.Product")]
    )
    assert (
        signatures[("Sales", "Tables/DWG.NamedCustomer")]
        != (source[("Sales", "Tables/DWG.NamedCustomer")])
    )


@pytest.mark.slow
@weaver_test(remote=True)
def test_the_previous_releases_catalogue_is_never_changed(journey, previous_estate):
    """Shape, every row and every object the source Warehouse holds."""

    journey.require("read the source catalogue again")
    after = journey["read the source catalogue again"].result
    before = previous_estate["before"]

    assert after.shape == before.shape
    assert after.content == before.content
    assert after.objects == before.objects


@pytest.mark.slow
@weaver_test(remote=True)
def test_a_damaged_source_is_refused_before_the_destination_is_touched(journey):
    journey.require("mirror the damaged source")
    seen = journey["mirror the damaged source"].result

    assert "cannot be mirrored: it has no Shortcut" in str(seen["refusal"])
    assert seen["after"] == seen["before"]


_FORGET_CATALOGUE_SQL = """\
declare @weaver_sql nvarchar(max);
select @weaver_sql = string_agg(
    convert(nvarchar(max), N'drop view [_].' + quotename(name) + N';'), char(10))
from sys.views where schema_name(schema_id) = N'_';
if @weaver_sql is not null exec sys.sp_executesql @weaver_sql;
set @weaver_sql = null;
select @weaver_sql = string_agg(
    convert(nvarchar(max), N'drop procedure [_].' + quotename(name) + N';'), char(10))
from sys.procedures where schema_name(schema_id) = N'_';
if @weaver_sql is not null exec sys.sp_executesql @weaver_sql;
set @weaver_sql = null;
select @weaver_sql = string_agg(
    convert(nvarchar(max), N'drop table [_].' + quotename(name) + N';'), char(10))
from sys.tables where schema_name(schema_id) = N'_';
if @weaver_sql is not null exec sys.sp_executesql @weaver_sql;
if schema_id(N'_') is not null exec(N'drop schema [_]');
"""
