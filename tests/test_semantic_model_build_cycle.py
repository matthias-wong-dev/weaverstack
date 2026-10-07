"""Semantic definitions use Build selection, installation and certification."""

import json
import shutil
from pathlib import Path

import pytest
from support.bundles import build_metadata
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient

from weaver.build_bundle.bundle import load_bundle
from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.build_bundle.execution import ExecutionIdentity
from weaver.build_bundle.execution_plan import execute_bundle
from weaver.build_bundle.planner import certifiable_identities
from weaver.build_bundle.targets import ItemBindings, WarehouseBinding, parse_build_item
from weaver.build_bundle.workflow import (
    BuildState,
    build_repository_bundle,
    read_build_state,
)
from weaver.catalogue.semantic import project_semantic_model
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.semantic_models.definition import decode_parts, encode_definition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.targets import ItemRef
from weaver.workspaces import Workspace

ITEM = WeaverItemId.parse("SemanticModel/Reporting")
ROOT = WeaverDocumentId.parse(str(ITEM))


def project(tmp_path, pbip):
    folder = tmp_path / "project" / str(ITEM)
    folder.mkdir(parents=True)
    if pbip:
        shutil.copytree(
            Path(__file__).parent / "fixtures/semantic_model/Probe",
            folder,
            dirs_exist_ok=True,
        )
    (folder / "extension.tmdl").write_text(
        'table Calendar\n\tpartition Calendar = calculated\n\t\tsource = ROW("Year", 2026)\n',
        encoding="utf-8",
    )
    return folder.parent.parent


class DefinitionClient:
    def __init__(self):
        self.calls = []
        self.definition = encode_definition({"model": {"culture": "en-US"}})
        self.failure = None
        self.read_failure = None

    def get_definition(self):
        self.calls.append(("get_definition", None))
        if self.read_failure:
            raise self.read_failure
        return self.definition

    def bind_data_sources(self):
        self.calls.append(("bind_data_sources", None))
        return ()

    def update_definition(self, definition, **options):
        self.calls.append(("update_definition", {"definition": definition, **options}))
        if self.failure:
            raise self.failure
        return {"status": "Succeeded"}


def session_for():
    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Catalogue")
    client = InventoryClient(
        "Demo",
        [
            ("SemanticModel", "Reporting_Dev"),
            ("Warehouse", "Reporting_Dev"),
            ("Warehouse", "Catalogue"),
        ],
    )
    session = TestSession(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(workspace, client=client),
    )
    session.answer_semantic_model("Demo", "Reporting_Dev", DefinitionClient())
    return session


def prepared(tmp_path, pbip=False):
    root = project(tmp_path, pbip)
    repository = parse_item_repository(Location(root.as_posix()))
    bindings = ItemBindings((parse_build_item(f"{ITEM}=SemanticModel/Reporting_Dev"),))
    session = session_for()
    state = read_build_state(
        bindings, required_catalogue_items=(ITEM,), session=session
    )
    return root, repository, bindings, session, state


def bundle_for(tmp_path, repository, bindings, state, name):
    return build_repository_bundle(
        repository,
        state=state,
        bindings=bindings,
        catalogue_binding=WarehouseBinding(ItemRef("Catalogue"), workspace_name="Demo"),
        execution=ExecutionIdentity("Demo"),
        source_store=FilesystemStore(),
        output=Location((tmp_path / name).as_posix()),
    )


def engine_model(repository, *, year=2026):
    # Fixed boundary response; the PBIP base was recorded from Fabric. This helper
    # does not parse the submitted TMDL or certify its platform validity.
    fixture = Path(__file__).parent / "fixtures/semantic_model/observed-probe.json"
    has_pbip = any(
        p.endswith(".pbip") for p in repository.semantic_models[ITEM].sources
    )
    model = (
        json.loads(fixture.read_text())
        if has_pbip
        else {
            "compatibilityLevel": 1606,
            "model": {
                "culture": "en-US",
                "defaultPowerBIDataSourceVersion": "powerBI_V3",
            },
        }
    )
    model["model"].setdefault("tables", []).append(
        {
            "name": "Calendar",
            "partitions": [
                {
                    "name": "Calendar",
                    "source": {
                        "type": "calculated",
                        "expression": f'ROW("Year", {year})',
                    },
                }
            ],
            "columns": [
                {
                    "name": "Year",
                    "type": "calculatedTableColumn",
                    "dataType": "int64",
                    "sourceColumn": "[Year]",
                }
            ],
        }
    )
    return model


def installed_state(repository, bindings, deployed, inventories):
    from weaver.build_bundle.semantic import bind_semantic_target

    targets = {
        item: bind_semantic_target(binding.to_bound_target(), inventories[item])
        for item, binding in bindings.by_item.items()
    }
    desired = desired_catalogue(
        repository, certifiable_identities(repository, bindings.by_item), targets
    )
    rows = {item: dict(tables) for item, tables in desired.rows.items()}
    rows[ITEM].update(
        project_semantic_model(
            ITEM, repository.semantic_models[ITEM], deployed=deployed
        )
    )
    return BuildState(Catalogue(rows), inventories)


@weaver_test()
@pytest.mark.parametrize("pbip", [False, True])
def test_build_deploys_and_certifies_readback_without_touching_source(tmp_path, pbip):
    root, repository, bindings, session, state = prepared(tmp_path, pbip)
    before = {
        p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }
    bundle = bundle_for(tmp_path, repository, bindings, state, "first")
    assert build_metadata(bundle.plan).selection.selected_for_build == (ROOT,)
    assert not build_metadata(bundle.plan).selection.selected_for_drop
    assert not bundle.plan.execution.spark_home_target_id
    deployed = engine_model(repository)
    semantic = session.semantic_model("Reporting_Dev")
    semantic.definition = encode_definition(deployed)
    semantic.calls.clear()
    session.calls.clear()
    report = execute_bundle(
        load_bundle(bundle.location, store=FilesystemStore()), session
    )
    assert report.succeeded, report.to_mapping()
    assert [c[0] for c in semantic.calls] == ["update_definition", "get_definition"]
    submitted = semantic.calls[0][1]
    assert submitted["allow_purge_data"] is True
    assert (
        decode_parts(submitted["definition"]) == repository.semantic_models[ITEM].parts
    )
    writes = session.tsql
    definition_write = next(
        i for i, s in enumerate(writes) if "MERGE" in s and "[_].[SemanticModel]" in s
    )
    child_write = next(
        i for i, s in enumerate(writes) if "MERGE" in s and "SemanticModelColumn" in s
    )
    certify = next(i for i, s in enumerate(writes) if "MERGE" in s and "Registry" in s)
    assert definition_write < certify and child_write < certify
    assert "calculatedTableColumn" in writes[definition_write]
    assert "Calendar" in writes[child_write] and "Year" in writes[child_write]
    assert not session.spark_sql
    assert before == {
        p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }
    # The catalogue fixture declares the rows independently of T-SQL execution.
    state2 = read_build_state(
        bindings, required_catalogue_items=(ITEM,), session=session
    )
    installed = installed_state(
        repository, bindings, deployed, state2.target_inventories
    )
    second = bundle_for(tmp_path, repository, bindings, installed, "second")
    assert not build_metadata(second.plan).selection.selected_for_build
    assert list(second.plan.actions()) == []


@weaver_test()
@pytest.mark.parametrize(
    "failure",
    ["update", "missing_columns", "wrong_expression", "stale_measure", "stale_table"],
)
def test_failed_deployment_or_readback_cannot_certify_changed_model(tmp_path, failure):
    root, repository, bindings, session, _ = prepared(tmp_path)
    semantic = session.semantic_model("Reporting_Dev")
    deployed = engine_model(repository)
    semantic.definition = encode_definition(deployed)
    observed = read_build_state(
        bindings, required_catalogue_items=(ITEM,), session=session
    )
    installed = installed_state(
        repository, bindings, deployed, observed.target_inventories
    )
    path = root / str(ITEM) / "extension.tmdl"
    path.write_text(path.read_text().replace("2026", "2027"), encoding="utf-8")
    changed = parse_item_repository(Location(root.as_posix()))
    bundle = bundle_for(tmp_path, changed, bindings, installed, "changed")
    assert build_metadata(bundle.plan).selection.impact.changed == (ROOT,)
    assert not build_metadata(bundle.plan).selection.selected_for_drop
    if failure == "update":
        semantic.failure = RuntimeError("update failed")
    elif failure == "missing_columns":
        missing = engine_model(changed, year=2027)
        del missing["model"]["tables"][0]["columns"]
        semantic.definition = encode_definition(missing)
    elif failure in {"stale_measure", "stale_table"}:
        stale = engine_model(changed, year=2027)
        if failure == "stale_measure":
            stale["model"]["tables"][0]["measures"] = [
                {"name": "Removed", "expression": "1"}
            ]
        else:
            stale["model"]["tables"].append({"name": "Removed"})
        semantic.definition = encode_definition(stale)
    # wrong_expression retains the previous model after a successful update.
    session.calls.clear()
    report = execute_bundle(bundle, session)
    assert not report.succeeded
    assert any("DELETE FROM [_].[Registry]" in statement for statement in session.tsql)
    assert not any(
        "MERGE" in statement and "Registry" in statement for statement in session.tsql
    )
    assert not any(
        "MERGE" in statement and "SemanticModel" in statement
        for statement in session.tsql
    )


@weaver_test()
@pytest.mark.parametrize("policy", ["item", "organisation"])
def test_policy_change_selects_only_effectively_changed_models(tmp_path, policy):
    from uuid import NAMESPACE_URL, uuid5

    from weaver.build_bundle.semantic import bind_semantic_target

    root = project(tmp_path, False)
    other = WeaverItemId.parse("SemanticModel/Other")
    other_root = WeaverDocumentId.model_root(other)
    shutil.copytree(root / str(ITEM), root / str(other))
    repository = parse_item_repository(Location(root.as_posix()))
    bindings = ItemBindings(
        (
            parse_build_item(f"{ITEM}=SemanticModel/Reporting_Dev"),
            parse_build_item(f"{other}=SemanticModel/Other_Dev"),
        )
    )
    from weaver.build_bundle.prune import TargetInventory

    inventories = {}
    rows = {}
    targets = {
        item: binding.to_bound_target() for item, binding in bindings.by_item.items()
    }
    for item in (ITEM, other):
        deployed = engine_model(repository)
        inventories[item] = TargetInventory(
            targets[item].id,
            "semanticmodel",
            targets[item].name,
            workspace_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            item_id=str(uuid5(NAMESPACE_URL, str(item))),
        )
        targets[item] = bind_semantic_target(targets[item], inventories[item])
        rows[item] = project_semantic_model(
            item, repository.semantic_models[item], deployed=deployed
        )
    desired = desired_catalogue(repository, {ROOT, other_root}, targets)
    installed = Catalogue(
        {item: {**dict(desired.rows[item]), **rows[item]} for item in rows}
    )
    if policy == "item":
        addon = root / str(ITEM) / "extension.tmdl"
        addon.write_text(addon.read_text().replace("2026", "2027"), encoding="utf-8")
    else:
        (root / "SemanticModel" / "extension.tmdl").write_text(
            "/// Shared description\nmodel Model\n", encoding="utf-8"
        )
    changed = parse_item_repository(Location(root.as_posix()))
    result = bundle_for(
        tmp_path, changed, bindings, BuildState(installed, inventories), "policy"
    )
    assert set(build_metadata(result.plan).selection.selected_for_build) == (
        {ROOT} if policy == "item" else {ROOT, other_root}
    )
    assert not build_metadata(result.plan).selection.selected_for_drop
    assert {
        a.resource_node_id
        for _, _, a in result.plan.actions()
        if a.executor == "semantic_catalogue"
    } == {str(i) for i in build_metadata(result.plan).selection.selected_for_build}


@weaver_test()
def test_build_freezes_typed_ids_without_reading_existing_definition(tmp_path):
    _, repository, bindings, session, _ = prepared(tmp_path)
    semantic = session.semantic_model("Reporting_Dev")
    semantic.calls.clear()
    semantic.read_failure = PermissionError(
        "Existing definition access is not required"
    )
    observed = read_build_state(
        bindings, required_catalogue_items=(ITEM,), session=session
    )
    bundle = bundle_for(tmp_path, repository, bindings, observed, "resolved")
    assert build_metadata(bundle.plan).selection.selected_for_build == (ROOT,)
    assert semantic.calls == []
    bound = next(
        t for t in bundle.plan.targets if t.logical_item_type == "SemanticModel"
    )
    resolved = session.resolve_item("Reporting_Dev", item_type="SemanticModel")
    assert bound.item_id == resolved.id
    assert bound.workspace_id == resolved.workspace_id
    assert (
        bound.item_id != session.resolve_item("Reporting_Dev", item_type="Warehouse").id
    )


@weaver_test()
def test_organisation_policy_only_selects_effectively_changed_models(tmp_path):
    root, repository, bindings, session, _ = prepared(tmp_path)
    other = root / "SemanticModel/Other"
    other.mkdir()
    (other / "extension.tmdl").write_text(
        '/// Local policy\nmodel Model\n\ntable Constant\n\tpartition Constant = calculated\n\t\tsource = ROW("Value", 1)\n',
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(root.as_posix()))
    from weaver.build_bundle.incremental import select_build
    from weaver.build_bundle.prune import TargetInventory

    selected = {WeaverDocumentId.model_root(i) for i in repository.semantic_models}
    catalogue = Catalogue.from_repository(repository)
    inventories = {
        i: TargetInventory(str(i), "semanticmodel", i.item_name)
        for i, c in repository.semantic_models.items()
    }
    (root / "SemanticModel/extension.tmdl").write_text(
        "/// Organisation policy\nmodel Model\n", encoding="utf-8"
    )
    changed = parse_item_repository(Location(root.as_posix()))
    selection = select_build(
        changed, catalogue.registered, selected=selected, inventories=inventories
    )
    assert selection.selected_for_build == (ROOT,)


def answer_catalogue(session, catalogue, bindings):
    from weaver.catalogue.connection import catalogue_connection
    from weaver.catalogue.reader import read_table
    from weaver.catalogue.render import InstallationScope, InstallationScopes
    from weaver.catalogue.state import READ_FOR_BUILD, read_target_occupancy

    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'_'",
        [
            {"TABLE_NAME": table.name, "COLUMN_NAME": column.public_name}
            for table in READ_FOR_BUILD
            for column in table.columns
        ],
    )
    before = len(session.tsql)
    read_target_occupancy(catalogue_connection(session))
    for statement in session.tsql[before:]:
        if "FROM [_].[Installation]" in statement:
            session.answer_tsql(
                statement,
                [
                    row
                    for tables in catalogue.rows.values()
                    for row in tables.get("Installation", ())
                ],
            )
    scope = InstallationScopes(
        tuple(
            InstallationScope(item.item_type, item.item_name)
            for item in sorted(bindings.by_item, key=str)
        )
    )
    for table in READ_FOR_BUILD:
        before = len(session.tsql)
        read_table(catalogue_connection(session), table, scope=scope)
        for statement in session.tsql[before:]:
            if f"FROM [_].[{table.name}]" in statement:
                session.answer_tsql(
                    statement,
                    [
                        row
                        for tables in catalogue.rows.values()
                        for row in tables.get(table.name, ())
                    ],
                )


@weaver_test()
@pytest.mark.parametrize("pbip", [False, True])
def test_public_build_bootstraps_catalogue_and_reaches_fixed_point(tmp_path, pbip):
    import weaver

    root = project(tmp_path, pbip)
    repository = parse_item_repository(Location(root.as_posix()))
    session = session_for()
    session.semantic_model("Reporting_Dev").definition = encode_definition(
        engine_model(repository)
    )
    result = weaver.build(
        root,
        items="SemanticModel/Reporting=SemanticModel/Reporting_Dev",
        session=session,
    )
    assert result.succeeded, result.errors
    assert not session.spark_sql and not session.python
    assert ROOT in result.selection.selected_for_build
    from weaver.build_bundle.targets import effective_item_bindings
    from weaver.build_bundle.workflow import read_target_inventories
    from weaver.catalogue.builtin import BUILTIN_ITEM
    from weaver.catalogue.tables import CATALOGUE_TABLES

    bindings = effective_item_bindings(
        ItemBindings(
            (parse_build_item("SemanticModel/Reporting=SemanticModel/Reporting_Dev"),)
        ),
        control_item="Catalogue",
        workspace_name="Demo",
    )
    before = len(session.tsql)
    read_target_inventories(bindings, session=session)
    for statement in session.tsql[before:]:
        if "from sys.objects" in statement:
            session.answer_tsql(
                statement,
                [
                    {"schema_name": "_", "object_name": table.name, "object_type": "U"}
                    for table in CATALOGUE_TABLES
                ],
            )
        elif "from sys.schemas" in statement:
            session.answer_tsql(statement, [{"name": "_"}])
    inventories = read_target_inventories(bindings, session=session)
    installed = installed_state(
        repository, bindings, engine_model(repository), inventories
    )
    answer_catalogue(session, installed.catalogue, bindings)
    semantic = session.semantic_model("Reporting_Dev")
    semantic.calls.clear()
    second = weaver.build(
        root, items=str(ITEM) + "=SemanticModel/Reporting_Dev", session=session
    )
    assert second.succeeded, second.errors
    assert second.installation_report.action_counts()["total"] == 0, [
        (a.executor, a.resource_node_id)
        for a in second.installation_report.action_results()
    ]
    assert not second.selection.selected_for_build
    assert semantic.calls == []
    assert all(
        identity.item != BUILTIN_ITEM
        for identity in second.selection.selected_for_build
    )

    addon = root / str(ITEM) / "extension.tmdl"
    addon.write_text(addon.read_text().replace("2026", "2027"), encoding="utf-8")
    changed = parse_item_repository(Location(root.as_posix()))
    semantic.definition = encode_definition(engine_model(changed, year=2027))
    semantic.calls.clear()
    third = weaver.build(
        root, items=str(ITEM) + "=SemanticModel/Reporting_Dev", session=session
    )
    assert third.succeeded, third.errors
    assert third.selection.selected_for_build == (ROOT,)
    assert [call[0] for call in semantic.calls] == [
        "update_definition",
        "get_definition",
    ]
