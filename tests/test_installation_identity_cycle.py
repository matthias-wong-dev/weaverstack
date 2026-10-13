"""Installation keeps logical keys and resolved Fabric identity."""

from support.weaver_test import weaver_test

from weaver.catalogue.tables import INSTALLATION


@weaver_test()
def test_installation_public_identity_schema_matches_fragment():
    from importlib.resources import files

    import yaml

    expected = (
        "Item type",
        "Item name",
        "Workspace name",
        "Workspace GUID",
        "Target name",
        "Target item GUID",
        "Weaver version",
        "Signature",
    )
    assert tuple(c.public_name for c in INSTALLATION.columns) == expected
    assert all(c.not_null for c in INSTALLATION.columns)
    assert INSTALLATION.key == ("item_type", "item_name")
    fragment = (
        files("weaver.fragments").joinpath("catalogue/_.Installation.sql").read_text()
    )
    metadata = yaml.safe_load(fragment.split("*/", 1)[0].removeprefix("/*"))
    assert tuple(metadata["Schema"]) == expected
    assert set(expected) <= set(metadata["Not null"]) | {"Item type", "Item name"}


def legacy_connection(*, nullable=True, legacy=True):
    from weaver.catalogue.connection import CatalogueConnection
    from weaver.catalogue.state import CHECKED_TABLES

    shape = [
        {
            "TABLE_NAME": t.name,
            "COLUMN_NAME": (
                {"workspace_id": "Workspace ID", "item_id": "Item ID"}.get(
                    c.name, c.public_name
                )
                if t == INSTALLATION and legacy
                else c.public_name
            ),
            "IS_NULLABLE": "YES" if t == INSTALLATION and nullable else "NO",
        }
        for t in CHECKED_TABLES
        for c in t.columns
        if not (t == INSTALLATION and legacy and c.name == "workspace_name")
    ]
    statements = []

    def query(sql):
        statements.append(sql)
        return shape if "INFORMATION_SCHEMA" in sql else []

    return CatalogueConnection(query), statements


@weaver_test()
def test_legacy_identity_read_retains_old_ids_and_physical_nullability():
    from weaver.catalogue.reader import table_query
    from weaver.catalogue.state import (
        Catalogue,
        read_catalogue_state,
        reconcile_catalogue_state,
    )

    connection, statements = legacy_connection()
    catalogue = read_catalogue_state(connection, ())
    assert catalogue.installation_upgrade["columns"]["Workspace ID"] == "YES"
    query = table_query(connection, INSTALLATION)
    assert "CAST([Workspace ID] AS varchar(128)) AS [workspace_id]" in query
    assert "CAST([Item ID] AS varchar(128)) AS [item_id]" in query
    assert "CAST(NULL AS varchar(128)) AS [workspace_name]" in query
    restored = Catalogue.from_mapping(catalogue.to_mapping())
    assert restored.installation_upgrade == catalogue.installation_upgrade
    assert (
        reconcile_catalogue_state(
            restored, inventories={}
        ).catalogue.installation_upgrade
        == catalogue.installation_upgrade
    )
    assert len([s for s in statements if "INFORMATION_SCHEMA" in s]) == 1


@weaver_test()
def test_legacy_upgrade_resolves_every_record_without_rebinding_or_refreshing_names():
    from support.workspaces import given_resolver

    from weaver.catalogue.installation import resolve_installations
    from weaver.catalogue.state import Catalogue

    resolver = given_resolver(warehouses=["Catalogue", "Sales"], lakehouses=["Landing"])
    connection, _ = legacy_connection()
    columns = connection.nullability_of(INSTALLATION)
    rows = [
        dict(
            item_type=kind,
            item_name=name,
            target_name=target,
            workspace_id=None,
            item_id=target,
            workspace_name=None,
            weaver_version="old",
            signature=name,
        )
        for kind, name, target in [
            ("Warehouse", "_weaver", "Catalogue"),
            ("Warehouse", "Orders", "Sales"),
            ("Lakehouse", "Raw", "Landing"),
        ]
    ]
    catalogue = Catalogue({}, installation_upgrade={"columns": columns})
    resolved = resolve_installations(catalogue, rows, resolver=resolver)
    assert len(resolved) == 3
    assert all(row["workspace_id"] == resolver.workspace.id for row in resolved)
    assert all(row["item_id"] != row["target_name"] for row in resolved)
    assert all(row["workspace_name"] is None for row in resolved)
    assert catalogue.installation_upgrade["rows"] == rows
    assert all(
        row["workspace_name"] == resolver.workspace.name
        for row in catalogue.installation_upgrade["resolved"]
    )
    assert [row["signature"] for row in resolved] == [row["signature"] for row in rows]
    assert resolve_installations(catalogue, rows, resolver=resolver) == resolved
    assert (
        len(resolver.client.requested) == 4
    )  # workspace and one typed lookup per recorded target


@weaver_test()
def test_replacement_is_one_transaction_preserving_all_rows_and_audits():
    import json
    from types import SimpleNamespace

    from support.workspaces import given_resolver

    from weaver.build_bundle.catalogue_actions import render_catalogue_upgrade
    from weaver.catalogue.installation import resolve_installations
    from weaver.catalogue.state import Catalogue
    from weaver.catalogue.tables import AUDIT_COLUMN_NAMES, public_column_name

    resolver = given_resolver(warehouses=["Sales", "Inventory"])
    connection, _ = legacy_connection()
    columns = {
        **connection.nullability_of(INSTALLATION),
        **{public_column_name(name): "NO" for name in AUDIT_COLUMN_NAMES},
    }
    catalogue = Catalogue({}, installation_upgrade={"columns": columns})
    rows = [
        dict(
            item_type="Warehouse",
            item_name=name,
            target_name=name,
            workspace_id=None,
            item_id=None,
            workspace_name=None,
            weaver_version="old",
            signature=name,
        )
        for name in ("Sales", "Inventory")
    ]
    resolve_installations(catalogue, rows, resolver=resolver)
    stage = render_catalogue_upgrade(
        catalogue, catalogue_target=SimpleNamespace(id="catalogue")
    )
    (script,) = json.loads(next(iter(stage.payloads.values())))
    assert script.count("BEGIN TRANSACTION") == 1
    assert "ROLLBACK" in script and "COMMIT" in script
    assert "EXEC(N'" in script
    assert "CREATE TABLE [_].[Installation_identity_stage]" in script
    assert "DROP TABLE [_].[Installation]" in script
    assert "sp_rename" in script and "OBJECT" in script
    assert "PRIMARY KEY NONCLUSTERED ([Item type], [Item name]) NOT ENFORCED" in script
    assert "[Workspace GUID] varchar(128) NOT NULL" in script
    assert "[Target item GUID] varchar(128) NOT NULL" in script
    assert "Inventory" in script and "Sales" in script
    assert "EXCEPT" in script and "COUNT_BIG(*)" in script
    assert (
        "FROM [_].[Installation_identity_stage] GROUP BY [Item type], [Item name] HAVING COUNT_BIG(*) > 1"
        in script
    )
    assert "Installation changed since planning" in script
    assert "Installation stage already exists" in script
    for name in AUDIT_COLUMN_NAMES:
        assert f"s.[{public_column_name(name)}]" in script
    assert "SYSDATETIME" not in script
    assert "SET XACT_ABORT" not in script
    from weaver.build_bundle.executors.base import InstallationContext
    from weaver.build_bundle.executors.tsql import TSqlBatchExecutor

    sent = []
    action = stage.batches[0].actions[0]
    context = InstallationContext(
        resolver=None,
        store=None,
        target=None,
        sql=SimpleNamespace(execute_script=sent.append),
    )
    TSqlBatchExecutor().execute(action, stage.payloads[action.payload], context)
    assert sent == [script]


@weaver_test()
def test_warehouse_inventory_and_publication_use_resolved_physical_identity(tmp_path):
    from types import SimpleNamespace

    from support.workspaces import given_resolver

    from weaver.build_bundle.catalogue_actions import desired_catalogue
    from weaver.build_bundle.targets import (
        ItemBinding,
        ItemBindings,
        WarehouseBinding,
        bind_inventory_target,
    )
    from weaver.build_bundle.workflow import read_target_inventories
    from weaver.declaration.model import WeaverItemId
    from weaver.sessions import TestSession
    from weaver.targets import ItemRef

    resolver = given_resolver(warehouses=["Sales"])
    item = WeaverItemId.parse("Warehouse/Orders")
    binding = ItemBinding(item, WarehouseBinding(ItemRef("Sales")))
    session = TestSession(workspace=resolver.configuration, resolver=resolver)
    inventories = read_target_inventories(
        ItemBindings((binding,)),
        session=session,
        sql_by_item={item: SimpleNamespace(query=lambda _: [])},
    )
    inventory = inventories[item]
    assert inventory.workspace_id == resolver.workspace.id
    assert inventory.workspace_name == resolver.workspace.name
    assert (
        inventory.item_id
        == resolver.resolve(ItemRef("Sales"), item_type="Warehouse").id
    )
    bound = bind_inventory_target(binding.to_bound_target(), inventory)
    assert bound.item_id == inventory.item_id
    assert bound.workspace_name == inventory.workspace_name
    assert bound.item_name == "Sales"
    from factories import single_document_repository, warehouse_table

    repository = single_document_repository(
        tmp_path,
        item=str(item),
        documents={"DWG.Customer.sql": warehouse_table("DWG.Customer")},
    )
    desired = desired_catalogue(repository, repository.source_documents, {item: bound})
    (row,) = desired.rows[item][INSTALLATION.name]
    assert row["workspace_name"] == inventory.workspace_name
    assert row["workspace_id"] == inventory.workspace_id
    assert row["item_id"] == inventory.item_id


@weaver_test()
def test_build_backfills_unselected_installations_before_semantic_planning(tmp_path):
    from test_semantic_model_build_cycle import ITEM, bundle_for, prepared

    from weaver.build_bundle.workflow import read_build_state
    from weaver.catalogue.connection import catalogue_connection
    from weaver.catalogue.reader import read_table
    from weaver.catalogue.render import InstallationScope, InstallationScopes
    from weaver.catalogue.state import READ_FOR_BUILD
    from weaver.graph import Graph

    _, repository, bindings, session, _ = prepared(tmp_path)
    connection, _ = legacy_connection()
    shape = [
        {"TABLE_NAME": name, "COLUMN_NAME": column, "IS_NULLABLE": "YES"}
        for name, columns in connection.shape().items()
        for column in columns.values()
    ]
    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'_'",
        shape,
    )
    rows = [
        dict(
            item_type=kind,
            item_name=name,
            target_name=target,
            workspace_name=None,
            workspace_id=None,
            item_id=None,
            weaver_version="old",
            signature=name,
        )
        for kind, name, target in [
            ("SemanticModel", "Reporting", "Reporting_Dev"),
            ("Warehouse", "_weaver", "Catalogue"),
        ]
    ]
    scopes = InstallationScopes((InstallationScope(ITEM.item_type, ITEM.item_name),))
    conn = catalogue_connection(session)
    for scope in (None, scopes):
        before = len(session.tsql)
        read_table(conn, INSTALLATION, scope=scope)
        session.answer_tsql(
            session.tsql[before:][-1], rows if scope is None else rows[:1]
        )
    state = read_build_state(
        bindings, required_catalogue_items=(ITEM,), session=session
    )
    assert len(state.catalogue.installation_upgrade["rows"]) == 2
    (row,) = state.catalogue.rows[ITEM][INSTALLATION.name]
    assert row["item_id"] == state.target_inventories[ITEM].item_id
    assert row["workspace_name"] is None
    assert state.catalogue.materialised == frozenset(
        t.name for t in READ_FOR_BUILD if t.name != "Mirror"
    )
    bundle = bundle_for(tmp_path, repository, bindings, state, "legacy-upgrade")
    actions = [a for _, _, a in bundle.plan.actions()]
    graph = Graph(
        (a.id for a in actions), ((dep, a.id) for a in actions for dep in a.depends_on)
    )
    for action in actions:
        if (
            action.executor in {"semantic_model", "semantic_catalogue"}
            or action.kind == "publish_registry"
        ):
            assert "upgrade-catalogue" in graph.ancestors(action.id)
    from weaver.build_bundle.execution_plan import execute_bundle
    from weaver.build_bundle.executors import default_executors
    from weaver.build_bundle.executors.tsql import TSqlBatchExecutor

    class RefuseUpgrade(TSqlBatchExecutor):
        def execute(self, action, payload, context):
            assert action.id == "upgrade-catalogue"
            raise RuntimeError("installation upgrade refused")

    executors = default_executors()
    executors["tsql_batch"] = RefuseUpgrade()
    semantic = session.semantic_model("Reporting_Dev")
    before = len(session.tsql)
    report = execute_bundle(bundle, session, executors=executors, concurrency=4)
    assert not report.succeeded
    assert (
        next(
            a for a in report.action_results() if a.action_id == "upgrade-catalogue"
        ).status
        == "failed"
    )
    assert not semantic.calls
    assert len(session.tsql) == before


@weaver_test()
def test_current_installation_rejects_nonsemantic_guid_mismatch():
    import pytest
    from support.workspaces import given_resolver

    from weaver.catalogue.installation import resolve_installations
    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverItemId
    from weaver.errors import BuildError

    resolver = given_resolver(warehouses=["Sales"])
    item = WeaverItemId.parse("Warehouse/Orders")
    row = dict(
        item_type=item.item_type,
        item_name=item.item_name,
        target_name="Sales",
        workspace_id=resolver.workspace.id,
        item_id="11111111-1111-1111-1111-111111111111",
    )
    with pytest.raises(BuildError, match="Target item GUID"):
        resolve_installations(Catalogue({}), (row,), resolver=resolver, items=(item,))


@weaver_test()
def test_frozen_warehouse_and_lakehouse_guids_validate_without_name_lookup_fallback():
    from dataclasses import replace

    import pytest
    from support.workspaces import given_resolver

    from weaver.build_bundle.installer import MutationBindings
    from weaver.errors import InstallError
    from weaver.mutation.targets import BoundTarget
    from weaver.sessions import TestSession
    from weaver.targets import ItemRef

    resolver = given_resolver(warehouses=["Sales"], lakehouses=["Landing"])
    session = TestSession(workspace=resolver.configuration, resolver=resolver)
    bindings = MutationBindings(session).bind(resolver.configuration)
    for kind, name, physical_kind in [
        ("warehouse", "Sales", "Warehouse"),
        ("lakehouse", "Landing", "Lakehouse"),
    ]:
        item = resolver.resolve(ItemRef(name), item_type=physical_kind)
        bound = BoundTarget(
            kind, kind, item.id, item_name=name, workspace_id=item.workspace_id
        )
        target = bindings.resolve_target(bound)
        if kind == "warehouse":
            bindings.sql_for(bound).query("SELECT 1")
        else:
            assert target.lakehouse.name == name
        with pytest.raises(InstallError, match="different physical binding"):
            bindings.resolve_target(
                replace(bound, item_id="11111111-1111-1111-1111-111111111111")
            )


@weaver_test()
def test_catalogue_control_reuses_the_resolved_builtin_target():
    from support.workspaces import given_resolver

    from weaver.build_bundle.planner import _catalogue_target
    from weaver.build_bundle.targets import ItemBinding, WarehouseBinding
    from weaver.catalogue.builtin import BUILTIN_ITEM
    from weaver.targets import ItemRef

    resolver = given_resolver(warehouses=["Catalogue"])
    resolved = resolver.resolve(ItemRef("Catalogue"), item_type="Warehouse")
    configured = WarehouseBinding(
        ItemRef("Catalogue"), workspace_name=resolver.workspace.name
    )
    target = ItemBinding(
        BUILTIN_ITEM,
        WarehouseBinding(
            ItemRef(resolved.name),
            workspace_id=resolved.workspace_id,
            item_id=resolved.id,
            workspace_name=resolver.workspace.name,
        ),
    ).to_bound_target()
    assert _catalogue_target(configured, (target,)) is target


@weaver_test()
def test_frozen_semantic_item_requires_the_configured_workspace_name():
    from types import SimpleNamespace

    import pytest
    from support.workspaces import InventoryClient

    from weaver.errors import WeaverError
    from weaver.fabric.resolution import FabricResolver
    from weaver.fabric.resources import Item
    from weaver.sessions.console import ConsoleSession
    from weaver.workspaces import Workspace

    workspace = Workspace(workspace="Old name")
    inventory = InventoryClient("New name", [("SemanticModel", "Reporting")])
    resolver = FabricResolver(workspace, client=inventory)
    credential = SimpleNamespace(
        get_token=lambda _: SimpleNamespace(token="test", expires_on=1)
    )
    recorded = Item(
        "11111111-1111-1111-1111-111111111111",
        "Reporting",
        "SemanticModel",
        "22222222-2222-2222-2222-222222222222",
    )
    with ConsoleSession(
        workspace=workspace, resolver=resolver, credential=credential
    ) as session:
        with pytest.raises(WeaverError, match="Old name"):
            session.semantic_model(recorded)
    assert inventory.requested == ["workspaces"]


@weaver_test()
def test_current_physical_not_null_schema_needs_no_second_migration():
    from weaver.catalogue.installation import installation_schema_upgrade

    connection, statements = legacy_connection(nullable=False, legacy=False)
    assert installation_schema_upgrade(connection) is None
    assert installation_schema_upgrade(connection) is None
    assert len(statements) == 1
    nullable, _ = legacy_connection(nullable=True, legacy=False)
    assert installation_schema_upgrade(nullable) is not None


@weaver_test()
def test_claim_reconciliation_retains_the_installation_upgrade_snapshot():
    from weaver.catalogue.claims import (
        CatalogueClaim,
        claim_rules_for_object_type,
        without_claims,
    )
    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverDocumentId

    upgrade = {"columns": {"Item ID": "YES"}, "rows": [], "resolved": []}
    catalogue = Catalogue({}, installation_upgrade=upgrade)
    claim = CatalogueClaim(
        WeaverDocumentId.parse("Warehouse/Sales/DWG.Customer"),
        claim_rules_for_object_type("table")[0],
    )
    retained = without_claims(catalogue, (claim,))
    assert retained.installation_upgrade == upgrade


@weaver_test()
def test_forgetting_shape_discards_legacy_nullability():
    connection, _ = legacy_connection()
    assert "Item ID" in connection.nullability_of(INSTALLATION)
    current, _ = legacy_connection(nullable=False, legacy=False)
    connection._query = current._query
    connection.forget_shape()
    assert "Item ID" not in connection.nullability_of(INSTALLATION)
    assert all(
        value == "NO" for value in connection.nullability_of(INSTALLATION).values()
    )


@weaver_test()
def test_workspace_name_refresh_keeps_existing_objects_and_signatures(tmp_path):
    from dataclasses import replace

    from support.bundles import build_metadata
    from support.workspaces import given_resolver
    from test_incremental_build_install import (
        _catalogue,
        _raw_binding,
        _raw_inventory,
        _repository,
    )
    from test_item_repository_declaration import _estate

    from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
    from weaver.build_bundle.catalogue_actions import desired_catalogue
    from weaver.build_bundle.targets import bind_inventory_target
    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverItemId
    from weaver.locations import Location
    from weaver.store import FilesystemStore
    from weaver.targets import ItemRef

    repository = _repository(_estate(tmp_path / "project"))
    item = WeaverItemId.parse("Lakehouse/Raw")
    bindings = _raw_binding()
    resolver = given_resolver(lakehouses=["Raw_Target"])
    physical = resolver.resolve(ItemRef("Raw_Target"), item_type="Lakehouse")
    inventory = replace(
        _raw_inventory(repository)[item],
        workspace_id=physical.workspace_id,
        item_id=physical.id,
        workspace_name="New name",
    )
    target = bind_inventory_target(bindings.by_item[item].to_bound_target(), inventory)
    installed = _catalogue(repository, str(item))
    desired = desired_catalogue(repository, installed.registered, {item: target})
    tables = dict(installed.rows[item])
    row = dict(desired.rows[item][INSTALLATION.name][0], workspace_name="Old name")
    tables[INSTALLATION.name] = (row,)
    current = Catalogue({item: tables})
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        catalogue=current,
        target_inventories={item: inventory},
        catalogue_binding=WarehouseBinding(ItemRef("Catalogue")),
        output=Location((tmp_path / "rename").as_posix()),
        store=FilesystemStore(),
    )
    assert not build_metadata(bundle.plan).selection.selected_for_build
    assert not build_metadata(bundle.plan).selection.selected_for_drop
    actions = [a for _, _, a in bundle.plan.actions()]
    assert not any(
        a.kind
        in {
            "build_table",
            "build_folder",
            "build_view",
            "write_file",
            "publish_registry",
        }
        for a in actions
    )
    assert not any(a.id == "upgrade-catalogue" for a in actions)
    installation = next(a for a in actions if a.id == "publish-catalogue")
    payload = bundle.store.read(bundle.location / installation.payload)
    assert b"New name" in payload and physical.id.encode() in payload
    assert row["workspace_name"] == "Old name"
    assert row["signature"] == desired.rows[item][INSTALLATION.name][0]["signature"]


@weaver_test()
def test_report_only_build_uses_backfilled_model_ids_before_publication(tmp_path):
    from test_report_build_cycle import prepared_project
    from test_semantic_model_build_cycle import ITEM, bundle_for

    from weaver.build_bundle.targets import ItemBindings, parse_build_item
    from weaver.build_bundle.workflow import read_build_state
    from weaver.catalogue.connection import catalogue_connection
    from weaver.catalogue.reader import table_query
    from weaver.catalogue.render import InstallationScope, InstallationScopes
    from weaver.catalogue.tables import REGISTRY
    from weaver.declaration.model import WeaverItemId
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location

    root, _, _, _, _ = prepared_project(tmp_path)
    from support.workspaces import InventoryClient

    from weaver.fabric.resolution import FabricResolver
    from weaver.sessions import TestSession
    from weaver.workspaces import Workspace

    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Catalogue")
    session = TestSession(
        workspace=workspace,
        resolver=FabricResolver(
            workspace,
            client=InventoryClient(
                "Demo",
                [
                    ("SemanticModel", "Reporting_Dev"),
                    ("Report", "Executive_Dev"),
                    ("Warehouse", "Catalogue"),
                ],
            ),
        ),
    )
    repository = parse_item_repository(Location(root.as_posix()))
    report = WeaverItemId.parse("Report/Executive")
    bindings = ItemBindings(
        (parse_build_item("Report/Executive=Report/Executive_Dev"),)
    )
    legacy, _ = legacy_connection()
    shape = [
        {"TABLE_NAME": table, "COLUMN_NAME": name, "IS_NULLABLE": "YES"}
        for table, columns in legacy.shape().items()
        for name in columns.values()
    ]
    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'_'",
        shape,
    )
    connection = catalogue_connection(session)
    rows = [
        dict(
            item_type="SemanticModel",
            item_name="Reporting",
            target_name="Reporting_Dev",
            workspace_name=None,
            workspace_id=None,
            item_id=None,
            signature="model-signature",
            weaver_version="old",
        )
    ]
    scopes = InstallationScopes(
        tuple(
            InstallationScope(i.item_type, i.item_name)
            for i in sorted((report, ITEM), key=str)
        )
    )
    for scope in (None, scopes):
        session.answer_tsql(table_query(connection, INSTALLATION, scope=scope), rows)
    session.answer_tsql(
        table_query(connection, REGISTRY, scope=scopes),
        [
            dict(
                item_type="SemanticModel",
                item_name="Reporting",
                schema_name="",
                object_name="",
                object_type="semantic_model",
                object_role="data",
                signature="model-signature",
            )
        ],
    )
    before = len(session.tsql)
    state = read_build_state(
        bindings,
        required_catalogue_items=(report, ITEM),
        session=session,
        repository=repository,
    )
    assert (
        len(
            [
                sql
                for sql in session.tsql[before:]
                if "INFORMATION_SCHEMA.COLUMNS" in sql
            ]
        )
        == 1
    )
    model = state.catalogue.rows[ITEM][INSTALLATION.name][0]
    assert model["workspace_name"] is None
    assert model["signature"] == "model-signature"
    assert (
        model["item_id"]
        == session.resolve_item("Reporting_Dev", item_type="SemanticModel").id
    )
    bundle = bundle_for(tmp_path, repository, bindings, state, "report-only-legacy")
    actions = [a for _, _, a in bundle.plan.actions()]
    assert not any(a.executor.startswith("semantic_") for a in actions)
    report_action = next(a for a in actions if a.executor == "report_definition")
    payload = bundle.store.read(bundle.location / report_action.payload)
    assert (
        model["item_id"].encode() in payload
        and model["workspace_id"].encode() in payload
    )
    assert any(a.id == "upgrade-catalogue" for a in actions)


@weaver_test()
def test_upgrade_rejects_malformed_names_new_guid_name_fallback_and_mismatches():
    import pytest
    from support.workspaces import given_resolver

    from weaver.catalogue.installation import resolve_installations
    from weaver.catalogue.state import Catalogue
    from weaver.errors import BuildError

    resolver = given_resolver(warehouses=["Sales"])
    row = dict(
        item_type="Warehouse",
        item_name="Orders",
        target_name="Sales",
        workspace_id=None,
        item_id=None,
    )
    connection, _ = legacy_connection()
    legacy = connection.nullability_of(INSTALLATION)
    current, _ = legacy_connection(legacy=False)
    cases = [
        (legacy, {"item_id": "arbitrary name"}),
        (current.nullability_of(INSTALLATION), {"item_id": "Sales"}),
        (legacy, {"item_id": "11111111-1111-1111-1111-111111111111"}),
        (legacy, {"workspace_id": "11111111-1111-1111-1111-111111111111"}),
        (legacy, {"workspace_id": " " + resolver.workspace.id}),
    ]
    for columns, values in cases:
        catalogue = Catalogue({}, installation_upgrade={"columns": columns})
        with pytest.raises(BuildError, match="GUID"):
            resolve_installations(catalogue, ({**row, **values},), resolver=resolver)
        assert "resolved" not in catalogue.installation_upgrade
    with pytest.raises(BuildError, match="duplicate installation"):
        resolve_installations(
            Catalogue({}, installation_upgrade={"columns": legacy}),
            (row, row),
            resolver=resolver,
        )


@weaver_test()
def test_current_catalogue_only_build_reuses_its_scoped_installation_read():
    from types import SimpleNamespace

    from support.workspaces import given_resolver

    from weaver.build_bundle.targets import ItemBinding, ItemBindings, WarehouseBinding
    from weaver.build_bundle.workflow import read_build_state
    from weaver.catalogue.builtin import BUILTIN_ITEM
    from weaver.sessions import TestSession
    from weaver.targets import ItemRef

    resolver = given_resolver(warehouses=["Weaver"])
    session = TestSession(workspace=resolver.configuration, resolver=resolver)
    bindings = ItemBindings(
        (ItemBinding(BUILTIN_ITEM, WarehouseBinding(ItemRef("Weaver"))),)
    )
    shape, _ = legacy_connection(nullable=False, legacy=False)
    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'_'",
        [
            {"TABLE_NAME": table, "COLUMN_NAME": column, "IS_NULLABLE": "NO"}
            for table, columns in shape.shape().items()
            for column in columns.values()
        ],
    )
    state = read_build_state(
        bindings,
        required_catalogue_items=(BUILTIN_ITEM,),
        session=session,
        sql_by_item={BUILTIN_ITEM: SimpleNamespace(query=lambda _: [])},
    )
    assert state.catalogue.installation_upgrade is None
    assert len([sql for sql in session.tsql if "FROM [_].[Installation]" in sql]) == 1
    assert (
        len([sql for sql in session.tsql if "INFORMATION_SCHEMA.COLUMNS" in sql]) == 1
    )


@weaver_test()
def test_schema_inventory_requires_observed_nullability():
    import pytest

    from weaver.catalogue.connection import CatalogueConnection
    from weaver.errors import CommandError

    connection = CatalogueConnection(
        lambda _: [{"TABLE_NAME": "Installation", "COLUMN_NAME": "Item ID"}]
    )
    with pytest.raises(CommandError, match="nullability"):
        connection.shape()
