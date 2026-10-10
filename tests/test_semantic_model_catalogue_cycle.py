"""Older catalogues gain semantic identity columns without losing state."""

from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import ITEM, bundle_for, prepared

from weaver.build_bundle.workflow import BuildState
from weaver.catalogue.connection import catalogue_connection
from weaver.catalogue.state import CHECKED_TABLES, Catalogue, read_catalogue_state
from weaver.catalogue.tables import INSTALLATION


@weaver_test()
def test_semantic_key_upgrade_delivers_warehouse_compatible_transactions(tmp_path):
    import json
    from types import SimpleNamespace

    from weaver.build_bundle.catalogue_actions import render_catalogue_upgrade
    from weaver.build_bundle.executors.base import InstallationContext
    from weaver.build_bundle.executors.tsql import TSqlBatchExecutor
    from weaver.catalogue.tables import SEMANTIC_TABLES

    catalogue = Catalogue(
        {},
        schema_removals=tuple(
            (table.name, column)
            for table in SEMANTIC_TABLES
            for column in ("Schema name", "Object name")
        ),
    )
    stage = render_catalogue_upgrade(
        catalogue, catalogue_target=SimpleNamespace(id="catalogue")
    )
    (batch,) = stage.batches
    (action,) = batch.actions
    payload = stage.payloads[action.payload]
    scripts = []
    context = InstallationContext(
        resolver=None,
        store=None,
        target=None,
        sql=SimpleNamespace(execute_script=scripts.append),
    )
    details = TSqlBatchExecutor().execute(action, payload, context)
    assert scripts == json.loads(payload)
    assert details == {"statements": 5}
    assert len(scripts) == len(SEMANTIC_TABLES)
    for script in scripts:
        assert "SET XACT_ABORT" not in script
        assert "EXEC(N'BEGIN TRY\nBEGIN TRANSACTION;\n" in script
        assert "\nCOMMIT;\nEND TRY\nBEGIN CATCH\n" in script
        assert "IF @@TRANCOUNT > 0 ROLLBACK;\nTHROW;\nEND CATCH;" in script


@weaver_test()
def test_old_catalogue_identity_columns_are_added_before_publication(tmp_path):
    _, repository, bindings, session, state = prepared(tmp_path)
    shape_sql = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'_'"
    session.answer_tsql(
        shape_sql,
        [
            {"TABLE_NAME": table.name, "COLUMN_NAME": table.public_name_of(column.name)}
            for table in CHECKED_TABLES
            for column in table.columns
            if not (
                table == INSTALLATION and column.name in {"workspace_id", "item_id"}
            )
        ],
    )
    catalogue = read_catalogue_state(catalogue_connection(session), (ITEM,))
    catalogue = Catalogue.from_mapping(catalogue.to_mapping())
    bundle = bundle_for(
        tmp_path,
        repository,
        bindings,
        BuildState(catalogue, state.target_inventories),
        "upgrade",
    )
    payloads = [
        bundle.store.read(bundle.location / action.payload)
        for _, _, action in bundle.plan.actions()
        if action.payload
    ]
    upgrades = [
        payload
        for payload in payloads
        if b"ALTER TABLE [_].[Installation] ADD" in payload
    ]
    assert len(upgrades) == 1
    assert b"[Workspace ID] varchar(128)" in upgrades[0]
    assert b"[Item ID] varchar(128)" in upgrades[0]
    assert not any(b"DROP TABLE [_].[Installation]" in payload for payload in payloads)
    from weaver.graph import Graph

    actions = [a for _, _, a in bundle.plan.actions()]
    success = Graph(
        (a.id for a in actions),
        ((dep, a.id) for a in actions for dep in a.depends_on),
    )
    for action in actions:
        if (
            action.executor in {"semantic_model", "semantic_catalogue"}
            or action.kind == "publish_registry"
        ):
            assert "upgrade-catalogue" in success.ancestors(action.id)


@weaver_test()
def test_semantic_key_upgrade_preserves_all_installations_in_place(tmp_path):
    from weaver.catalogue.tables import SEMANTIC_TABLES
    from weaver.graph import Graph

    _, repository, bindings, session, state = prepared(tmp_path)
    shape_sql = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = N'_'"
    old_shape = [
        {"TABLE_NAME": table.name, "COLUMN_NAME": table.public_name_of(column.name)}
        for table in CHECKED_TABLES
        for column in table.columns
        if column.name != "table_ordinal"
    ]
    old_shape.extend(
        {"TABLE_NAME": table.name, "COLUMN_NAME": column}
        for table in SEMANTIC_TABLES
        for column in ("Schema name", "Object name")
    )
    session.answer_tsql(shape_sql, old_shape)
    catalogue = read_catalogue_state(catalogue_connection(session), (ITEM,))
    catalogue = Catalogue.from_mapping(catalogue.to_mapping())
    assert set(catalogue.schema_removals) == {
        (table.name, column)
        for table in SEMANTIC_TABLES
        for column in ("Schema name", "Object name")
    }
    assert catalogue.schema_additions == (("SemanticModelTable", "Table ordinal"),)
    bundle = bundle_for(
        tmp_path,
        repository,
        bindings,
        BuildState(catalogue, state.target_inventories),
        "key-upgrade",
    )
    actions = [a for _, _, a in bundle.plan.actions()]
    upgrade = next(a for a in actions if a.id == "upgrade-catalogue")
    sql = bundle.store.read(bundle.location / upgrade.payload).decode()
    assert "DROP TABLE" not in sql
    assert "DELETE FROM" not in sql
    assert "UPDATE " not in sql
    assert "[Table ordinal] bigint NULL" in sql
    for table in SEMANTIC_TABLES:
        assert (
            f"ALTER TABLE [_].[{table.name}] DROP CONSTRAINT [PK_{table.name}]" in sql
        )
        for column in ("Schema name", "Object name"):
            assert f"ALTER TABLE [_].[{table.name}] DROP COLUMN [{column}]" in sql
        keys = ", ".join(f"[{table.public_name_of(key)}]" for key in table.key)
        assert f"PRIMARY KEY NONCLUSTERED ({keys}) NOT ENFORCED" in sql
        # A replay after success must not compile references to dropped columns.
        assert "EXEC(N'" in sql
        assert ": nonempty legacy semantic identity" in sql
        assert ": semantic keys are not unique" in sql
        assert "ROLLBACK" in sql
    success = Graph(
        (a.id for a in actions), ((dep, a.id) for a in actions for dep in a.depends_on)
    )
    for action in actions:
        if (
            action.executor in {"semantic_model", "semantic_catalogue"}
            or action.kind == "publish_registry"
        ):
            assert upgrade.id in success.ancestors(action.id)
