"""Older catalogues gain semantic identity columns without losing state."""

from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import ITEM, bundle_for, prepared

from weaver.build_bundle.workflow import BuildState
from weaver.catalogue.connection import catalogue_connection
from weaver.catalogue.state import CHECKED_TABLES, Catalogue, read_catalogue_state
from weaver.catalogue.tables import INSTALLATION


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
