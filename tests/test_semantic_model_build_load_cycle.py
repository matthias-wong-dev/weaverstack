"""Build invalidates semantic LoadStatus only when its definition changes."""

import shutil

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import (
    ITEM,
    ROOT,
    answer_catalogue,
    engine_model,
    installed_state,
    project,
    session_for,
)
from test_semantic_model_load_cycle import COMPLETED, REQUEST_ID, answer_installed
from test_semantic_model_rest_boundary import Client, response

import weaver
from weaver.build_bundle.targets import (
    ItemBindings,
    effective_item_bindings,
    parse_build_item,
)
from weaver.build_bundle.workflow import read_target_inventories
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import CATALOGUE_TABLES, LOAD_STATUS
from weaver.declaration.repository import parse_item_repository
from weaver.fabric.semantic_model import SemanticModelClient
from weaver.locations import Location
from weaver.run.record import load_status_row
from weaver.semantic_models.definition import encode_definition


def answer_built_inventory(session, bindings):
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
    return read_target_inventories(bindings, session=session)


@weaver_test()
@pytest.mark.parametrize("pbip", [False, True])
def test_public_build_load_fixed_point_and_changed_definition_cycle(tmp_path, pbip):
    root = project(tmp_path, pbip)
    repository = parse_item_repository(Location(root.as_posix()))
    with session_for() as session:
        definition = session.semantic_model("Reporting_Dev")
        definition.definition = encode_definition(engine_model(repository))
        selector = f"{ITEM}=SemanticModel/Reporting_Dev"
        first = weaver.build(root, items=selector, session=session)
        assert first.succeeded, first.errors
        bindings = effective_item_bindings(
            ItemBindings((parse_build_item(selector),)),
            control_item="Catalogue",
            workspace_name="Demo",
        )
        inventories = answer_built_inventory(session, bindings)
        observed = installed_state(
            repository, bindings, engine_model(repository), inventories
        )
        answer_installed(session, observed.catalogue.rows)
        physical = session.resolve_item("Reporting_Dev", item_type="SemanticModel")
        rest = Client(
            response({}, 202, {"x-ms-request-id": REQUEST_ID}),
            response(COMPLETED),
            response({}, 202, {"x-ms-request-id": REQUEST_ID}),
            response(COMPLETED),
        )
        session.answer_semantic_model(
            physical.workspace_id,
            physical.id,
            SemanticModelClient(
                physical.workspace_id, physical.id, fabric=Client(), power_bi=rest
            ),
        )
        hidden = root.with_name("project-away")
        root.rename(hidden)
        loaded = weaver.load(str(ITEM), session=session)
        hidden.rename(root)
        assert loaded.succeeded and len(loaded.nodes) == 1
        loaded_row = load_status_row(
            loaded.nodes[0], ROOT, workflow_id=loaded.workflow_id
        )
        rows = {item: dict(tables) for item, tables in observed.catalogue.rows.items()}
        rows[ITEM][LOAD_STATUS.name] = (loaded_row,)
        loaded_catalogue = Catalogue(rows)
        answer_catalogue(session, loaded_catalogue, bindings)
        session.calls.clear()
        second = weaver.build(root, items=selector, session=session)
        assert second.succeeded, second.errors
        assert not second.selection.selected_for_build
        assert second.installation_report.action_counts()["total"] == 0
        assert not any(
            "[_].[LoadStatus]" in s and ("DELETE" in s or "MERGE" in s)
            for s in session.tsql
        )

        addon = root / str(ITEM) / "addon.yml"
        addon.write_text(addon.read_text().replace("2026", "2027"), encoding="utf-8")
        changed = parse_item_repository(Location(root.as_posix()))
        definition.definition = encode_definition(engine_model(changed, year=2027))
        session.calls.clear()
        third = weaver.build(root, items=selector, session=session)
        assert third.succeeded and third.selection.selected_for_build == (ROOT,)
        writes = [s for s in session.tsql if "MERGE" in s or "DELETE FROM" in s]
        assert any("[_].[LoadStatus]" in s and "Pending" in s for s in writes)
        assert not any("[_].[Bookmark]" in s for s in writes)
        rebuilt = installed_state(
            changed, bindings, engine_model(changed, year=2027), inventories
        )
        answer_installed(session, rebuilt.catalogue.rows)
        shutil.rmtree(root)
        reloaded = weaver.load(str(ITEM), session=session)
        assert reloaded.succeeded and len(reloaded.nodes) == 1
        assert sum(call[0] == "POST" for call in rest.calls) == 2
        assert not session.python and not session.spark_sql
