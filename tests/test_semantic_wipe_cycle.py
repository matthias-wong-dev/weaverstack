"""Semantic wipe crosses the shared mutation executor and verifies native readback."""

import copy

import pytest
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient

from weaver import wipe
from weaver.fabric.resolution import FabricResolver
from weaver.semantic_models.definition import encode_definition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import Workspace


class RecordedModel:
    def __init__(self, item):
        self.workspace_id = item.workspace_id
        self.model_id = item.id
        self.calls = []
        self.updated = False
        self.before = {"model": {"culture": "en-US", "tables": [{"name": "Old"}]}}
        self.after = {"model": {"culture": "en-US"}}
        self.native = None
        self.connections = []

    def get_definition(self, **options):
        self.calls.append(("read", options))
        if options.get("format") == "TMDL":
            return self.native
        return encode_definition(self.after if self.updated else self.before)

    def get_connections(self):
        self.calls.append(("connections", None))
        return self.connections

    def update_definition(self, definition, **options):
        self.calls.append(("update", {"definition": definition, **options}))
        self.updated = True
        return {"status": "Succeeded"}


def setup(catalogue=None):
    workspace = Workspace(workspace="Demo", catalogue=catalogue)
    inventory = InventoryClient(
        "Demo",
        [
            ("SemanticModel", "Reporting"),
            ("Warehouse", "Reporting"),
            ("Warehouse", "Weaver"),
        ],
    )
    session = TestSession(
        workspace=workspace,
        resolver=FabricResolver(workspace, client=inventory),
        store=FilesystemStore(),
    )
    item = session.resolve_item("Reporting", item_type="SemanticModel")
    client = RecordedModel(item)
    session.answer_semantic_model("Demo", "Reporting", client)
    return session, client


@pytest.mark.parametrize("decoded", [False, True], ids=["direct", "format5"])
@weaver_test()
def test_plain_public_wipe_is_one_bound_rest_action_with_verified_readback(decoded):
    session, client = setup()
    captured = []
    execute = session.execute_mutation

    def observe(plan, payloads):
        if decoded:
            from weaver.mutation.bundle import plan_from_yaml, plan_to_yaml

            before = plan.to_mapping()
            plan = plan_from_yaml(plan_to_yaml(plan))
            assert plan.to_mapping() == before
        captured.append((plan, payloads))
        return execute(plan, payloads)

    session.execute_mutation = observe
    result = wipe("SemanticModel/Reporting", session=session)

    assert result.emptied == ("SemanticModel/Reporting",)
    assert len(captured) == 1
    plan, payloads = captured[0]
    actions = [action for _, _, action in plan.actions()]
    assert len(actions) == 1 and actions[0].executor == "semantic_wipe"
    assert plan.targets[0].kind == "semanticmodel"
    assert plan.targets[0].item_id == client.model_id
    assert plan.execution.spark_home_target_id is None
    updates = [value for name, value in client.calls if name == "update"]
    assert len(updates) == 1 and updates[0]["definition"]["format"] == "TMDL"
    assert updates[0]["allow_purge_data"] is True
    assert client.calls[-2][0] == "read" and client.calls[-1][0] == "connections"
    assert result.reports[0].location.value == "semanticmodel://Reporting"
    assert result.items[0].counts == {"entries": 1}
    assert not any(
        call.kind in {"spark_sql", "query_tsql", "execute_tsql_script"}
        for call in session.calls
    )


@weaver_test()
def test_public_dry_run_reports_semantic_content_without_dispatch():
    session, client = setup()
    result = wipe("SemanticModel/Reporting", session=session, dry_run=True)
    assert result.dry_run and not client.updated
    assert result.reports[0].dry_run
    assert result.reports[0].location.value == "semanticmodel://Reporting"
    assert result.reports[0].removed == ("tables/Old",)
    assert not any(call.kind == "execute_mutation" for call in session.calls)


@weaver_test()
def test_public_preserve_path_keeps_the_verified_source_shell():
    from test_semantic_wipe_representation import _preserved_inputs

    from weaver.semantic_models.definition import encode_parts

    session, client = setup()
    original, parts, connections = _preserved_inputs()
    client.before, client.native, client.connections = (
        original,
        encode_parts(parts),
        connections,
    )
    partition = copy.deepcopy(original["model"]["tables"][0]["partitions"][0])
    partition["name"] = "Source"
    client.after = {
        "model": {
            "culture": "en-US",
            "expressions": original["model"]["expressions"],
            "tables": [
                {"name": "__WeaverSource", "isHidden": True, "partitions": [partition]}
            ],
        }
    }
    result = wipe("SemanticModel/Reporting", session=session, preserve_data_source=True)
    assert result.plan.preserve_data_source
    assert result.emptied == ("SemanticModel/Reporting",)
    assert any(
        options.get("format") == "TMDL"
        for name, options in client.calls
        if name == "read"
    )
    assert not any(
        value.startswith("expressions/") for value in result.reports[0].removed
    )


@weaver_test()
def test_readback_failure_is_a_failed_wipe_not_an_emptied_result():
    from weaver.errors import CommandError

    session, client = setup()
    client.after = client.before
    with pytest.raises(CommandError, match="wipe did not complete.*readback"):
        wipe("SemanticModel/Reporting", session=session)
    assert client.updated


@weaver_test()
def test_unsupported_preservation_refuses_before_any_target_mutates():
    from test_semantic_wipe_representation import _preserved_inputs

    from weaver.errors import CommandError
    from weaver.semantic_models.definition import encode_parts

    session, client = setup()
    original, parts, connections = _preserved_inputs()
    connections[0]["connectivityType"] = "ShareableCloud"
    client.before, client.native, client.connections = (
        original,
        encode_parts(parts),
        connections,
    )
    with pytest.raises(CommandError, match="preserve-data-source"):
        wipe(
            ("Warehouse/Reporting", "SemanticModel/Reporting"),
            session=session,
            preserve_data_source=True,
        )
    assert not client.updated
    assert not any(call.kind == "execute_mutation" for call in session.calls)


@pytest.mark.parametrize("fail_readback", [False, True])
@weaver_test()
def test_catalogue_certification_is_removed_before_semantic_reset(fail_readback):
    from support.semantic_wipe import catalogue_answers

    from weaver.errors import CommandError

    session, client = setup(catalogue="Warehouse/Weaver")
    rows = [
        {
            "item_type": "SemanticModel",
            "item_name": "LogicalModel",
            "target_name": "Reporting",
        },
        {"item_type": "Warehouse", "item_name": "Source", "target_name": "Reporting"},
    ]
    for statement, answer in catalogue_answers(rows).items():
        session.answer_tsql(statement, answer)
    before_update = []
    update = client.update_definition

    def observe(definition, **options):
        before_update.extend(session.tsql)
        return update(definition, **options)

    client.update_definition = observe
    if fail_readback:
        client.after = client.before
        with pytest.raises(CommandError, match="wipe did not complete.*readback"):
            wipe("SemanticModel/Reporting", session=session)
    else:
        result = wipe("SemanticModel/Reporting", session=session)
        assert str(result.items[-1].target) == "Warehouse/Weaver"
        assert result.items[-1].outcome == "preserved"
        assert result.unbound["logical_items"] == ["SemanticModel/LogicalModel"]
    for table in ("Registry", "LoadStatus"):
        assert any(
            statement.startswith(f"DELETE FROM [_].[{table}]")
            and "N'SemanticModel'" in statement
            and "N'LogicalModel'" in statement
            for statement in before_update
        )
    assert not any(
        "DROP " in statement or "N'Source'" in statement for statement in session.tsql
    )


@pytest.mark.parametrize("drift", ["definition", "connection"])
@weaver_test()
def test_model_drift_after_preparation_refuses_before_reset(drift):
    from weaver.errors import CommandError

    session, client = setup()
    execute = session.execute_mutation

    def changed(plan, payloads):
        if drift == "definition":
            client.before["model"]["description"] = "Another deployment"
        else:
            client.connections = [
                {"connectivityType": "ShareableCloud", "id": "different"}
            ]
        return execute(plan, payloads)

    session.execute_mutation = changed
    with pytest.raises(CommandError, match="changed after wipe preparation"):
        wipe("SemanticModel/Reporting", session=session)
    assert not client.updated
