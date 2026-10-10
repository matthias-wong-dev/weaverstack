"""Public semantic wipe, source retention and rebuild on a fixed Fabric item."""

import hashlib
import json

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_public_cycle import _project
from test_semantic_model_public_cycle import ITEM, SCOPE
from test_semantic_model_public_cycle import (
    scratch_build_context as scratch_build_context,
)

import weaver
from weaver.catalogue.reader import read_table
from weaver.catalogue.render import InstallationScope
from weaver.catalogue.tables import (
    CURRENT_STATE_TABLES,
    INSTALLATION,
    LOAD_STATUS,
    PROJECTED_TABLES,
    TABLE_DICTIONARY,
)
from weaver.semantic_models.definition import decode_model
from weaver.semantic_models.wipe import connection_signature


def _fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()


@weaver_test(remote=True, resources={"rest", "tds"})
@pytest.mark.parametrize("preserve", [False, True], ids=["plain", "preserve"])
def test_public_wipe_preserves_item_sources_and_catalogue_then_rebuilds(
    scratch_build_context, tmp_path, preserve
):
    context = scratch_build_context
    folder = tmp_path / "project" / str(ITEM)
    _project(folder, "source-extension")
    project = folder.parent.parent
    selection = f"{ITEM}=SemanticModel/{context.target}"
    built = weaver.build(project, items=selection, session=context.session)
    assert built.succeeded, built.errors
    loaded = context.load(str(ITEM), session=context.session)
    assert loaded.succeeded, loaded.to_mapping()
    assert (
        read_table(context.connection, LOAD_STATUS, scope=SCOPE)[0]["result"]
        == "succeeded"
    )
    model = context.model
    before = decode_model(model.get_definition())
    before_connections = model.get_connections()
    assert len(before_connections) == 1
    assert before_connections[0]["connectivityType"] == "Automatic"
    assert before_connections[0]["connectionDetails"]["type"] == "SQL"
    source_scope = InstallationScope("Warehouse", "_weaver")
    source_before = _fingerprint(
        read_table(context.connection, TABLE_DICTIONARY, scope=source_scope)
    )
    source_installation = read_table(
        context.connection, INSTALLATION, scope=source_scope
    )
    assert source_installation
    item_path = f"workspaces/{model.workspace_id}/items/{model.model_id}"
    item_before = model.fabric.get_json(item_path)
    connections_before = _fingerprint(model.fabric.paged("connections"))
    binding_before = connection_signature(before_connections)
    capture = []
    execute = context.session.execute_mutation

    def observe(plan, payloads=None, **options):
        capture.append(plan)
        return execute(plan, payloads, **options)

    plan = weaver.plan_wipe(
        f"SemanticModel/{context.target}",
        preserve_data_source=preserve,
        session=context.session,
    )
    assert [str(target) for target in plan.targets] == [
        f"SemanticModel/{context.target}"
    ]
    assert plan.catalogue_action == "unbind" and not plan.empties_the_catalogue
    preview = weaver.wipe(plan=plan, dry_run=True, session=context.session)
    assert preview.dry_run
    assert decode_model(model.get_definition()) == before
    assert connection_signature(model.get_connections()) == binding_before
    context.session.execute_mutation = observe
    try:
        result = weaver.wipe(plan=plan, session=context.session)
    finally:
        context.session.execute_mutation = execute
    assert result.emptied == (f"SemanticModel/{context.target}",)
    assert result.unbound["logical_items"] == [str(ITEM)]
    assert len(capture) == 1
    actions = [action for _, _, action in capture[0].actions()]
    assert [a.executor for a in actions] == [
        "tsql_batch",
        "semantic_wipe",
        "tsql_batch",
    ]
    assert capture[0].execution.spark_home_target_id is None
    observed = decode_model(model.get_definition())["model"]
    assert not any(
        observed.get(kind)
        for kind in ("relationships", "roles", "perspectives", "cultures")
    )
    if preserve:
        (anchor,) = observed["tables"]
        assert anchor["name"] == "__WeaverSource" and anchor["isHidden"] is True
        assert (
            not anchor.get("columns")
            and not anchor.get("measures")
            and not anchor.get("hierarchies")
        )
        assert (
            len(anchor["partitions"]) == 1
            and anchor["partitions"][0]["mode"] == "directLake"
        )
        assert connection_signature(model.get_connections()) == binding_before
        assert model.refresh(timeout=300)["status"] == "Completed"
        assert connection_signature(model.get_connections()) == binding_before
    else:
        assert not observed.get("tables") and not observed.get("expressions")
        assert not model.get_connections()
    for table in (*PROJECTED_TABLES, *CURRENT_STATE_TABLES):
        assert not read_table(context.connection, table, scope=SCOPE), table.name
    assert (
        read_table(context.connection, INSTALLATION, scope=source_scope)
        == source_installation
    )
    assert (
        _fingerprint(
            read_table(context.connection, TABLE_DICTIONARY, scope=source_scope)
        )
        == source_before
    )
    item_after = model.fabric.get_json(item_path)
    assert (item_after["id"], item_after["displayName"], item_after["type"]) == (
        item_before["id"],
        item_before["displayName"],
        item_before["type"],
    )
    assert _fingerprint(model.fabric.paged("connections")) == connections_before
    repeated = weaver.wipe(
        f"SemanticModel/{context.target}",
        preserve_data_source=preserve,
        session=context.session,
    )
    assert repeated.emptied == result.emptied
    rebuilt = weaver.build(project, items=selection, session=context.session)
    assert rebuilt.succeeded, rebuilt.errors
    assert (
        read_table(context.connection, LOAD_STATUS, scope=SCOPE)[0]["result"]
        == "pending"
    )
    assert context.load(str(ITEM), session=context.session).succeeded
    assert connection_signature(model.get_connections()) == binding_before
    fixed = weaver.build(project, items=selection, session=context.session)
    assert fixed.succeeded, fixed.errors
    assert not fixed.selection.impact.changed
    print(
        "SEMANTIC_WIPE_EVIDENCE "
        + json.dumps(
            {
                "preserve": preserve,
                "model_id": model.model_id,
                "source_fingerprint": source_before,
                "connection_signature": binding_before,
                "tables_after_wipe": len(observed.get("tables", [])),
                "columns_after_wipe": sum(
                    len(t.get("columns", [])) for t in observed.get("tables", [])
                ),
                "partitions_after_wipe": sum(
                    len(t.get("partitions", [])) for t in observed.get("tables", [])
                ),
                "repeated_wipe": True,
                "rebuild_load_fixed_point": True,
            },
            sort_keys=True,
        )
    )
