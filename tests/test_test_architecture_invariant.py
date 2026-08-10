"""The test suite says what each module proves in the module name.

New and renamed modules use ``test_<subject>_<claim>.py``. Existing modules are
grandfathered explicitly and classified here; an exception cannot appear merely
because somebody added a file. Rename work removes entries from this map.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
CLAIMS = frozenset(
    {
        "declaration",
        "representation",
        "boundary",
        "install",
        "primitive",
        "cycle",
        "invariant",
        "journey",
    }
)

#: The claims this taxonomy replaced. No module carries one any more, and the
#: rule below is what keeps it that way: ``lifecycle`` became ``cycle``, because
#: it was used for everything from a state machine to an end-to-end run;
#: ``render`` and ``binding`` are both ``representation``, unless physical access
#: is the claim, in which case they are ``boundary``.
RETIRED_CLAIMS = frozenset({"render", "binding", "lifecycle"})

MODULE_NAME = re.compile(rf"^test_.+_(?P<claim>{'|'.join(sorted(CLAIMS))})\.py$")
RETIRED_NAME = re.compile(
    rf"^test_.+_(?P<claim>{'|'.join(sorted(RETIRED_CLAIMS))})\.py$"
)


def _classified(claim: str, paths: str) -> dict[str, str]:
    assert claim in CLAIMS
    return {path: claim for path in paths.split()}


# The formerly undifferentiated root layer, now classified even before its files
# are renamed. These are the four jobs described by the architecture handover:
# contracts, rendered output, logical/physical binding, and enforced invariants.
LEGACY_ROOT_CLAIMS = {
    **_classified(
        "declaration",
        """
        tests/test_catalogue_tables.py
        tests/test_cli.py
        tests/test_config.py
        tests/test_declaration_dependencies.py
        tests/test_declaration_dependencies_spark.py
        tests/test_declaration_dependencies_tsql.py
        tests/test_declaration_estate_end_to_end.py
        tests/test_declaration_graph.py
        tests/test_declaration_logical_keys.py
        tests/test_declaration_metadata.py
        tests/test_declaration_references.py
        tests/test_declaration_reserved_schema.py
        tests/test_declaration_schemas.py
        tests/test_diagnostics.py
        tests/test_environment_definition.py
        tests/test_fabric_item_creation.py
        tests/test_fabric_session.py
        tests/test_fabric_tokens.py
        tests/test_item_dependencies.py
        tests/test_item_graph.py
        tests/test_item_repository.py
        tests/test_lakehouse.py
        tests/test_livy_environment.py
        tests/test_local_session.py
        tests/test_locations.py
        tests/test_objects.py
        tests/test_resolution.py
        tests/test_sql_authentication.py
        tests/test_sql_pool.py
        tests/test_sql_resolution.py
        tests/test_store.py
        tests/test_targets.py
        tests/test_weaver_model.py
        tests/test_workspaces.py
        """,
    ),
    **_classified(
        "representation",
        """
        tests/test_build_bundle_model.py
        tests/test_build_stages.py
        tests/test_declaration_build_columns.py
        tests/test_declaration_create_ddl.py
        tests/test_declaration_tsql_ddl.py
        tests/test_sql_shaping.py
        tests/test_warehouse_wipe_sql.py
        """,
    ),
    **_classified(
        "boundary",
        """
        tests/test_alias_execution.py
        tests/test_build_installer.py
        tests/test_fabric_alias_transport.py
        tests/test_folder_executor.py
        tests/test_lakehouse_spark_location.py
        tests/test_livy_plumbing.py
        tests/test_onelake_pagination.py
        tests/test_spark_destination.py
        tests/test_spark_sql_executor.py
        tests/test_spark_table_executor.py
        tests/test_sql_endpoint_refresh.py
        tests/test_sql_execution.py
        tests/test_tsql_executor.py
        tests/test_wipe_shortcuts.py
        """,
    ),
    **_classified(
        "representation",
        """
        tests/test_catalogue_state.py
        tests/test_cli_build.py
        tests/test_cli_wipe.py
        tests/test_item_catalogue.py
        """,
    ),
    **_classified(
        "install",
        """
        tests/test_incremental_build.py
        tests/test_item_build_planner.py
        """,
    ),
    **_classified(
        "cycle",
        """
        tests/test_item_build_workflow.py
        tests/test_push.py
        tests/test_unbind.py
        tests/test_wipe.py
        """,
    ),
    **_classified(
        "invariant",
        """
        tests/test_neutrality.py
        tests/test_public_api.py
        """,
    ),
}


# Nested legacy modules are just as explicit: directories describe the cost or
# fixture family, never the claim. New files in these directories still have to
# carry a claim suffix because this is a closed list.
LEGACY_NESTED_CLAIMS = {
    **_classified(
        "declaration",
        """
        tests/fabric/test_livy_protocol.py
        tests/spark/test_local_lakehouses.py
        tests/targeted/test_load_contract.py
        tests/targeted/test_load_result.py
        """,
    ),
    **_classified(
        "representation",
        """
        tests/targeted/test_catalogue_diff.py
        tests/targeted/test_catalogue_projection.py
        tests/targeted/test_catalogue_publication.py
        tests/targeted/test_document_action.py
        tests/targeted/test_inventory_prune.py
        """,
    ),
    **_classified(
        "boundary",
        """
        tests/fabric/test_alias_discovery.py
        tests/fabric/test_item_catalogue_fabric.py
        tests/spark/boundary/test_catalogue_fidelity.py
        tests/spark/boundary/test_inventory_fidelity.py
        tests/spark/test_cross_item_alias_incremental.py
        """,
    ),
    **_classified(
        "install",
        """
        tests/targeted/test_alias_planning.py
        tests/targeted/test_incremental_impact.py
        tests/targeted/test_item_plan.py
        tests/targeted/test_prune.py
        tests/targeted/test_reconciliation.py
        tests/targeted/test_schema_stage.py
        """,
    ),
    **_classified(
        "primitive",
        """
        tests/fabric/test_authored_object_attachment.py
        tests/fabric/test_cross_item_alias.py
        tests/fabric/test_livy_import.py
        tests/fabric/test_onelake_store.py
        tests/fabric/test_onelake_wipe.py
        tests/fabric/test_published_weaver.py
        tests/fabric/test_shared_wipe.py
        tests/fabric/test_warehouse_wipe.py
        tests/spark/boundary/test_actions_delta.py
        tests/spark/test_authored_objects.py
        tests/spark/test_diagnostics_session.py
        tests/spark/test_local_aliases.py
        tests/spark/test_local_wipe.py
        tests/spark/test_sql_table_build.py
        tests/spark/test_wipe_delta.py
        """,
    ),
    **_classified(
        "cycle",
        """
        tests/fabric/test_fabric_resources.py
        tests/spark/test_catalogue_builtin_build.py
        tests/spark/test_catalogue_initialise.py
        tests/spark/test_item_catalogue_build.py
        tests/spark/test_mixed_estate.py
        tests/spark/test_multi_destination.py
        tests/targeted/test_build_workflow.py
        """,
    ),
    **_classified(
        "invariant",
        """
        tests/fabric/test_onelake_mount_contract.py
        tests/support/test_livy_telemetry.py
        tests/support/test_observation.py
        tests/targeted/test_build_intent.py
        tests/targeted/test_executor_parity.py
        """,
    ),
}

GRANDFATHERED = LEGACY_ROOT_CLAIMS | LEGACY_NESTED_CLAIMS

def _test_modules() -> set[str]:
    return {
        path.relative_to(ROOT).as_posix()
        for path in TESTS.rglob("test_*.py")
    }


def test_every_test_module_names_a_claim_or_is_explicitly_grandfathered():
    unnamed = {
        path
        for path in _test_modules()
        if not MODULE_NAME.fullmatch(Path(path).name) and path not in GRANDFATHERED
    }

    assert not unnamed, (
        "test modules must be named test_<subject>_<claim>.py with a claim from "
        f"{sorted(CLAIMS)}; classify an existing module deliberately before "
        f"grandfathering it: {sorted(unnamed)}"
    )


def test_every_grandfathered_module_exists_and_still_needs_its_exception():
    modules = _test_modules()
    missing = set(GRANDFATHERED) - modules
    renamed_but_not_removed = {
        path
        for path in GRANDFATHERED
        if path in modules and MODULE_NAME.fullmatch(Path(path).name)
    }

    assert not missing, f"remove stale grandfather entries: {sorted(missing)}"
    assert not renamed_but_not_removed, (
        "these modules now name their claim; remove their grandfather entries: "
        f"{sorted(renamed_but_not_removed)}"
    )


def test_no_module_takes_a_claim_the_taxonomy_retired():
    """The migration is finished, and finished is a thing a test can hold.

    Without this the old words come back one module at a time, and a suite with
    two taxonomies is a suite with none — a reader cannot tell whether
    ``binding`` means the old undifferentiated sense or somebody's shorthand for
    one of the two words that replaced it.
    """

    named = sorted(
        path for path in _test_modules() if RETIRED_NAME.fullmatch(Path(path).name)
    )

    assert not named, (
        "these claims were retired — lifecycle is now cycle, and render and "
        "binding are representation or boundary depending on whether the claim "
        f"involves physical access: {named}"
    )


def test_every_legacy_root_module_has_a_decided_claim():
    legacy_root = {
        path
        for path in _test_modules()
        if Path(path).parent == Path("tests")
        and not MODULE_NAME.fullmatch(Path(path).name)
    }

    assert legacy_root == set(LEGACY_ROOT_CLAIMS)
