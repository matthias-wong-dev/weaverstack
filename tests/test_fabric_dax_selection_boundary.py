"""DAX-dependent Fabric cases have an explicit selection boundary."""

import ast
import tomllib
from pathlib import Path

from support.weaver_test import weaver_test

DAX_CASES = {
    "test_semantic_catalogue_free_cycle.py": {
        "test_build_and_load_without_a_catalogue"
    },
    "test_semantic_direct_lake_cycle.py": {
        "test_generated_direct_lake_build_load_and_both_wipes"
    },
    "test_semantic_model_boundary.py": {
        "test_focused_doctor_uses_only_rest",
        "test_dax_engine_error_is_not_a_successful_empty_result",
    },
    "test_semantic_model_public_cycle.py": {
        "test_pbip_extension_build_load_dax_and_removed_properties",
        "test_existing_warehouse_source_build_persists_lineage_and_loads_without_source",
    },
    "test_semantic_acceptance_journey.py": {
        "test_semantic_model_build_load_test_health_lifecycle"
    },
}


@weaver_test()
def test_dax_selection_marks_only_the_cases_that_execute_dax():
    folder = Path(__file__).parent / "fabric"
    marked = {}
    for path in folder.glob("test_*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and any(ast.unparse(d) == "pytest.mark.dax" for d in node.decorator_list)
        }
        if names:
            marked[path.name] = names
    assert marked == DAX_CASES
    config = tomllib.loads(
        (folder.parent.parent / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert any(
        marker.startswith("dax:")
        for marker in config["tool"]["pytest"]["ini_options"]["markers"]
    )
