"""A workspace with no Weaver catalogue builds, refreshes and queries a model."""

import json

from support.semantic_fixture_source import ConfiguredSemanticSource
from support.weaver_test import weaver_test
from test_semantic_model_boundary import (
    restored_semantic_model as restored_semantic_model,
)

import weaver


@weaver_test(remote=True, resources={"rest"})
def test_build_and_load_without_a_catalogue(
    restored_semantic_model, semantic_model_session, fixed_semantic_model_name, tmp_path
):
    session = semantic_model_session
    item = f"SemanticModel/{fixed_semantic_model_name}"
    folder = tmp_path / "project" / item
    folder.mkdir(parents=True)
    (folder / f"{folder.name}.tmdl").write_text(
        "model Model\n\tdiscourageImplicitMeasures\n\n"
        'table Calendar\n\tpartition Calendar = calculated\n\t\tsource = ROW("Year", 2026)\n\n'
        "\tmeasure Years = COUNTROWS(Calendar)\n",
        encoding="utf-8",
    )
    source = ConfiguredSemanticSource.capture(restored_semantic_model)
    source.retain(folder)
    assert not session.workspace.catalogue
    built = weaver.build(folder.parent.parent, items=f"{item}={item}", session=session)
    assert built.succeeded, built.errors
    assert built.installation_report.action_counts()["succeeded"] == 2
    loaded = source.run("load", weaver.load, item, session=session)
    assert loaded.succeeded, loaded.to_mapping()
    (node,) = loaded.nodes
    assert node.primitive_kind == "semantic_refresh"
    assert node.result.status == "Completed"
    assert restored_semantic_model.query_dax('EVALUATE ROW("N", [Years])') == [
        {"[N]": 1}
    ]
    print(
        json.dumps(
            {"build": built.to_mapping(), "load": loaded.to_mapping()}, default=str
        )
    )
