"""Authored source lineage orders Load without claiming a physical access path."""

import json

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM, source_project
from test_semantic_source_build_cycle import (
    SubmittedDefinition,
    answer_catalogue,
    capture_publication,
    loadable_source_catalogue,
    source_session,
)

import weaver
from weaver.catalogue.claims import catalogue_columns
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.installed import primitive_candidates
from weaver.load_plan import load_dag


def source_rows(kind):
    if kind == "Warehouse":
        return loadable_source_catalogue()
    item = WeaverItemId.parse("Lakehouse/Serving")
    identity = WeaverDocumentId.parse("Lakehouse/Serving/Tables/Cake.Sales")
    ((_, primitive),) = primitive_candidates(identity, "table")
    schema, name = catalogue_columns(primitive)
    scope = {"item_type": item.item_type, "item_name": item.item_name}
    return Catalogue(
        {
            item: {
                "Installation": ({**scope, "target_name": "Serving_Dev"},),
                "Registry": (
                    {
                        **scope,
                        "schema_name": "Tables/Cake",
                        "object_name": "Sales",
                        "object_type": "table",
                        "object_role": "data",
                        "signature": "source",
                    },
                    {
                        **scope,
                        "schema_name": schema,
                        "object_name": name,
                        "object_type": "file",
                        "object_role": "load",
                        "signature": "load",
                    },
                ),
            }
        }
    )


@pytest.mark.parametrize("kind", ["Warehouse", "Lakehouse"])
@weaver_test()
def test_generated_source_roundtrip_retains_known_physical_barrier(
    tmp_path, monkeypatch, kind
):
    from support.semantic_models import source_model
    from test_semantic_source_build_cycle import SourceSession
    from test_semantic_source_session_boundary import EndpointInventory

    from weaver.build_bundle.targets import (
        ItemBindings,
        effective_item_bindings,
        parse_build_item,
    )
    from weaver.fabric.resolution import FabricResolver
    from weaver.store import FilesystemStore
    from weaver.workspaces import TargetDeclaration, Workspace

    sources = source_rows(kind)
    source = next(iter(sources.rows))
    reference = f"{source}/{'Tables/' if kind == 'Lakehouse' else ''}Cake.Sales"
    root = source_project(tmp_path, value=reference)
    observed = source_model(
        relations={"Sales": "Sales"},
        lakehouse=kind == "Lakehouse",
        descriptions={} if kind == "Lakehouse" else {"Sales": "Sales description"},
        notes=kind == "Warehouse",
    )
    observed["model"]["expressions"][0]["name"] = str(source)
    observed["model"]["tables"][0]["partitions"][0]["source"]["expressionSource"] = str(
        source
    )
    observed["model"]["tables"][0]["annotations"] = [
        {"name": "Weaver.Source", "value": reference}
    ]
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={source: TargetDeclaration("Serving_Dev")},
    )
    inventory = EndpointInventory(
        "Demo",
        [
            (kind, "Serving_Dev"),
            ("Warehouse", "Catalogue"),
            ("SemanticModel", "Reporting_Dev"),
        ],
    )
    bindings = effective_item_bindings(
        ItemBindings(
            tuple(
                parse_build_item(v)
                for v in (
                    f"{ITEM}=SemanticModel/Reporting_Dev",
                    f"{source}={kind}/Serving_Dev",
                )
            )
        ),
        control_item="Catalogue",
        workspace_name="Demo",
    )
    with SourceSession(
        workspace=workspace,
        resolver=FabricResolver(workspace, client=inventory),
        store=FilesystemStore(),
    ) as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, sources, bindings)
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        rows = published()[ITEM]
        (table,) = rows["SemanticModelTable"]
        assert (table["source_mode"], table["source_access"]) == ("directLake", "sql")
        stored = Catalogue.from_mapping(
            json.loads(json.dumps(Catalogue({**sources.rows, ITEM: rows}).to_mapping()))
        )
        dag = stored.dag()
        assert not dag.unresolved
        (edge,) = [e for e in dag.edges if e.downstream.item == ITEM]
        assert str(edge.upstream) == reference and edge.source_access == "sql"
        plan = load_dag(dag, items=(source, ITEM))
        barriers = [
            n
            for n in plan.nodes
            if n.primitive_kind in {"endpoint_refresh", "onelake_publication"}
        ]
        (barrier,) = barriers
        assert barrier.primitive_kind == (
            "onelake_publication" if kind == "Warehouse" else "endpoint_refresh"
        )
        assert barrier.physical_target.kind == kind.lower()
        producer = next(n for n in plan.nodes if str(n.logical_id) == reference)
        consumer = next(n for n in plan.nodes if n.primitive_kind == "semantic_refresh")
        assert producer.node_id in plan.upstream(barrier.node_id)
        assert barrier.node_id in plan.upstream(consumer.node_id)


@pytest.mark.parametrize("kind", ["Warehouse", "Lakehouse"])
@pytest.mark.parametrize("mode", ["directLake", "import", "dual"])
@weaver_test()
def test_authored_source_build_roundtrip_keeps_order_without_physical_barriers(
    tmp_path, monkeypatch, kind, mode
):
    from weaver.build_bundle.targets import (
        ItemBindings,
        effective_item_bindings,
        parse_build_item,
    )

    sources = source_rows(kind)
    source = next(iter(sources.rows))
    reference = f"{source}/{'Tables/' if kind == 'Lakehouse' else ''}Cake.Sales"
    root = source_project(tmp_path)
    path = root / str(ITEM) / "Reporting.tmdl"
    path.write_text(
        "table Sales\n"
        f"\tannotation Weaver.Source = {reference}\n"
        "\tcolumn Value\n\t\tdataType: int64\n"
        f"\tpartition Authored = m\n\t\tmode: {mode}\n"
        '\t\tsource = #table({"Value"}, {{1}})\n'
    )
    original = path.read_bytes()
    observed = {
        "compatibilityLevel": 1606,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "tables": [
                {
                    "name": "Sales",
                    "description": "Sales description" if kind == "Warehouse" else None,
                    "annotations": [{"name": "Weaver.Source", "value": reference}],
                    "columns": [{"name": "Value", "dataType": "int64"}],
                    "partitions": [
                        {
                            "name": "Authored",
                            "mode": mode,
                            "source": {
                                "type": "m",
                                "expression": '#table({"Value"}, {{1}})',
                            },
                        }
                    ],
                }
            ],
        },
    }
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(
            session,
            sources,
            effective_item_bindings(
                ItemBindings(
                    (
                        parse_build_item(f"{ITEM}=SemanticModel/Reporting_Dev"),
                        parse_build_item(f"{source}={kind}/Serving_Dev"),
                    )
                ),
                control_item="Catalogue",
                workspace_name="Demo",
            ),
        )
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        rows = published()[ITEM]
        (dependency,) = rows["Dependency"]
        assert dependency["dependency_reference"] == reference
        (table,) = rows["SemanticModelTable"]
        assert (table["source_mode"], table["source_access"]) == (mode, None)
        stored = Catalogue.from_mapping(
            json.loads(json.dumps(Catalogue({**sources.rows, ITEM: rows}).to_mapping()))
        )
        dag = stored.dag()
        assert not dag.unresolved
        (edge,) = [e for e in dag.edges if e.downstream.item == ITEM]
        assert str(edge.upstream) == reference
        assert (edge.source_mode, edge.source_access) == (mode, None)
        plan = load_dag(dag, items=(source, ITEM))
        assert not any(
            n.primitive_kind in {"endpoint_refresh", "onelake_publication"}
            for n in plan.nodes
        )
        producer = next(n for n in plan.nodes if str(n.logical_id) == reference)
        consumer = next(n for n in plan.nodes if n.primitive_kind == "semantic_refresh")
        assert producer.node_id in plan.upstream(consumer.node_id)
        assert path.read_bytes() == original
