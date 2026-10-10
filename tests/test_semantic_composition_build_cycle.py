import builtins
import json

from support.weaver_test import weaver_test
from support.workspaces import InventoryClient
from test_powerbi_project_declaration import write
from test_report_build_cycle import ReportBoundary
from test_semantic_model_build_cycle import DefinitionClient

import weaver
from weaver.fabric.resolution import FabricResolver
from weaver.report_definition import decode_report
from weaver.semantic_models.definition import decode_parts, encode_definition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import TargetDeclaration, Workspace


def variant_project(root):
    write(
        root,
        "PowerBI/Sales/Normal.SemanticModel/definition.pbism",
        '{"version":"4.2","settings":{}}',
    )
    write(
        root,
        "PowerBI/Sales/Normal.SemanticModel/definition/model.tmdl",
        "model Model\n\tculture: en-US\n\tref table Zebra\n\tref table Alpha\n\tannotation ACME.Observe = true\n",
    )
    for name in ("Zebra", "Alpha"):
        write(
            root,
            f"PowerBI/Sales/Normal.SemanticModel/definition/tables/{name}.tmdl",
            f"table {name}\n",
        )
    write(root, "PowerBI/Sales/Normal.tmdl", "table Zebra\n\tmeasure Amount = 1\n")
    write(
        root,
        "PowerBI/policy.tmdl",
        "model Model\n\tculture: en-AU\n\tannotation ACME.Observe = true\n",
    )
    for name, culture in (("Executive", "en-GB"), ("Public", "en-NZ")):
        write(
            root,
            f"PowerBI/Sales/{name}.tmdl",
            f"model Model\n\tannotation Weaver.BaseSemanticModels = Normal\n\tculture: {culture}\n\ntable {name}View\n",
        )
        write(
            root,
            f"PowerBI/Sales/{name}.Report/definition.pbir",
            json.dumps(
                {
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
                    "version": "4.0",
                    "datasetReference": {"byPath": {"path": "../Normal.SemanticModel"}},
                }
            ),
        )
        write(root, f"PowerBI/Sales/{name}.Report/report.json", b"{}\r\n")
    write(
        root,
        "PowerBI/annotations/ACME__Observe.py",
        'import builtins\nfrom weaver.semantic_models import Annotation\nclass ACME__Observe(Annotation):\n    scopes = {"model"}\n    def apply(self, target):\n        builtins._weaver_composition_calls.append((target.culture, tuple(t.name for t in target.tables)))\n',
    )


def variant_session():
    targets = {
        f"SemanticModel/{n}": TargetDeclaration(n + "_Dev")
        for n in ("Normal", "Executive", "Public")
    }
    targets.update(
        {
            f"Report/{n}": TargetDeclaration(n + "_Report_Dev")
            for n in ("Executive", "Public")
        }
    )
    workspace = Workspace(workspace="Demo", targets=targets)
    session = TestSession(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(
            workspace,
            client=InventoryClient(
                "Demo",
                [
                    ("SemanticModel", n + "_Dev")
                    for n in ("Normal", "Executive", "Public")
                ]
                + [("Report", n + "_Report_Dev") for n in ("Executive", "Public")],
            ),
        ),
    )
    models, reports, events = {}, {}, []
    for name, culture in (
        ("Normal", "en-AU"),
        ("Executive", "en-GB"),
        ("Public", "en-NZ"),
    ):
        client = DefinitionClient()
        annotations = [{"name": "ACME.Observe", "value": "true"}]
        if name != "Normal":
            annotations.append({"name": "Weaver.BaseSemanticModels", "value": "Normal"})
        client.definition = encode_definition(
            {
                "model": {
                    "culture": culture,
                    "annotations": annotations,
                    "tables": [
                        {
                            "name": "Zebra",
                            "measures": [{"name": "Amount", "expression": "1"}],
                        },
                        {"name": "Alpha"},
                    ]
                    + ([{"name": name + "View"}] if name != "Normal" else []),
                }
            }
        )
        session.answer_semantic_model("Demo", name + "_Dev", client)
        models[name] = client
    for name in ("Executive", "Public"):
        client = ReportBoundary(events)
        session.answer_report("Demo", name + "_Report_Dev", client)
        reports[name] = client
    return session, models, reports


@weaver_test()
def test_selected_variant_public_build_runs_final_annotations_once_without_base_deployment(
    tmp_path, monkeypatch
):
    calls = []
    monkeypatch.setattr(builtins, "_weaver_composition_calls", calls, raising=False)
    variant_project(tmp_path)
    session, models, reports = variant_session()
    before = {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    for name, culture in (("Executive", "en-GB"), ("Public", "en-NZ")):
        result = weaver.build(
            tmp_path, items=[f"SemanticModel/{name}", f"Report/{name}"], session=session
        )
        assert result.succeeded, result.errors
        assert calls[-1][0] == culture
        assert set(calls[-1][1]) == {"Zebra", "Alpha", name + "View"}
        assert len(calls) == (1 if name == "Executive" else 2)
        assert [call for call, _ in models[name].calls] == [
            "update_definition",
            "invalid_measures",
            "get_definition",
        ]
        assert not models["Normal"].calls
        part = next(
            c[1]["definition"]
            for c in models[name].calls
            if c[0] == "update_definition"
        )
        assert set(decode_parts(part)) >= {
            "definition/tables/Zebra.tmdl",
            "definition/tables/Alpha.tmdl",
            f"definition/tables/{name}View.tmdl",
        }
        reference = json.loads(
            decode_report(reports[name].definition)["definition.pbir"]
        )["datasetReference"]["byConnection"]["connectionString"]
        resolved = session.resolve_item(
            name + "_Dev", item_type="SemanticModel", workspace=session.workspace
        )
        assert resolved.id in reference
    assert before == {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    assert not session.tsql and not session.spark_sql


@weaver_test()
def test_project_and_type_public_build_select_every_named_model(tmp_path, monkeypatch):
    monkeypatch.setattr(builtins, "_weaver_composition_calls", [], raising=False)
    variant_project(tmp_path)
    session, models, reports = variant_session()
    for selector in ("PowerBI/Sales", "PowerBI", "SemanticModel"):
        builtins._weaver_composition_calls.clear()
        result = weaver.build(tmp_path, items=selector, session=session)
        assert result.succeeded, result.errors
        assert len(builtins._weaver_composition_calls) == 3
        assert set(result.items) == {
            "SemanticModel/Normal",
            "SemanticModel/Executive",
            "SemanticModel/Public",
        } | (
            {"Report/Executive", "Report/Public"}
            if selector != "SemanticModel"
            else set()
        )
    assert all(len(client.calls) == 9 for client in models.values())
    assert all(
        client.calls == ["update", "read", "update", "read"]
        for client in reports.values()
    )
