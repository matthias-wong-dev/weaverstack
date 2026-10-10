"""Configured semantic fixture guards execute before the first mutation."""

import copy
import json
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.semantic_models.definition import encode_definition, encode_parts

M = 'let\n    Source = Sql.Database("source.example", "Catalogue"),\n    Navigation = Source{[Schema="_", Item="TableDictionary"]}[Data]\nin\n    Navigation'
PARTITION = {
    "name": "CatalogueObjects",
    "mode": "import",
    "source": {"type": "m", "expression": M},
}
PARTS = {
    "definition.pbism": b'{"version":"4.2"}',
    "definition/model.tmdl": b"model Model\n    culture: en-US\n\nref table CatalogueObjects\n",
    "definition/tables/CatalogueObjects.tmdl": (
        "table CatalogueObjects\n    partition CatalogueObjects = m\n        mode: import\n        source =\n"
        + "\n".join("            " + line for line in M.splitlines())
        + "\n"
    ).encode(),
}
BINDING = {
    "id": "configured-connection",
    "connectivityType": "ShareableCloud",
    "connectionDetails": {"type": "SQL", "path": "source.example;Catalogue"},
}


class Model:
    workspace_id = "workspace"
    model_id = "model"

    def __init__(self):
        self.observed = {
            "model": {
                "culture": "en-US",
                "tables": [
                    {
                        "name": "CatalogueObjects",
                        "partitions": [copy.deepcopy(PARTITION)],
                    }
                ],
            }
        }
        self.parts = copy.deepcopy(PARTS)
        self.connections = [copy.deepcopy(BINDING)]
        self.calls = []
        self.fabric = SimpleNamespace(
            request=lambda *a, **k: self.calls.append("request"),
            get_json=lambda path: {
                **copy.deepcopy(BINDING),
                "credentialDetails": {"credentialType": "WorkspaceIdentity"},
                "lastUsedTime": len(self.calls),
            },
        )

    def get_definition(self, *, format=None):
        self.calls.append("native" if format else "observed")
        if format:
            return {**encode_parts(self.parts), "format": "TMDL"}
        return encode_definition(self.observed)

    def get_connections(self):
        self.calls.append("binding")
        return copy.deepcopy(self.connections)


def contract():
    from support.semantic_fixture_source import ConfiguredSemanticSource

    model = Model()
    return model, ConfiguredSemanticSource.capture(model)


def plan(definition=None, *, executor="semantic_model", preserve=True):
    spec = {
        "target_id": "model",
        "definition": definition or encode_parts(PARTS),
        "preserve_data_source": preserve,
    }
    action = SimpleNamespace(executor=executor, payload="definition.json")
    return SimpleNamespace(actions=lambda: [(None, None, action)]), {
        "definition.json": json.dumps(spec).encode()
    }


@weaver_test()
def test_admission_captures_native_source_and_binding_before_dispatch(tmp_path):
    model, source = contract()
    assert model.calls[:3] == ["observed", "native", "binding"]
    calls = []
    session = SimpleNamespace(execute_mutation=lambda *a, **k: calls.append("mutation"))
    source.attach(session)
    p, payloads = plan()
    session.execute_mutation(p, payloads)
    assert calls == ["mutation"] and source.touched
    source.detach(session)


@weaver_test()
@pytest.mark.parametrize("change", ["remove", "server", "navigation", "mode", "m"])
def test_generated_source_change_refused_before_any_action(change):
    model, source = contract()
    parts = copy.deepcopy(PARTS)
    path = "definition/tables/CatalogueObjects.tmdl"
    if change == "remove":
        parts = {
            "definition.pbism": PARTS["definition.pbism"],
            "definition/model.tmdl": b"model Model\n",
        }
    else:
        old, new = {
            "server": (b"source.example", b"other.example"),
            "navigation": (b"TableDictionary", b"Registry"),
            "mode": (b"mode: import", b"mode: directQuery"),
            "m": (b"Navigation\n", b"Table.FirstN(Navigation, 1)\n"),
        }[change]
        parts[path] = parts[path].replace(old, new)
    calls = []
    session = SimpleNamespace(execute_mutation=lambda *a, **k: calls.append("mutation"))
    source.attach(session)
    p, payloads = plan(encode_parts(parts))
    with pytest.raises(AssertionError, match="source"):
        session.execute_mutation(p, payloads)
    assert not calls and not source.touched


@weaver_test()
def test_binding_drift_refused_before_mutation():
    model, source = contract()
    model.connections[0]["id"] = "different"
    calls = []
    session = SimpleNamespace(execute_mutation=lambda *a, **k: calls.append("mutation"))
    source.attach(session)
    p, payloads = plan()
    with pytest.raises(AssertionError, match="binding"):
        session.execute_mutation(p, payloads)
    assert not calls


@weaver_test()
def test_plain_configured_wipe_refused_before_mutation():
    _, source = contract()
    calls = []
    session = SimpleNamespace(execute_mutation=lambda *a, **k: calls.append("mutation"))
    source.attach(session)
    p, payloads = plan(executor="semantic_wipe", preserve=False)
    with pytest.raises(AssertionError, match="preserve_data_source"):
        session.execute_mutation(p, payloads)
    assert not calls and not source.touched


@weaver_test()
def test_retained_carrier_uses_captured_native_m_and_guards_repeat_change_cleanup(
    tmp_path,
):
    model, source = contract()
    folder = tmp_path / "RefreshAcceptance"
    folder.mkdir()
    target = folder / "RefreshAcceptance.tmdl"
    target.write_text(
        'model Model\n\ntable Calendar\n    partition Calendar = calculated\n        source = ROW("Year", 2026)\n'
    )
    source.retain(folder)
    text = target.read_text()
    assert "__WeaverSource" in text
    assert all(line in text for line in M.splitlines())
    parts = {
        "definition.pbism": PARTS["definition.pbism"],
        "definition/model.tmdl": target.read_bytes(),
    }
    calls = []
    session = SimpleNamespace(execute_mutation=lambda *a, **k: calls.append("mutation"))
    source.attach(session)
    for stage in ("initial", "repeat", "changed", "wipe", "rebuild"):
        p, payloads = plan(
            encode_parts(parts),
            executor="semantic_wipe" if stage == "wipe" else "semantic_model",
        )
        session.execute_mutation(p, payloads)
    source.verify("cleanup")
    assert len(calls) == 5
    assert source.evidence[-1]["stage"] == "cleanup"
    assert source.evidence[0]["binding"] == source.evidence[-1]["binding"]
    assert (
        source.evidence[0]["shared_metadata"] != source.evidence[-1]["shared_metadata"]
    )


@weaver_test()
@pytest.mark.parametrize("bad", ["automatic", "path", "native", "unbound"])
def test_unsupported_fixture_admission_has_no_mutation(bad):
    from support.semantic_fixture_source import ConfiguredSemanticSource

    model = Model()
    if bad == "automatic":
        model.connections[0]["connectivityType"] = "Automatic"
    elif bad == "path":
        model.connections[0]["connectionDetails"]["path"] = "different;Catalogue"
    elif bad == "native":
        model.parts["definition/tables/CatalogueObjects.tmdl"] = PARTS[
            "definition/tables/CatalogueObjects.tmdl"
        ].replace(b"source.example", b"other.example")
    else:
        model.connections = []
    with pytest.raises(
        (AssertionError, CommandError), match="source|preserve-data-source"
    ):
        ConfiguredSemanticSource.capture(model)
    assert not any(call == "mutation" for call in model.calls)


@weaver_test()
@pytest.mark.parametrize("status", [403, 500])
def test_unreadable_owner_connection_skips_the_guarded_fixture(tmp_path, status):
    from support.semantic_fixture_source import guarded_source

    from weaver.fabric.client import FabricError

    model = Model()

    def refuse(path):
        raise FabricError("refused", status_code=status)

    model.fabric.get_json = refuse
    session = SimpleNamespace(execute_mutation=lambda *a, **k: None)
    execute = session.execute_mutation
    expected = pytest.skip.Exception if status == 403 else FabricError
    with pytest.raises(expected) as raised:
        with guarded_source(
            model, session, tmp_path / "backup.json", lambda value: None, name="Sales"
        ):
            pytest.fail("the guarded fixture yielded")
    if status == 403:
        assert str(raised.value) == (
            "Sales's connection is not readable by this sign-in; "
            "sign in as the model's owner"
        )
    assert session.execute_mutation is execute
    assert "request" not in model.calls
    assert not (tmp_path / "backup.json").exists()


@weaver_test()
def test_restoration_replays_only_captured_native_source_and_owned_state(monkeypatch):
    from weaver.semantic_models.definition import decode_parts

    model, source = contract()
    calls = []
    model.update_definition = lambda definition, **options: calls.append(
        (definition, options)
    )
    model.refresh = lambda **options: calls.append(("refresh", options))
    source.restore(lambda value: calls.append(("settle", value.model_id)))
    assert not calls
    source.touched = True
    source.restore(lambda value: calls.append(("settle", value.model_id)))
    assert calls[0] == ("settle", "model")
    assert decode_parts(calls[1][0]) == PARTS
    assert calls[1][1] == {"allow_purge_data": True, "timeout": 300}
    assert calls[2] == ("refresh", {"timeout": 300})
    assert source.evidence[-1]["stage"] == "cleanup"


@weaver_test()
def test_source_drift_blocks_restoration_before_definition_write():
    model, source = contract()
    model.observed["model"]["tables"][0]["partitions"][0]["source"]["expression"] = (
        M.replace("TableDictionary", "Registry")
    )
    source.touched = True
    calls = []
    model.update_definition = lambda *a, **k: calls.append("mutation")
    with pytest.raises(AssertionError, match="source"):
        source.restore(lambda value: calls.append("settle"))
    assert not calls


@weaver_test()
def test_public_fixture_wipe_forwards_preservation_and_session(monkeypatch):
    import weaver

    _, source = contract()
    calls = []
    session = object()
    monkeypatch.setattr(weaver, "wipe", lambda *a, **k: calls.append((a, k)))
    source.wipe("SemanticModel/Configured", session=session)
    assert calls == [
        (
            ("SemanticModel/Configured",),
            {"preserve_data_source": True, "session": session},
        )
    ]
    with pytest.raises(AssertionError, match="preserve_data_source"):
        source.wipe(
            "SemanticModel/Configured", session=session, preserve_data_source=False
        )
    assert len(calls) == 1


@weaver_test()
def test_reshaping_cycles_use_the_scratch_model_not_the_configured_one(monkeypatch):
    """The guard refuses these shapes, so they run where nothing is guarded."""

    import importlib
    import inspect
    from pathlib import Path

    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fabric"))
    reshaping = {
        "test_semantic_direct_lake_cycle": (
            "test_generated_direct_lake_build_load_and_both_wipes"
        ),
        "test_semantic_model_public_cycle": (
            "test_existing_warehouse_source_build_persists_lineage_and_loads_without_source"
        ),
    }
    for module, name in reshaping.items():
        test = getattr(importlib.import_module(module), name)
        parameters = inspect.signature(test).parameters
        assert "scratch_build_context" in parameters, name
        assert "semantic_build_context" not in parameters, name


@weaver_test()
def test_journey_copies_native_source_and_checks_physical_metadata(
    tmp_path, monkeypatch
):
    import importlib
    from pathlib import Path

    from weaver.semantic_models import TmdlDefinition

    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fabric"))
    journey = importlib.import_module("test_semantic_acceptance_journey")
    _, source = contract()
    metadata = {
        "server": "source.example",
        "database": "Catalogue",
        "schema": "_",
        "object": "TableDictionary",
    }
    source.require_metadata(metadata)
    folder, _ = journey._project(
        tmp_path, source_metadata=metadata, native_source=source.expression
    )
    model = TmdlDefinition(
        {
            "definition/model.tmdl": (
                folder / f"{journey.ITEM.item_name}.tmdl"
            ).read_bytes()
        }
    ).model
    assert {
        partition.source.strip()
        for table in model.tables
        for partition in table.partitions
    } == {M}
    with pytest.raises(AssertionError, match="source"):
        source.require_metadata({**metadata, "object": "Registry"})


@weaver_test()
def test_generated_definition_guard_precedes_catalogue_actions():
    _, source = contract()
    calls = []
    session = SimpleNamespace(
        execute_mutation=lambda *a, **k: calls.append("catalogue-write")
    )
    source.attach(session)
    p, payloads = plan(
        encode_parts(
            {
                "definition.pbism": PARTS["definition.pbism"],
                "definition/model.tmdl": b"model Model\n",
            }
        )
    )
    semantic = list(p.actions())[0]
    p.actions = lambda: [
        (None, None, SimpleNamespace(executor="tsql_batch", payload=None)),
        semantic,
    ]
    with pytest.raises(AssertionError, match="source"):
        session.execute_mutation(p, payloads)
    assert not calls


@weaver_test()
def test_load_cannot_repair_a_lost_configured_binding_with_a_new_client():
    from weaver.fabric.semantic_model import SemanticModelClient

    model, source = contract()
    writes = []
    model.fabric.request = lambda *a, **k: writes.append((a, k))
    get_json = model.fabric.get_json
    model.fabric.get_json = lambda path: (
        {"value": [BINDING]} if path == "connections" else get_json(path)
    )
    session = SimpleNamespace(execute_mutation=lambda *a, **k: None)
    source.attach(session)
    client = SemanticModelClient(
        model.workspace_id, model.model_id, fabric=model.fabric, power_bi=object()
    )
    client.data_sources = lambda: [
        {
            "datasourceType": "Sql",
            "connectionDetails": {"server": "source.example", "database": "Catalogue"},
        }
    ]
    with pytest.raises(AssertionError, match="rebind"):
        client.bind_data_sources()
    assert not writes
    source.detach(session)


@weaver_test()
def test_direct_definition_client_cannot_remove_configured_source():
    from weaver.fabric.semantic_model import SemanticModelClient

    model, source = contract()
    writes = []
    model.fabric.request = lambda *a, **k: writes.append((a, k))
    model.fabric.wait_for_operation = lambda *a, **k: {}
    source.attach(SimpleNamespace(execute_mutation=lambda *a, **k: None))
    client = SemanticModelClient(
        model.workspace_id, model.model_id, fabric=model.fabric, power_bi=object()
    )
    definition = encode_parts(
        {
            "definition.pbism": PARTS["definition.pbism"],
            "definition/model.tmdl": b"model Model\n",
        }
    )
    with pytest.raises(AssertionError, match="source"):
        client.update_definition(definition, allow_purge_data=True, timeout=300)
    assert not writes
    assert not source.touched


@weaver_test()
def test_restored_fixture_admission_and_refusal_do_not_write_during_teardown(
    tmp_path, monkeypatch
):
    import importlib
    from pathlib import Path

    from weaver.fabric.semantic_model import SemanticModelClient

    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fabric"))
    boundary = importlib.import_module("test_semantic_model_boundary")
    monkeypatch.setattr(boundary, "_settle_refreshes", lambda model: None)
    model = Model()
    writes = []
    model.fabric.request = lambda *a, **k: writes.append((a, k))
    model.fabric.wait_for_operation = lambda *a, **k: {}
    session = SimpleNamespace(
        execute_mutation=lambda *a, **k: writes.append("mutation")
    )
    fixture = boundary.restored_semantic_model.__wrapped__(model, session, tmp_path)
    assert next(fixture) is model
    assert model.calls[:3] == ["observed", "native", "binding"]
    client = SemanticModelClient(
        model.workspace_id, model.model_id, fabric=model.fabric, power_bi=object()
    )
    with pytest.raises(AssertionError, match="source"):
        client.update_definition(
            encode_parts(
                {
                    "definition.pbism": PARTS["definition.pbism"],
                    "definition/model.tmdl": b"model Model\n",
                }
            ),
            allow_purge_data=True,
        )
    with pytest.raises(StopIteration):
        next(fixture)
    assert not writes
