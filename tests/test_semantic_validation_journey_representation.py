"""The validation journey uses Import, separate from generated Direct Lake cases."""

import importlib
from pathlib import Path

from support.weaver_test import weaver_test

from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location
from weaver.semantic_models import TmdlDefinition
from weaver.semantic_models.binding import bind_semantic_sources


@weaver_test()
def test_validation_journey_uses_import_source_tables(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fabric"))
    journey = importlib.import_module("test_semantic_acceptance_journey")
    journey._project(
        tmp_path,
        source_metadata={
            "server": "source.example",
            "database": "Source",
            "schema": "_",
            "object": "TableDictionary",
        },
    )
    parsed = parse_item_repository(Location(tmp_path.as_posix()))
    reference = "Warehouse/_weaver/_.TableDictionary"
    metadata = {
        reference: {
            "reference": reference,
            "server": "source.example",
            "database": "Source",
            "schema": "_",
            "object": "TableDictionary",
            "object_type": "table",
            "source_columns": [
                {"column_name": name, "data_type": "varchar"}
                for name in (
                    "Item type",
                    "Item name",
                    "Schema name",
                    "Object name",
                    "Object type",
                    "Signature",
                )
            ],
        }
    }
    compiled = bind_semantic_sources(parsed, metadata, {journey.ITEM})
    model = TmdlDefinition(compiled.semantic_models[journey.ITEM].parts).model
    assert set(table.name for table in model.tables) == {"Objects", "Reference"}
    assert {
        partition.mode for table in model.tables for partition in table.partitions
    } == {"import"}
    assert model.tables["Objects"].columns["Signature"].isHidden


@weaver_test()
def test_journey_acquires_its_configured_catalogue_through_session(
    tmp_path, monkeypatch
):
    import inspect

    from weaver.sessions.base import Session
    from weaver.workspaces import Workspace

    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fabric"))
    journey = importlib.import_module("test_semantic_acceptance_journey")
    calls = []
    expected = object()

    class RecordingSession:
        workspace = Workspace(workspace="Fixture", catalogue="Warehouse/Catalogue")

        def semantic_source(self, *args, **kwargs):
            inspect.signature(Session.semantic_source).bind(self, *args, **kwargs)
            calls.append((args, kwargs))
            return expected

    session = RecordingSession()
    assert journey._catalogue_source(session) is expected
    assert calls == [
        (
            (session.workspace.catalogue_item,),
            {
                "item_type": "Warehouse",
                "schema": "_",
                "name": "TableDictionary",
                "include_columns": False,
            },
        )
    ]
