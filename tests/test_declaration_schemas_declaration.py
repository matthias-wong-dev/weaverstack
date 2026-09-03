"""Schemas an item owns: implied by its identities, described by an optional file."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import (
    SchemaSes,
    inferred_schema,
    parse_item_repository,
    parse_schema_document,
    read_schema_document,
)
from weaver.errors import DiscoveryError
from weaver.locations import Location


def parse(text: str, path: str = "schemas/Sales.yml") -> SchemaSes:
    return parse_schema_document(textwrap.dedent(text), path)


# --- parsing -----------------------------------------------------------------


@weaver_test()
def test_a_minimal_schema_declares_only_its_id():
    schema = parse("Schema ID: Sales")
    assert schema.schema_id == "Sales"
    assert schema.description is None


@weaver_test()
def test_a_schema_carries_a_multiline_description():
    schema = parse(
        """
        Schema ID: Sales

        Description: |
          Curated sales objects,
          across two lines.
        """
    )
    assert schema.description == "Curated sales objects,\nacross two lines."


@weaver_test()
def test_a_missing_id_is_refused():
    with pytest.raises(DiscoveryError, match="Schema ID is required"):
        parse("Description: no id here")


@weaver_test()
def test_a_blank_id_is_refused():
    with pytest.raises(DiscoveryError, match="Schema ID is required"):
        parse("Schema ID: '   '")


@weaver_test()
def test_a_dotted_id_is_refused():
    with pytest.raises(DiscoveryError, match="single bare name"):
        parse("Schema ID: Sales.Order")


@weaver_test()
def test_an_unknown_key_is_refused():
    with pytest.raises(DiscoveryError, match="unknown schema key"):
        parse("Schema ID: Sales\nColour: blue")


@weaver_test()
def test_a_non_mapping_is_refused():
    with pytest.raises(DiscoveryError, match="must be a YAML mapping"):
        parse("- just a list")


# --- filename identity -------------------------------------------------------


@weaver_test()
def test_the_filename_must_match_the_id():
    with pytest.raises(DiscoveryError, match="match exactly"):
        read_schema_document("_schemas/Sales.yml", b"Schema ID: Reporting")


@weaver_test()
def test_the_filename_match_is_case_sensitive():
    with pytest.raises(DiscoveryError, match="match exactly"):
        read_schema_document("_schemas/sales.yml", b"Schema ID: Sales")


@weaver_test()
def test_a_matching_filename_reads():
    schema = read_schema_document("_schemas/Sales.yml", b"Schema ID: Sales")
    assert schema.schema_id == "Sales"


@weaver_test()
def test_an_inferred_schema_is_an_ordinary_schema():
    """One representation, so the repository and the catalogue read one kind."""

    schema = inferred_schema("Sales")

    assert isinstance(schema, SchemaSes)
    assert schema.schema_id == "Sales"
    assert schema.description is None
    assert schema.raw == {}
    assert not schema.is_explicit
    # Deterministic and specific to the name, so the catalogue records a
    # signature that moves when the schema does and not when an object does.
    assert schema.source_hash == inferred_schema("Sales").source_hash


# --- schema declaration across a repository ----------------------------------


PY_TABLE = '''"""
Table ID: {schema}.Thing

Description: A thing.

Lineage: Upstream.

Primary key: Id

Schema:
  Id: string
"""

from weaver import Table


class {schema}__Thing(Table):
    def read(self):
        return [], []
'''


ITEM = "Lakehouse/Raw"


def build(tmp_path: Path, *, schemas: list[str], objects: dict[str, str]) -> Location:
    directory = tmp_path / ITEM / "schemas"
    directory.mkdir(parents=True)
    for schema in schemas:
        (directory / f"{schema}.yml").write_text(
            f"Schema ID: {schema}\n", encoding="utf-8"
        )
    tables = tmp_path / ITEM / "Tables"
    tables.mkdir(parents=True, exist_ok=True)
    for name, text in objects.items():
        (tables / name).write_text(textwrap.dedent(text), encoding="utf-8")
    return Location(str(tmp_path))


def _schema(repository, item: str, name: str) -> SchemaSes:
    """One item-owned schema, by the name the repository holds it under."""

    return next(
        document
        for identity, document in repository.schema_documents.items()
        if str(identity.item) == item and identity.schema == name
    )


@weaver_test()
def test_a_managed_object_implies_its_schema(tmp_path):
    """The declaration Weaver needs is the object; the schema follows from it."""

    root = build(
        tmp_path,
        schemas=[],
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )

    repository = parse_item_repository(root)

    assert f"{ITEM}/Tables/Widget.Thing" in repository.dependency_graph.nodes
    assert _schema(repository, ITEM, "Widget").schema_id == "Widget"


@weaver_test()
def test_an_implied_schema_carries_no_description(tmp_path):
    root = build(
        tmp_path,
        schemas=[],
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )

    schema = _schema(parse_item_repository(root), ITEM, "Widget")

    assert schema.description is None
    assert schema.relative_path is None
    assert not schema.is_explicit
    assert schema.source_hash


@weaver_test()
def test_a_schema_file_describes_the_same_schema(tmp_path):
    """The file is metadata for the schema the object already established."""

    directory = tmp_path / ITEM / "schemas"
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={"Sales__Thing.py": PY_TABLE.format(schema="Sales")},
    )
    (directory / "Sales.yml").write_text(
        "Schema ID: Sales\nDescription: Sales and order processing.\n",
        encoding="utf-8",
    )

    repository = parse_item_repository(root)
    owned = [
        identity
        for identity in repository.schema_documents
        if str(identity.item) == ITEM and identity.schema == "Sales"
    ]
    schema = _schema(repository, ITEM, "Sales")

    assert len(owned) == 1, "the file describes one schema; it does not add a second"
    assert schema.description == "Sales and order processing."
    assert schema.is_explicit


@weaver_test()
def test_an_implied_schema_signature_ignores_the_object_that_implied_it(tmp_path):
    """Editing a table is a change to the table, not to its schema's metadata."""

    first = build(
        tmp_path / "a",
        schemas=[],
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )
    second = build(
        tmp_path / "b",
        schemas=[],
        objects={
            "Widget__Thing.py": PY_TABLE.format(schema="Widget").replace(
                "A thing.", "A revised thing."
            )
        },
    )

    assert _schema(parse_item_repository(first), ITEM, "Widget").source_hash == (
        _schema(parse_item_repository(second), ITEM, "Widget").source_hash
    )
    assert inferred_schema("Widget").source_hash != inferred_schema("Sales").source_hash


@weaver_test()
def test_a_declared_schema_lets_the_object_read(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={"Sales__Thing.py": PY_TABLE.format(schema="Sales")},
    )
    assert (
        f"{ITEM}/Tables/Sales.Thing"
        in parse_item_repository(root).dependency_graph.nodes
    )


@weaver_test()
def test_an_unused_schema_is_still_valid(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales", "Unused"],
        objects={"Sales__Thing.py": PY_TABLE.format(schema="Sales")},
    )
    repo = parse_item_repository(root)
    declared = {schema.schema for schema in repo.schema_documents}
    assert "Unused" in declared


@weaver_test()
def test_an_implied_and_a_declared_spelling_may_not_differ_by_case(tmp_path):
    """Schema identity is exact, so the two are separate names in collision.

    The message names both files, because either spelling can be the one to
    change and the author has to find them.
    """

    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={"sales__Thing.py": PY_TABLE.format(schema="sales")},
    )

    with pytest.raises(DiscoveryError, match="differ only by case") as raised:
        parse_item_repository(root)

    message = str(raised.value)
    assert f"{ITEM}/schemas/Sales.yml" in message
    assert f"{ITEM}/Tables/sales__Thing.py" in message


# --- case-only duplicate schemas ---------------------------------------------
#
# A case-insensitive file system cannot hold both Abc.yml and abc.yml, so the
# rejection is driven through the reader directly with a stub store rather than
# real files. It is a case-sensitive file system this guards against.


class _StubStore:
    def __init__(self, contents: dict[str, bytes]) -> None:
        self._contents = contents

    def read(self, location) -> bytes:
        for relative, data in self._contents.items():
            if location.value.endswith(relative):
                return data
        raise KeyError(location.value)


# --- what an implied schema reaches -------------------------------------------


@weaver_test()
def test_an_implied_schema_reaches_the_physical_plan(tmp_path):
    """An item with no schema file still creates the schema its table needs.

    The build stage derives what to create from the selected identities, so this
    holds the repository model to the thing that consumes it.
    """

    from weaver.build_bundle.schemas import lakehouse_schema_stage

    root = build(
        tmp_path,
        schemas=[],
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )
    repository = parse_item_repository(root)
    item = next(model for model in repository.items if str(model.identity) == ITEM)

    # `_` is Weaver's own, composed into every item. `Widget` is the one the
    # authored table established.
    assert "Widget" in [schema.schema for schema in item.schemas]

    from factories import bound_target, target_inventory

    planned = lakehouse_schema_stage(
        set(item.documents),
        item=item.identity,
        target=bound_target(),
        inventory=target_inventory(),
    )
    created = [
        action
        for batch in planned.batches
        for action in batch.actions
        if action.kind == "create_schema"
    ]

    assert len(created) == 1
    assert "Widget" in planned.payloads[created[0].payload].decode("utf-8")
