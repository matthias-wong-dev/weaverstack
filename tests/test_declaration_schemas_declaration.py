"""Schema Weaver document files: one declared schema per file, matched to its filename."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import (
    SchemaSes,
    is_schema_file,
    parse_item_repository,
    parse_schema_document,
    read_schema_document,
)
from weaver.errors import DiscoveryError
from weaver.locations import Location


def parse(text: str, path: str = "_schemas/Sales.yml") -> SchemaSes:
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
def test_is_schema_file_only_matches_the_schemas_directory():
    assert is_schema_file("_schemas/Sales.yml")
    assert not is_schema_file("Sales.yml")
    assert not is_schema_file("_helpers/Sales.yml")
    assert not is_schema_file("_schemas/nested/Sales.yml")
    assert not is_schema_file("_schemas/notes.md")


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
    for name, text in objects.items():
        (tmp_path / ITEM / name).write_text(textwrap.dedent(text), encoding="utf-8")
    return Location(str(tmp_path))


@weaver_test()
def test_a_native_object_needs_its_schema_declared(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],  # not Widget
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )
    with pytest.raises(DiscoveryError, match="schema 'Widget' is not declared"):
        parse_item_repository(root)


@weaver_test()
def test_a_declared_schema_lets_the_object_read(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={"Sales__Thing.py": PY_TABLE.format(schema="Sales")},
    )
    assert f"{ITEM}/Sales.Thing" in parse_item_repository(root).dependency_graph.nodes


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
def test_the_error_names_the_expected_schema_file(tmp_path):
    root = build(
        tmp_path,
        schemas=[],
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )
    with pytest.raises(DiscoveryError, match="schema 'Widget' is not declared"):
        parse_item_repository(root)


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
