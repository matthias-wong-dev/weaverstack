"""Schema SES files: one declared schema per file, matched to its filename."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from weaver import Location
from weaver.errors import DiscoveryError
from weaver.ses import (
    SchemaSes,
    is_schema_file,
    parse_schema_document,
    read_repository,
    read_schema_document,
)


def parse(text: str, path: str = "_schemas/Sales.yml") -> SchemaSes:
    return parse_schema_document(textwrap.dedent(text), path)


# --- parsing -----------------------------------------------------------------


def test_a_minimal_schema_declares_only_its_id():
    schema = parse("Schema ID: Sales")
    assert schema.schema_id == "Sales"
    assert schema.description is None


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


def test_a_missing_id_is_refused():
    with pytest.raises(DiscoveryError, match="Schema ID is required"):
        parse("Description: no id here")


def test_a_blank_id_is_refused():
    with pytest.raises(DiscoveryError, match="Schema ID is required"):
        parse("Schema ID: '   '")


def test_a_dotted_id_is_refused():
    with pytest.raises(DiscoveryError, match="single bare name"):
        parse("Schema ID: Sales.Order")


def test_an_unknown_key_is_refused():
    with pytest.raises(DiscoveryError, match="unknown schema key"):
        parse("Schema ID: Sales\nColour: blue")


def test_a_non_mapping_is_refused():
    with pytest.raises(DiscoveryError, match="must be a YAML mapping"):
        parse("- just a list")


# --- filename identity -------------------------------------------------------


def test_the_filename_must_match_the_id():
    with pytest.raises(DiscoveryError, match="match exactly"):
        read_schema_document("_schemas/Sales.yml", b"Schema ID: Reporting")


def test_the_filename_match_is_case_sensitive():
    with pytest.raises(DiscoveryError, match="match exactly"):
        read_schema_document("_schemas/sales.yml", b"Schema ID: Sales")


def test_a_matching_filename_reads():
    schema = read_schema_document("_schemas/Sales.yml", b"Schema ID: Sales")
    assert schema.schema_id == "Sales"


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


def build(tmp_path: Path, *, schemas: list[str], objects: dict[str, str]) -> Location:
    directory = tmp_path / "_schemas"
    directory.mkdir()
    for schema in schemas:
        (directory / f"{schema}.yml").write_text(f"Schema ID: {schema}\n", encoding="utf-8")
    for name, text in objects.items():
        (tmp_path / name).write_text(textwrap.dedent(text), encoding="utf-8")
    return Location(str(tmp_path))


def test_a_native_object_needs_its_schema_declared(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],  # not Widget
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )
    with pytest.raises(DiscoveryError, match="schema 'Widget' is not declared"):
        read_repository(root)


def test_a_declared_schema_lets_the_object_read(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={"Sales__Thing.py": PY_TABLE.format(schema="Sales")},
    )
    assert "delta:Sales.Thing" in read_repository(root).graph.nodes


def test_an_unused_schema_is_still_valid(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales", "Unused"],
        objects={"Sales__Thing.py": PY_TABLE.format(schema="Sales")},
    )
    repo = read_repository(root)
    assert "Unused" in repo.schemas
    assert "Unused" not in repo.schemas_by_namespace["lakehouse"]


def test_the_error_names_the_expected_schema_file(tmp_path):
    root = build(
        tmp_path,
        schemas=[],
        objects={"Widget__Thing.py": PY_TABLE.format(schema="Widget")},
    )
    with pytest.raises(DiscoveryError, match=r"_schemas/Widget\.yml"):
        read_repository(root)
