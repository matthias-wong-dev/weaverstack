"""The built-in catalogue repository, read the way any repository is read.

The claim this file exists to substantiate is that a catalogue table is an
*ordinary* Weaver object. So it is checked with the ordinary SES reader, through
the ordinary closure and validation, with no catalogue-aware shortcut — if these
pass, the normal build path can build the catalogue, which is the whole bootstrap
argument.

It also pins the committed resources against the table definitions. The text is
committed because it is the contract a reviewer reads; that makes drift possible,
so drift is made loud instead.
"""

from __future__ import annotations

from importlib.resources import files

import pytest

from weaver import LocalStore, Location
from weaver.catalogue.legacy import (
    CATALOGUE_REPOSITORY,
    CATALOGUE_SCHEMA,
    CATALOGUE_TABLES,
    AUDIT_COLUMN_NAMES,
)
from weaver.catalogue.builtin import (
    RESOURCE_DIRECTORY,
    RESOURCE_PACKAGE,
    SCHEMA_FILE,
    render_sources,
    repository_files,
)
from weaver.ses import SPARK_SQL, TABLE, read_repository


@pytest.fixture(scope="module")
def builtin(tmp_path_factory):
    """The built-in repository, materialised from resources and read normally."""

    root = tmp_path_factory.mktemp("builtin")
    for relative, data in repository_files().items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return read_repository(
        Location(value=str(root)), store=LocalStore(), name=CATALOGUE_REPOSITORY
    )


# --- the resources ship ------------------------------------------------------


def test_the_resources_are_reachable_the_way_a_wheel_exposes_them():
    """``importlib.resources``, not a filesystem path — that is what works installed."""

    directory = files(RESOURCE_PACKAGE) / RESOURCE_DIRECTORY
    assert directory.is_dir()
    assert (directory / SCHEMA_FILE).is_file()
    for table in CATALOGUE_TABLES:
        assert (directory / f"{table.qualified}.spark.sql").is_file()


def test_the_committed_resources_match_the_table_definitions():
    """Drift is loud. Add a column and this fails until the resource is regenerated.

    The alternative — generating the SES at runtime — would remove the drift but
    also remove the reviewable declaration, which is the more valuable half.
    """

    shipped = repository_files()
    canonical = render_sources()
    assert sorted(shipped) == sorted(canonical)
    for relative, text in canonical.items():
        assert shipped[relative].decode("utf-8") == text, relative


def test_only_the_ses_files_travel():
    """No ``__init__.py``, no compiled artefacts — they are not part of the repository.

    Anything else copied in would become a support file of the installed
    repository and would change its signature.
    """

    assert set(repository_files()) == {SCHEMA_FILE} | {
        f"{table.qualified}.spark.sql" for table in CATALOGUE_TABLES
    }


# --- it reads as a repository ------------------------------------------------


def test_every_catalogue_table_is_an_object_of_the_repository(builtin):
    assert sorted(document.qualified for document in builtin.documents) == sorted(
        table.qualified for table in CATALOGUE_TABLES
    )


def test_the_repository_carries_no_support_files(builtin):
    assert builtin.support_files == ()


def test_every_object_is_a_spark_sql_table_in_the_reserved_schema(builtin):
    for document in builtin.documents:
        assert document.kind == TABLE
        assert document.language == SPARK_SQL
        assert document.object_id.schema == CATALOGUE_SCHEMA


def test_the_reserved_schema_is_declared(builtin):
    assert set(builtin.schemas) == {CATALOGUE_SCHEMA}
    assert builtin.schemas[CATALOGUE_SCHEMA].description
    # The schema declaration is hashed like an object's, because the catalogue
    # records a signature for its schema rows.
    assert builtin.schemas[CATALOGUE_SCHEMA].source_hash


def test_every_object_declares_static_and_prohibit_rebuild(builtin):
    for document in builtin.documents:
        assert document.document.static, document.qualified
        assert document.document.prohibit_rebuild, document.qualified


def test_every_object_declares_its_full_schema(builtin):
    for table in CATALOGUE_TABLES:
        document = builtin[table.qualified].document
        assert document.has_declared_schema
        assert [column.name for column in document.schema] == list(table.column_names)
        assert [column.type for column in document.schema] == [
            column.type for column in table.columns
        ]


def test_every_object_depends_on_nothing_and_says_so(builtin):
    for document in builtin.documents:
        assert document.document.declares_dependencies
        assert document.document.dependencies == ()
    assert builtin.dependency_edges == ()


def test_the_declared_primary_key_is_the_catalogue_key(builtin):
    """The tables describe themselves, which is what makes the bootstrap meaningful.

    Declaring the key in SES is also what makes those columns physically not null,
    so the not-null guarantee the representation asserts is the one the built table
    actually has.
    """

    for table in CATALOGUE_TABLES:
        document = builtin[table.qualified].document
        assert document.primary_key == table.key
        for name in table.key:
            declared = next(c for c in document.schema if c.name == name)
            assert declared.not_null, f"{table.name}.{name}"


def test_every_not_null_column_of_the_representation_is_not_null_in_ses(builtin):
    for table in CATALOGUE_TABLES:
        document = builtin[table.qualified].document
        declared = {column.name: column.not_null for column in document.schema}
        for column in table.columns:
            assert declared[column.name] == column.not_null, f"{table.name}.{column.name}"


def test_ses_derives_the_same_comparison_columns_as_the_representation(builtin):
    """Two independent definitions of "what makes a row different" must agree.

    SES defaults comparison columns to every declared non-key column; the
    representation computes the same set for its MERGE guard. If they ever
    diverged, an unchanged row could be written or a changed one skipped.
    """

    for table in CATALOGUE_TABLES:
        document = builtin[table.qualified].document
        assert document.comparison_columns == table.comparison_columns


def test_every_column_carries_a_note(builtin):
    for table in CATALOGUE_TABLES:
        document = builtin[table.qualified].document
        for column in document.schema:
            assert column.note is not None, f"{table.name}.{column.name}"
            assert column.note.literal


def test_a_note_about_a_reference_survives_as_prose_not_as_a_reference(builtin):
    """``description_reference`` is described in terms of ``$Schema.Object``.

    Written raw that would parse as a reference and be refused, so the generator
    escapes it. The parsed note must read as prose with a single dollar.
    """

    document = builtin["_.TableDictionary"].document
    note = next(c.note for c in document.schema if c.name == "description_reference")
    assert note.reference is None
    assert "$Schema.Object" in note.literal


def test_no_object_declares_an_audit_column(builtin):
    """Build appends them, so declaring one would be both wrong and refused."""

    for document in builtin.documents:
        declared = {column.name for column in document.document.schema}
        assert not declared & set(AUDIT_COLUMN_NAMES)


def test_the_audit_columns_arrive_anyway_in_the_effective_schema(builtin):
    for table in CATALOGUE_TABLES:
        document = builtin[table.qualified].document
        assert [column.name for column in document.effective_schema] == list(
            table.physical_columns
        )


# --- the body -----------------------------------------------------------------


def test_the_body_declares_the_shape_and_returns_no_rows(builtin):
    for table in CATALOGUE_TABLES:
        body = builtin[table.qualified].sql_body
        assert body is not None
        assert body.rstrip().endswith("where 1 = 0")
        for column in table.columns:
            assert f"cast(null as {column.type}) as `{column.name}`" in body


def test_the_body_is_one_result_set_and_creates_nothing(builtin):
    for document in builtin.documents:
        analysis = document.sql_analysis
        assert analysis is not None
        assert analysis.result_set_count == 1
        # Weaver writes the CREATE; a body that wrote its own would be a smell.
        assert analysis.permanent_ddl == ()


def test_the_query_column_order_matches_the_declared_order(builtin):
    """The executor requires the sets to be equivalent by exact name.

    Order is not part of that check, but the physical column order comes from the
    declaration, so keeping the two aligned means the file reads the way the table
    is built.
    """

    for table in CATALOGUE_TABLES:
        body = builtin[table.qualified].sql_body or ""
        positions = [body.index(f"`{column.name}`") for column in table.columns]
        assert positions == sorted(positions), table.name


# --- the resources reach an installed wheel -----------------------------------


def test_the_ses_resources_are_packaged_into_a_built_wheel(tmp_path):
    """Built, not assumed. A Fabric Environment installs a wheel, not this checkout.

    Hatch includes package data by default, so nothing declares these files
    explicitly — which is exactly why it is worth a test: a change to the build
    configuration could drop them and every other test here would still pass, since
    they read from the source tree.
    """

    import subprocess
    import sys
    import zipfile
    from pathlib import Path

    project = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(tmp_path)],
        cwd=project,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"wheel build unavailable here: {result.stderr.strip()[-200:]}")

    (wheel,) = tmp_path.glob("*.whl")
    packaged = set(zipfile.ZipFile(wheel).namelist())
    for relative in render_sources():
        assert f"weaver/builtin/catalogue/{relative}" in packaged, relative
