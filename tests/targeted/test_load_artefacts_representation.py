"""What a repository's load layer owns, and how it is physically bound.

`weaver.etl` answers one question — given this source, which load artefacts
exist, where do they go, and what is each one signed by — and everything
downstream reads that answer rather than deriving its own. So the claims here are
about the answer itself: which sources produce artefacts and which produce none,
that a path-shaped identity survives being written down and read back, and that a
signature moves for exactly the reasons it should.

The proxy bodies are deliberately not asserted. They are placeholders this branch
replaces later, and pinning their text here would make the replacement look like a
regression.
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    WAREHOUSE_ITEM,
    FixtureCatalogue,
    folder_document,
    item_id,
    lakehouse_table,
    schema_document,
    spark_view,
    warehouse_table,
    warehouse_view,
)

from weaver.build_bundle.incremental import select_build
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId
from weaver.etl import (
    FILE_TYPE,
    LOAD_ROOT,
    PROCEDURE_TYPE,
    item_load_artefacts,
    load_artefacts,
)
from weaver.locations import Location

CUSTOMER = "DWG.Customer"
SUMMARY = "DWG.Summary"
VIEW = "DWG.Active"

SPARK_TABLE = """\
/*
Table ID: {object_id}

Description: A Spark SQL table.

Lineage: Declared for a test.

Dependencies: []

Schema:
  CustomerId: string
*/
select cast(null as string) as CustomerId
 where 1 = 0;
"""


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def estate(tmp_path):
    """One of each source a load artefact can come from, and two that make none.

    A Python table, a Python folder, a helper module, a Spark SQL table and a
    Spark SQL view in the Lakehouse; a table and a view in the Warehouse. Between
    them they cover every branch of the derivation, which is what lets the tests
    below assert the whole set rather than one artefact at a time.
    """

    root = tmp_path / "repo"
    _write(root, f"{ITEM}/schemas/DWG.yml", schema_document("DWG"))
    _write(root, f"{ITEM}/schemas/Raw.yml", schema_document("Raw"))
    _write(root, f"{ITEM}/DWG__Customer.py", lakehouse_table(CUSTOMER))
    _write(root, f"{ITEM}/DWG.Summary.sql", SPARK_TABLE.format(object_id=SUMMARY))
    _write(root, f"{ITEM}/DWG.Active.sql", spark_view(VIEW, depends_on=CUSTOMER))
    _write(root, f"{ITEM}/Files/Raw__Export.py", folder_document("Raw.Export"))
    _write(root, f"{ITEM}/lib/dates.py", "def parse_date(value):\n    return value\n")
    # Not Python, and deployed all the same: lib/ is reproduced verbatim, so a
    # module that reads a data file beside it finds one.
    _write(root, f"{ITEM}/lib/data/holidays.csv", "date,name\n2026-01-01,New Year\n")
    _write(root, f"{WAREHOUSE_ITEM}/schemas/Sales.yml", schema_document("Sales"))
    _write(
        root, f"{WAREHOUSE_ITEM}/Sales.Customer.sql", warehouse_table("Sales.Customer")
    )
    _write(
        root,
        f"{WAREHOUSE_ITEM}/Sales.Live.sql",
        warehouse_view(
            "Sales.Live", select="select 1 as CustomerId", depends_on="Sales.Customer"
        ),
    )
    return parse_item_repository(Location(str(root)))


def identities(artefacts) -> list[str]:
    return sorted(str(artefact.identity) for artefact in artefacts)


# --- what the source owns -----------------------------------------------------


def test_every_source_that_owns_a_load_artefact_owns_exactly_one(estate):
    """The whole derivation, as one visible set.

    Worth asserting whole rather than piecemeal: the interesting property is not
    that a Python file is deployed but that *this* is the complete set — that a
    view contributes nothing, that a helper module nobody declares still gets a
    claim, that a data file beside that module travels with it, and that the
    generated folder document does not deploy itself.
    """

    assert identities(load_artefacts(estate)) == [
        f"{ITEM}/file:{LOAD_ROOT}/DWG__Customer.py",
        # The SQL-authored table, compiled into the module it deploys as.
        f"{ITEM}/file:{LOAD_ROOT}/DWG__Summary.py",
        f"{ITEM}/file:{LOAD_ROOT}/Files/Raw__Export.py",
        f"{ITEM}/file:{LOAD_ROOT}/lib/data/holidays.csv",
        f"{ITEM}/file:{LOAD_ROOT}/lib/dates.py",
        f"{WAREHOUSE_ITEM}/procedure:_/Load Sales.Customer",
    ]


def test_a_view_owns_no_load_artefact_on_either_side(estate):
    """A view's definition *is* its query, so there is nothing for a load to do.

    Asserted on both engines because the two are separate branches of the
    derivation and would fail independently.
    """

    produced = identities(load_artefacts(estate))

    assert not any("Active" in name for name in produced)
    assert not any("Live" in name for name in produced)


def test_the_authored_files_segment_is_preserved(estate):
    """`Sales__X.py` and `Files/Sales__X.py` are different documents.

    Flattening the segment would deploy two legitimately distinct documents to
    one path, so the runtime tree reproduces the authored one exactly.
    """

    assert f"{ITEM}/file:{LOAD_ROOT}/Files/Raw__Export.py" in identities(
        load_artefacts(estate)
    )


def test_the_generated_folder_document_does_not_deploy_itself(estate):
    """It is infrastructure, not authored source, so it owns no artefact."""

    assert not any("___Load" in name for name in identities(load_artefacts(estate)))


def test_the_builtin_catalogue_item_has_no_load_layer(estate):
    """Weaver's own control plane is not a user ETL package."""

    assert item_load_artefacts(estate, item=item_id("Lakehouse/_weaver")) == ()


def test_each_artefact_is_typed_as_what_it_physically_is(estate):
    types = {
        str(artefact.identity): artefact.object_type
        for artefact in load_artefacts(estate)
    }

    assert types[f"{ITEM}/file:{LOAD_ROOT}/lib/dates.py"] == FILE_TYPE
    assert types[f"{WAREHOUSE_ITEM}/procedure:_/Load Sales.Customer"] == PROCEDURE_TYPE


# --- identity -----------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        f"{ITEM}/file:{LOAD_ROOT}/lib/dates.py",
        f"{ITEM}/file:{LOAD_ROOT}/Files/Raw__Export.py",
        f"{WAREHOUSE_ITEM}/procedure:_/Load Sales.Customer",
        f"{ITEM}/{CUSTOMER}",
        f"{ITEM}/Files/Raw.Export",
    ],
)
def test_an_identity_survives_being_written_down_and_read_back(text):
    """The plan records selection as text, so the spelling has to be lossless.

    A path, a filename with an extension, a name with a dot *and* a space —
    every one of these would be ambiguous under the two-part `Schema.Object`
    grammar, which is why the shape is part of what is written.
    """

    assert str(WeaverDocumentId.parse(text)) == text


def test_the_two_halves_are_the_real_target_name(estate):
    """Nothing is encoded to fit a validator, so nothing has to be decoded.

    What the Registry stores is what the target actually calls the object: a
    containing path and a complete filename, or a schema and a procedure name
    that carries the identity of what it loads.
    """

    by_id = {str(a.identity): a.identity for a in load_artefacts(estate)}

    module = by_id[f"{ITEM}/file:{LOAD_ROOT}/lib/dates.py"]
    assert (module.object_id.schema, module.object_id.object) == (
        f"{LOAD_ROOT}/lib",
        "dates.py",
    )

    procedure = by_id[f"{WAREHOUSE_ITEM}/procedure:_/Load Sales.Customer"]
    assert (procedure.object_id.schema, procedure.object_id.object) == (
        "_",
        "Load Sales.Customer",
    )


# --- signatures ---------------------------------------------------------------


def signature_of(repository, text: str) -> str:
    return next(
        artefact.signature
        for artefact in load_artefacts(repository)
        if str(artefact.identity) == text
    )


def test_a_deployed_module_is_signed_by_its_own_bytes(tmp_path):
    """Editing a helper moves its artefact's signature and no other."""

    def build(helper: str):
        root = tmp_path / helper[-4]
        _write(root, f"{ITEM}/schemas/DWG.yml", schema_document("DWG"))
        _write(root, f"{ITEM}/DWG__Customer.py", lakehouse_table(CUSTOMER))
        _write(root, f"{ITEM}/lib/dates.py", helper)
        return parse_item_repository(Location(str(root)))

    before = build("def a(value):\n    return value\n")
    after = build("def b(value):\n    return value\n")

    module = f"{ITEM}/file:{LOAD_ROOT}/lib/dates.py"
    assert signature_of(before, module) != signature_of(after, module)


def test_a_template_version_moves_only_the_bodies_it_renders(estate, monkeypatch):
    """The salts are separate because the two generators evolve separately.

    Bumping the Spark one invalidates generated Spark SQL and nothing else — not
    the Warehouse procedures, and not deployed Python, which is signed by bytes
    that no generator produced.
    """

    import weaver.declaration.load

    spark_file = f"{ITEM}/file:{LOAD_ROOT}/DWG__Summary.py"
    procedure = f"{WAREHOUSE_ITEM}/procedure:_/Load Sales.Customer"
    module = f"{ITEM}/file:{LOAD_ROOT}/DWG__Customer.py"
    before = {
        name: signature_of(estate, name) for name in (spark_file, procedure, module)
    }

    monkeypatch.setattr(weaver.declaration.load, "SPARK_LOAD_VERSION", 99)
    after = {
        name: signature_of(estate, name) for name in (spark_file, procedure, module)
    }

    assert after[spark_file] != before[spark_file]
    assert after[procedure] == before[procedure]
    assert after[module] == before[module]


def test_the_template_versions_do_not_reach_the_repository_signature(
    estate, monkeypatch
):
    """It describes authored content, and a renderer's version is not authored."""

    import weaver.declaration.load

    monkeypatch.setattr(weaver.declaration.load, "TSQL_LOAD_VERSION", 99)

    assert parse_item_repository(estate.root).signature == estate.signature


def test_the_deployed_tree_carries_every_lib_file_not_only_python(estate):
    """`lib/` is reproduced verbatim, whatever is in it.

    A `.py` filter here was reading across from the *top level*, where a Weaver
    document is python, sql or yml. But a helper module that reads a data file
    beside it needs that file to have travelled with it — and it did not, so on
    Fabric the module found nothing.
    """

    deployed = {
        str(artefact.identity)
        for artefact in item_load_artefacts(estate, item=item_id())
    }

    assert f"{ITEM}/file:{LOAD_ROOT}/lib/dates.py" in deployed
    assert f"{ITEM}/file:{LOAD_ROOT}/lib/data/holidays.csv" in deployed


def test_an_alias_declaration_is_not_runtime_source(estate):
    """It declares where a name points; nothing imports it at load time."""

    deployed = {
        str(artefact.identity)
        for artefact in item_load_artefacts(estate, item=item_id())
    }

    assert not any(name.endswith("alias.yml") for name in deployed)


# --- incremental selection ----------------------------------------------------


def selected(repository, *, item: str = ITEM):
    """What a build would do to an item whose estate is already correct."""

    identity = item_id(item)
    catalogue = FixtureCatalogue.from_repository(repository, item=identity)
    wanted = {key for key in repository.source_documents if key.item == identity}
    wanted |= {a.identity for a in item_load_artefacts(repository, item=identity)}
    return select_build(repository, catalogue.registered, selected=wanted)


def test_an_unchanged_estate_selects_no_load_work(estate):
    """The premise every incremental claim rests on."""

    assert selected(estate).selected_for_build == ()


def test_changing_one_module_rebuilds_that_module_alone(tmp_path):
    """Granularity is one target file per claim, which is the point of claiming
    them one by one: a repository of a hundred modules must not redeploy all of
    them because one changed."""

    root = tmp_path / "repo"
    _write(root, f"{ITEM}/schemas/DWG.yml", schema_document("DWG"))
    _write(root, f"{ITEM}/DWG__Customer.py", lakehouse_table(CUSTOMER))
    _write(root, f"{ITEM}/lib/dates.py", "def parse(value):\n    return value\n")
    _write(root, f"{ITEM}/lib/text.py", "def upper(value):\n    return value\n")
    before = parse_item_repository(Location(str(root)))

    catalogue = FixtureCatalogue.from_repository(before, item=item_id())
    _write(root, f"{ITEM}/lib/text.py", "def upper(value):\n    return value.upper()\n")
    after = parse_item_repository(Location(str(root)))

    wanted = {key for key in after.source_documents if key.item == item_id()}
    wanted |= {a.identity for a in item_load_artefacts(after, item=item_id())}
    selection = select_build(after, catalogue.registered, selected=wanted)

    assert [str(value) for value in selection.selected_for_build] == [
        f"{ITEM}/file:{LOAD_ROOT}/lib/text.py"
    ]


def test_a_changed_upstream_document_does_not_rebuild_an_unchanged_artefact(tmp_path):
    """The load layer is bundle sequencing, not an authored graph edge.

    A view rebuilt because its table changed is ordinary impact propagation. A
    deployed module has no edge to travel along and its bytes have not moved, so
    nothing reaches it.
    """

    root = tmp_path / "repo"
    _write(root, f"{ITEM}/schemas/DWG.yml", schema_document("DWG"))
    _write(root, f"{ITEM}/DWG__Customer.py", lakehouse_table(CUSTOMER))
    _write(root, f"{ITEM}/DWG.Active.sql", spark_view(VIEW, depends_on=CUSTOMER))
    _write(root, f"{ITEM}/lib/dates.py", "def parse(value):\n    return value\n")
    before = parse_item_repository(Location(str(root)))

    catalogue = FixtureCatalogue.from_repository(before, item=item_id())
    _write(
        root,
        f"{ITEM}/DWG__Customer.py",
        lakehouse_table(CUSTOMER, columns={"CustomerId": "string", "Name": "string"}),
    )
    after = parse_item_repository(Location(str(root)))

    wanted = {key for key in after.source_documents if key.item == item_id()}
    wanted |= {a.identity for a in item_load_artefacts(after, item=item_id())}
    rebuilt = {
        str(value)
        for value in select_build(
            after, catalogue.registered, selected=wanted
        ).selected_for_build
    }

    # The table changed, so the view over it is rebuilt, and the table's own
    # deployed copy is rebuilt because its bytes changed.
    assert f"{ITEM}/{VIEW}" in rebuilt
    assert f"{ITEM}/file:{LOAD_ROOT}/DWG__Customer.py" in rebuilt
    # The helper is untouched, and nothing carried impact to it.
    assert f"{ITEM}/file:{LOAD_ROOT}/lib/dates.py" not in rebuilt
