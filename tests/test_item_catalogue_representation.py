"""Pure-Python tests for item-scoped catalogue projection and DML."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test
from test_item_dependencies_declaration import _dependency_estate
from test_item_repository_declaration import _estate

from weaver.catalogue.projection import (
    CatalogueProjection,
    project_item_catalogue,
    project_shortcut_registry,
)
from weaver.catalogue.reconcile import reconcile
from weaver.catalogue.tables import (
    CATALOGUE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    REGISTRY,
    SCHEMA_DICTIONARY,
    SCOPE_ITEM_NAME,
    SCOPE_ITEM_TYPE,
    SHORTCUT,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.locations import Location


def _project(repository, item_text: str, target: str, *, target_kind="lakehouse"):
    item = WeaverItemId.parse(item_text)
    retained = [
        identity for identity in repository.source_documents if identity.item == item
    ]
    # Every declaration, not only the logical ones: a physical shortcut is
    # installed here exactly as a logical one is.
    retained.extend(
        declaration.destination
        for declaration in repository.shortcuts
        if declaration.owner == item
    )
    # The projection is source-only. Shortcut certification and the Installation
    # row are composed on top, exactly as publication composes them, so this
    # helper still yields the whole picture these tests assert against.
    projection = project_item_catalogue(repository, item=item, retained=retained)
    rows = dict(projection.rows)
    rows[REGISTRY.name] = tuple(
        rows.get(REGISTRY.name, ())
    ) + project_shortcut_registry(
        repository, item=item, retained=retained, target_kind=target_kind
    )
    item_model = next(m for m in repository.items if m.identity == item)
    rows[INSTALLATION.name] = (
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "target_name": target,
            "weaver_version": "1.2.3",
            "signature": item_model.signature,
        },
    )
    return CatalogueProjection(scope=projection.scope, rows=rows)


def _registry_row(projection, schema: str, name: str):
    return next(
        row
        for row in projection.for_table(REGISTRY)
        if row["schema_name"] == schema and row["object_name"] == name
    )


@weaver_test()
def test_every_catalogue_table_is_keyed_by_exact_item_without_repository():
    for table in CATALOGUE_TABLES:
        assert table.key[:2] == ("item_type", "item_name")
        assert table.column_names[:2] == table.key[:2]
        assert "repository" not in table.column_names
        assert "object_namespace" not in table.column_names


@weaver_test()
def test_tables_and_files_with_same_name_are_distinct_registry_rows(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    projection = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    rows = projection.for_table(REGISTRY)

    customer = [row for row in rows if row["object_name"] == "Customer"]
    assert {row["schema_name"] for row in customer} == {"Sales", "Files/Sales"}


@weaver_test()
def test_folder_schema_is_catalogued_as_files_slash_declared_schema(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    projection = _project(repository, "Lakehouse/Raw", "Raw_Dev")

    schemas = {row["schema_name"] for row in projection.for_table(SCHEMA_DICTIONARY)}
    # `Files/_` is the generated runtime folder's schema. It is catalogued by the
    # same rule as any other folder schema, which is the point: nothing about the
    # load layer gets a namespace convention of its own.
    assert schemas == {"Sales", "Files/Sales", "Files/_"}


@weaver_test()
def test_no_catalogue_table_keeps_a_hidden_namespace_dimension():
    namespace_columns = {
        "object_namespace",
        "destination_namespace",
        "source_namespace",
        "reference_namespace",
    }
    for table in CATALOGUE_TABLES:
        assert namespace_columns.isdisjoint(table.column_names)


@weaver_test()
def test_two_items_of_same_type_have_independent_scope_and_dml(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    raw = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    curated = _project(repository, "Lakehouse/Curated", "Curated_Dev")

    raw_sql = "\n".join(reconcile(raw).statements)
    curated_sql = "\n".join(reconcile(curated).statements)
    assert "[Item name] = N'Raw'" in raw_sql
    assert "[Item name] = N'Curated'" not in raw_sql
    assert "[Item name] = N'Curated'" in curated_sql
    assert "[Item name] = N'Raw'" not in curated_sql


@weaver_test()
def test_rebinding_changes_only_installation_attribute_not_scope(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    first = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    second = _project(repository, "Lakehouse/Raw", "Raw_Prod")

    first_row = first.for_table(INSTALLATION)[0]
    second_row = second.for_table(INSTALLATION)[0]
    assert first.scope == second.scope
    assert first_row["target_name"] == "Raw_Dev"
    assert second_row["target_name"] == "Raw_Prod"
    assert {key: value for key, value in first_row.items() if key != "target_name"} == {
        key: value for key, value in second_row.items() if key != "target_name"
    }


@weaver_test()
def test_installation_records_the_item_signature_not_the_repository_signature(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    projection = _project(repository, "Lakehouse/Raw", "Raw_Dev")
    row = projection.for_table(INSTALLATION)[0]

    assert row["signature"] == repository["Lakehouse/Raw"].signature
    assert row["signature"] != repository.signature


@weaver_test()
def test_shortcut_rows_reproduce_what_was_declared(tmp_path):
    """Including how the target is read, which is the reader's other question."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(repository, "Warehouse/Reporting", "Reporting_Dev")
    row = projection.for_table(SHORTCUT)[0]

    assert row[SCOPE_ITEM_TYPE] == "Warehouse"
    assert row[SCOPE_ITEM_NAME] == "Reporting"
    assert row["shortcut_id"] == "Sales.PortableCustomer"
    assert row["schema_name"] == "Sales"
    assert row["object_name"] == "PortableCustomer"
    assert row["shortcut_type"] == "view"
    assert row["target_type"] == "logical"
    assert row["target_item_type"] == "Lakehouse"
    assert row["target_item_name"] == "Curated"
    assert row["target_schema_name"] == "Sales"
    assert row["target_object_name"] == "Customer"
    # A logical target is bound, so where it lands is Installation's answer.
    assert row["target_workspace_name"] is None


@weaver_test()
def test_a_shortcut_destination_is_registered_as_the_object_it_actually_is(tmp_path):
    """No ``shortcut`` *type*. To every reader of the catalogue an external
    reference in a Warehouse is a view, and that is what it is recorded as. What
    it is *for* is the role, and where it points is ``_.Shortcut``."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="warehouse"
    )
    row = _registry_row(projection, "Sales", "PortableCustomer")

    assert row["object_type"] == "view"
    assert row["object_role"] == "shortcut"


@weaver_test()
def test_a_lakehouse_shortcut_is_registered_as_a_table(tmp_path):
    """The same shortcut against a Lakehouse is a table — a OneLake shortcut is how
    it is made, not what it is."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="lakehouse"
    )

    assert (
        _registry_row(projection, "Sales", "PortableCustomer")["object_type"] == "table"
    )


@weaver_test()
def test_a_shortcut_signature_is_its_declaration_and_not_its_targets_content(tmp_path):
    """A rebuilt source does not redeclare the shortcut, so it must not change
    its signature: that would replace every downstream shortcut on every reload."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="warehouse"
    )
    declaration = next(
        declaration
        for declaration in repository.shortcuts
        if str(declaration.destination) == "Warehouse/Reporting/Sales.PortableCustomer"
    )
    source = repository.source_documents[declaration.logical_source]

    registry = _registry_row(projection, "Sales", "PortableCustomer")
    assert registry["signature"] == declaration.signature
    assert registry["signature"] != source.effective_signature
    assert projection.for_table(SHORTCUT)[0]["signature"] == declaration.signature


@weaver_test()
def test_a_shortcut_describes_nothing_beyond_its_registration(tmp_path):
    """It holds no columns, no keys and no dependencies of its own. Only the two
    rows that say it exists and what it stands for."""

    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(
        repository, "Warehouse/Reporting", "Reporting_Dev", target_kind="warehouse"
    )
    destination = ("Sales", "PortableCustomer")

    for table in CATALOGUE_TABLES:
        if table in (REGISTRY, SHORTCUT, SCHEMA_DICTIONARY, INSTALLATION):
            continue
        assert not [
            row
            for row in projection.for_table(table)
            if (row.get("schema_name"), row.get("object_name")) == destination
        ], f"{table.name} should hold no row for a shortcut destination"


@weaver_test()
def test_dependency_row_belongs_to_consumer_item_and_preserves_authored_name(tmp_path):
    repository = parse_item_repository(Location(str(_dependency_estate(tmp_path))))
    projection = _project(repository, "Warehouse/Reporting", "Reporting_Dev")
    row = projection.for_table(DEPENDENCY)[0]

    assert row["item_type"] == "Warehouse"
    assert row["item_name"] == "Reporting"
    assert row["dependency_reference"] == "Sales.PortableCustomer"
    # The author wrote a local name; resolution followed the shortcut across, so
    # the row keeps both the spelling and the item the edge actually reached.
    assert row["referenced_item_type"] == "Lakehouse"
    assert row["referenced_item_name"] == "Curated"


@weaver_test()
def test_registry_merge_is_last_and_item_scoped(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    reconciliation = reconcile(_project(repository, "Lakehouse/Raw", "Raw_Dev"))

    assert reconciliation.registry.table is REGISTRY
    assert reconciliation.statements[-1] == reconciliation.registry.merge
    assert "`repository`" not in reconciliation.registry.merge
    assert "[Item type] = N'Lakehouse'" in reconciliation.registry.merge
    assert "[Item name] = N'Raw'" in reconciliation.registry.merge


# --- reading a catalogue of an older shape -------------------------------------


class _Absent(Exception):
    def getErrorClass(self):  # noqa: N802 - Spark's own spelling
        return "TABLE_OR_VIEW_NOT_FOUND"


class _FakeCatalogue:
    """The narrowest thing ``read_catalogue_state`` will accept.

    Duck-typed on purpose: the module names no engine API, so a shape check
    needs no connection. A table this does not hold is absent, which is the
    bootstrap answer rather than a failure.
    """

    def __init__(self, columns_by_table):
        self._columns = columns_by_table

    def columns_of(self, table) -> dict[str, str] | None:
        if table.name not in self._columns:
            return None
        return {}

    def rows(self, *_a, **_k):
        raise AssertionError("no rows should be read")


class _Shaped(_FakeCatalogue):
    """A catalogue whose tables carry exactly the columns named, publicly spelled."""

    def columns_of(self, table) -> dict[str, str] | None:
        if table.name not in self._columns:
            return None
        return {
            table.public_name_of(name).casefold(): table.public_name_of(name)
            for name in self._columns[table.name]
        }


@weaver_test()
def test_a_registry_without_the_epoch_column_is_refused_by_name():
    """It can be read but not written — the merge sets the build_datetime on every insert.

    Failing here says which column of which table is wrong. Letting it through
    would move the failure into the install, where it arrives as an engine
    complaint about an unknown column and says nothing about the catalogue.
    """

    from weaver.catalogue.state import read_catalogue_state
    from weaver.errors import BuildError

    older = [name for name in REGISTRY.physical_columns if name != "build_datetime"]

    with pytest.raises(BuildError, match=r"Registry\.Build datetime"):
        read_catalogue_state(_Shaped({"Registry": older}), ())


def _shaped(*names: str):
    """A catalogue holding exactly these tables, each with its full shape."""

    from weaver.catalogue.tables import CATALOGUE_TABLES

    by_name = {table.name: table for table in CATALOGUE_TABLES}
    return _Shaped({name: list(by_name[name].physical_columns) for name in names})


def _whole():
    """Every catalogue table present — the only complete state a build accepts."""

    from weaver.catalogue.tables import CATALOGUE_TABLES

    return _shaped(*(table.name for table in CATALOGUE_TABLES))


@weaver_test()
def test_a_registry_with_the_epoch_column_is_accepted():
    from weaver.catalogue.state import read_catalogue_state
    from weaver.catalogue.tables import CATALOGUE_TABLES

    state = read_catalogue_state(_whole(), ())

    assert state.present_tables == {table.name for table in CATALOGUE_TABLES}


# --- which absences are the first run, and which are damage -------------------
#
# One rule, and the thing that decides it is whether anything else is there.
# The tempting exception — a *dictionary* table, because the built-in item will
# recreate it — is the case these tests exist to refuse. Recreating the table is
# not restoring its contents, and an ordinary build is scoped to the items it
# was pointed at, so it can only ever write those items' rows back.


@weaver_test()
def test_a_catalogue_with_no_tables_at_all_is_the_bootstrap_state():
    """The first build creates the catalogue, so nothing there is not a fault."""

    from weaver.catalogue.state import read_catalogue_state

    state = read_catalogue_state(_Shaped({}), ())

    assert state.present_tables == frozenset()
    assert not state.rows


@weaver_test()
def test_a_missing_dictionary_table_beside_a_populated_catalogue_is_refused():
    """The physical table would come back; another item's rows would not.

    On an estate holding `Sales` and `Finance`, a build scoped to `Sales` would
    recreate the table and write Sales' rows — leaving Finance with none, while
    Finance's Registry and Installation rows survived to claim objects the
    dictionaries no longer describe. Nothing later notices: the next build finds
    a table that exists and rows that match whatever it was scoped to.
    """

    from weaver.catalogue.state import read_catalogue_state
    from weaver.catalogue.tables import CATALOGUE_TABLES
    from weaver.errors import BuildError

    all_but_one = [
        table.name for table in CATALOGUE_TABLES if table.name != "TableDictionary"
    ]

    with pytest.raises(BuildError, match="catalogue is incomplete"):
        read_catalogue_state(_shaped(*all_but_one), ())


@weaver_test()
def test_a_missing_registry_beside_a_populated_catalogue_is_refused():
    """It cannot be re-derived at all, so it is the plainest case of the rule."""

    from weaver.catalogue.state import read_catalogue_state
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="catalogue is incomplete"):
        read_catalogue_state(_shaped("Installation", "TableDictionary"), ())


@weaver_test()
def test_a_missing_installation_beside_a_populated_catalogue_is_refused():
    from weaver.catalogue.state import read_catalogue_state
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="catalogue is incomplete"):
        read_catalogue_state(_shaped("Registry", "TableDictionary"), ())


@weaver_test()
def test_the_incomplete_catalogue_error_names_what_is_gone_and_what_survived():
    """A reader has to tell damage from a first run, and know where to look."""

    from weaver.catalogue.state import read_catalogue_state
    from weaver.errors import BuildError

    with pytest.raises(BuildError) as raised:
        read_catalogue_state(_shaped("Registry", "Shortcut", "Dependency"), ())

    message = str(raised.value)
    assert "Installation" in message and "TableDictionary" in message
    assert "Shortcut" in message and "Dependency" in message


@weaver_test()
def test_the_incomplete_catalogue_error_sends_the_reader_to_a_repair():
    """Not to a rebuild: a scoped build is the thing that cannot fix this."""

    from weaver.catalogue.state import read_catalogue_state
    from weaver.errors import BuildError

    with pytest.raises(BuildError) as raised:
        read_catalogue_state(_shaped("Registry", "Installation"), ())

    message = str(raised.value)
    assert "repair" in message
    assert "scoped build" in message


@weaver_test()
def test_a_catalogue_predating_an_introduced_table_still_builds():
    """An estate built by an older Weaver is an upgrade, not damage.

    The two are indistinguishable from the physical state alone, and what tells
    them apart is consequence. The refusal above exists so a scoped build cannot
    recreate a table and lose rows belonging to items it was not pointed at; a
    table that has never existed anywhere has no such rows to lose, so creating
    it costs nothing and the unbuilt items are correctly represented by having
    no rows in it yet.

    Without this, adding a dictionary table would stop every existing estate
    from building until somebody repaired a catalogue that was never broken —
    which is exactly what `_.TestDictionary` did on a real Fabric workspace.
    """

    from weaver.catalogue.state import INTRODUCED_TABLES, read_catalogue_state
    from weaver.catalogue.tables import CATALOGUE_TABLES

    as_an_older_weaver_left_it = [
        table.name for table in CATALOGUE_TABLES if table.name not in INTRODUCED_TABLES
    ]

    state = read_catalogue_state(_shaped(*as_an_older_weaver_left_it), ())

    assert state.rows == {}


@weaver_test()
def test_an_introduced_table_does_not_excuse_a_genuinely_damaged_catalogue():
    """The exemption is for that table alone, not for whatever else is gone."""

    from weaver.catalogue.state import INTRODUCED_TABLES, read_catalogue_state
    from weaver.catalogue.tables import CATALOGUE_TABLES
    from weaver.errors import BuildError

    damaged = [
        table.name
        for table in CATALOGUE_TABLES
        if table.name not in INTRODUCED_TABLES and table.name != "TableDictionary"
    ]

    with pytest.raises(BuildError, match="catalogue is incomplete") as raised:
        read_catalogue_state(_shaped(*damaged), ())

    message = str(raised.value)
    assert "TableDictionary" in message
    # And it does not accuse the reader of having lost the new table too.
    assert "TestDictionary" not in message.split("missing while")[0]


# --- shortcuts of every kind ---------------------------------------------------
#
# A physical shortcut and a schema shortcut are registered and described exactly
# as a logical one is. Each of these was a real defect that the full Fabric
# journey found first, which it should not have had to.


def _shortcut_estate(tmp_path):
    """One Lakehouse declaring a shortcut of every kind, physical and logical."""

    from test_item_repository_declaration import _schema, _table, _write

    root = tmp_path / "shortcuts"
    _write(root, "Lakehouse/Raw/schemas/Sales.yml", _schema("Sales"))
    _write(root, "Lakehouse/Raw/Sales__Customer.py", _table("Sales.Customer"))
    _write(root, "Lakehouse/Curated/schemas/Sales.yml", _schema("Sales"))
    _write(
        root,
        "Lakehouse/Curated/shortcuts.py",
        "from weaver import Shortcut\n\n"
        "Sales__Landed = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="logical",\n'
        '    target="Lakehouse/Raw/Sales.Customer",\n)\n\n'
        "Sales__External = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/Ref.Customer",\n'
        '    workspace="Shared Data",\n)\n\n'
        "Sales__Incoming = Shortcut(\n"
        '    shortcut_type="folder",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Landing/Files/Incoming/Daily",\n'
        '    workspace="Shared Data",\n)\n\n'
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/Ref",\n'
        '    workspace="Shared Data",\n)\n',
    )
    return parse_item_repository(Location(str(root)))


def _shortcut_rows(tmp_path):
    repository = _shortcut_estate(tmp_path)
    projection = _project(repository, "Lakehouse/Curated", "Curated_Dev")
    return repository, {
        row["shortcut_id"]: row for row in projection.for_table(SHORTCUT)
    }


@weaver_test()
def test_a_physical_shortcut_is_registered_like_any_other(tmp_path):
    """It is installed here exactly as a logical one is.

    Without a Registry row it is uncertified, so the next build prunes it and a
    load refuses the name as not installed.
    """

    repository = _shortcut_estate(tmp_path)
    projection = _project(
        repository, "Lakehouse/Curated", "Curated_Dev", target_kind="lakehouse"
    )
    registered = {
        (row["schema_name"], row["object_name"]): row
        for row in projection.for_table(REGISTRY)
    }

    assert registered[("Sales", "External")]["object_type"] == "table"
    assert registered[("Sales", "External")]["object_role"] == "shortcut"
    # A folder is catalogued under Files/<schema>, as a declared one is.
    assert registered[("Files/Sales", "Incoming")]["object_type"] == "folder"
    # A schema shortcut is registered as the schema it presents.
    assert registered[("Reference", "Reference")]["object_type"] == "schema"
    assert registered[("Reference", "Reference")]["object_role"] == "shortcut"


@weaver_test()
def test_a_schema_shortcut_is_not_a_schema_the_item_owns(tmp_path):
    """Its namespace belongs to the item it points at.

    Reported as a schema in use, the item would claim a namespace it does not
    own and a build would try to create it.
    """

    repository = _shortcut_estate(tmp_path)
    projection = _project(repository, "Lakehouse/Curated", "Curated_Dev")
    in_use = {row["schema_name"] for row in projection.for_table(SCHEMA_DICTIONARY)}

    assert "Sales" in in_use
    assert "Reference" not in in_use


@weaver_test()
def test_a_shortcut_row_names_the_shortcut_the_way_registry_does(tmp_path):
    """``Shortcut ID`` keys it; ``Schema``/``Object`` are the reader's identity.

    So a reader joins Registry and Shortcut without splitting an id.
    """

    _repository, rows = _shortcut_rows(tmp_path)

    assert rows["Sales.Landed"]["schema_name"] == "Sales"
    assert rows["Sales.Landed"]["object_name"] == "Landed"
    assert rows["Sales.Incoming"]["object_name"] == "Incoming"
    # A schema shortcut presents a namespace, so it names no object.
    assert rows["Reference"]["schema_name"] == "Reference"
    assert rows["Reference"]["object_name"] is None


@weaver_test()
def test_a_logical_target_is_stored_whole_for_a_reader_to_rebuild(tmp_path):
    """The producer's identity, without Installation and without parsing.

    ``Lakehouse/Raw/Sales.Customer`` comes straight out of the four target
    columns, which is what lets the estate DAG be reconstructed from the
    catalogue alone.
    """

    _repository, rows = _shortcut_rows(tmp_path)
    row = rows["Sales.Landed"]

    assert row["target_type"] == "logical"
    rebuilt = (
        f"{row['target_item_type']}/{row['target_item_name']}/"
        f"{row['target_schema_name']}.{row['target_object_name']}"
    )
    assert rebuilt == "Lakehouse/Raw/Sales.Customer"
    # A logical target is bound, so where it lands stays Installation's answer.
    assert row["target_workspace_name"] is None


@weaver_test()
def test_a_physical_target_names_the_fabric_item_and_its_workspace(tmp_path):
    """It has no logical producer, so nothing here pretends it does."""

    _repository, rows = _shortcut_rows(tmp_path)

    external = rows["Sales.External"]
    assert external["target_type"] == "physical"
    assert external["target_item_name"] == "Reference"
    assert external["target_schema_name"] == "Ref"
    assert external["target_object_name"] == "Customer"
    assert external["target_workspace_name"] == "Shared Data"

    # A target that names a path or a namespace names no object.
    assert rows["Reference"]["target_object_name"] is None
    assert rows["Sales.Incoming"]["target_object_name"] is None
    assert rows["Sales.Incoming"]["target_schema_name"] == "Incoming/Daily"
