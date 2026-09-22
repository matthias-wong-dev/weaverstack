"""What the catalogue records about an upsert's comparison set.

Two different things share one name. The effective set a load compares is every
eligible non-key column unless an author named a narrower one; the catalogue
records only what was named. Storing the effective set would publish a default
nobody wrote, derived from the schema in the row beside it, and wide enough on a
wide table to exceed the column that holds it.

Comparison needs a primary key. An unkeyed incremental table appends and an
unkeyed one that is not incremental is replaced, so neither compares anything.

The runtime half is unchanged and asserted here beside the catalogue half,
because the point is that they differ.
"""

from __future__ import annotations

from pathlib import Path
from shutil import copytree

from support.weaver_test import weaver_test

from weaver.build_bundle.workflow import prepare_repository
from weaver.catalogue.capacity import capacity_of, stored_size
from weaver.catalogue.projection import project_item_catalogue
from weaver.catalogue.tables import LIST_TYPE, TABLE_DICTIONARY
from weaver.declaration.model import WeaverItemId
from weaver.locations import Location
from weaver.operations.check import check
from weaver.runtime.load_contract import LoadContract
from weaver.store import FilesystemStore

ITEM = WeaverItemId.parse("Lakehouse/Raw")

#: The width an ordinary serialised column set gets, for comparison with the
#: unbounded one a named comparison set gets.
ORDINARY = int(LIST_TYPE.removeprefix("varchar(").removesuffix(")"))


def _project(root: Path) -> Path:
    fixture = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
    copytree(fixture, root)
    return root


def _table(
    root: Path,
    *,
    columns: tuple[str, ...],
    key: str | None = "CustomerId",
    comparison: tuple[str, ...] = (),
) -> Path:
    """The fixture's own table, rewritten. Its consumers still reference it."""

    schema = "".join(f"  {name}: string\n" for name in columns)
    declared = "Primary key: " + key + "\n" if key else ""
    if comparison:
        declared += "\nComparison columns: " + ", ".join(comparison) + "\n"
    document = root / "Lakehouse" / "Raw" / "Tables" / "DWG__Customer.py"
    document.write_text(
        '"""\nTable ID: DWG.Customer\n\nDescription: Customers.\n'
        f"\nLineage: The sales system.\n\n{declared}"
        f"\nSchema:\n{schema}"
        '"""\n\nfrom weaver import Table\n\n\n'
        "class DWG__Customer(Table):\n    def read(self):\n        return None\n",
        encoding="utf-8",
    )
    return document


def _recorded(root: Path):
    """The projected TableDictionary row for the rewritten table, and its document."""

    with prepare_repository(
        Location(root.as_posix()), source_store=FilesystemStore()
    ) as prepared:
        repository = prepared.repository
        owned = next(one for one in repository.items if one.identity == ITEM)
        rows = project_item_catalogue(
            repository, item=ITEM, retained=owned.documents
        ).for_table(TABLE_DICTIONARY)
        row = next(row for row in rows if row["object_name"] == "Customer")
        source = next(
            source
            for identity, source in repository.source_documents.items()
            if identity.item == ITEM and identity.object_id.object == "Customer"
        )
        return row, source.document


# --- a default is not a declaration --------------------------------------------


@weaver_test()
def test_a_keyed_table_that_named_nothing_records_nothing(tmp_path):
    """And the load still compares every eligible non-key column."""

    root = _project(tmp_path / "default")
    _table(root, columns=("CustomerId", "CustomerName", "Region"))

    row, document = _recorded(root)

    assert row["comparison_columns"] is None
    assert LoadContract.from_document(document).comparison_columns == (
        "CustomerName",
        "Region",
    )


@weaver_test()
def test_a_named_comparison_set_is_recorded_exactly_as_it_was_written(tmp_path):
    root = _project(tmp_path / "named")
    _table(
        root,
        columns=("CustomerId", "CustomerName", "Region"),
        comparison=("Region",),
    )

    row, document = _recorded(root)

    assert row["comparison_columns"] == "Region"
    assert LoadContract.from_document(document).comparison_columns == ("Region",)


@weaver_test()
def test_an_unkeyed_table_records_none_because_it_compares_nothing(tmp_path):
    """No key, no upsert: incremental appends and the rest is replaced."""

    root = _project(tmp_path / "unkeyed")
    _table(root, columns=("CustomerName", "Region"), key=None)

    row, _document = _recorded(root)

    assert row["comparison_columns"] is None


# --- and a wide table is not refused for a set nobody wrote --------------------


def _wide(count: int) -> tuple[str, ...]:
    return ("CustomerId",) + tuple(f"Column{index:05d}" for index in range(count))


@weaver_test()
def test_a_table_too_wide_to_serialise_its_default_still_checks(tmp_path):
    """The default would be many times an ordinary column set. Nothing stores it."""

    columns = _wide(900)
    root = _project(tmp_path / "wide")
    _table(root, columns=columns)

    row, document = _recorded(root)
    effective = LoadContract.from_document(document).comparison_columns

    assert stored_size(", ".join(columns[1:])) > ORDINARY
    assert row["comparison_columns"] is None
    # Changing any business column is still an update.
    assert effective == columns[1:]
    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_named_set_that_wide_is_recorded_in_full(tmp_path):
    """Nothing is narrowed to fit: the column it lands in is unbounded."""

    columns = _wide(900)
    root = _project(tmp_path / "wide-named")
    _table(root, columns=columns, comparison=columns[1:])

    row, document = _recorded(root)
    named = ", ".join(columns[1:])

    assert capacity_of(TABLE_DICTIONARY.column("comparison_columns")) is None
    assert stored_size(named) > ORDINARY
    assert row["comparison_columns"] == named
    assert LoadContract.from_document(document).comparison_columns == columns[1:]


@weaver_test()
def test_a_wide_unkeyed_table_checks_too(tmp_path):
    root = _project(tmp_path / "wide-unkeyed")
    _table(root, columns=_wide(900)[1:], key=None)

    assert check(root).project_folder == root.as_posix()
