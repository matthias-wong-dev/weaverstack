"""What the catalogue can store, and what a Warehouse table can hold.

Both limits are the platform's, and neither can be discovered at install time
without having already lost something: a value wider than its column is
narrowed by the cast that writes it, and a table past the column ceiling is
refused after the build has started. So both are read off the project.

The widths here are the shipped catalogue declaration's own, read from it
rather than repeated, so a column that changes width changes what these prove.
"""

from __future__ import annotations

from pathlib import Path
from shutil import copytree

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.capacity import (
    capacity_of,
    stored_size,
)
from weaver.catalogue.tables import TABLE_DICTIONARY
from weaver.errors import DiscoveryError
from weaver.operations.check import check

#: The widest catalogue column, and the one authored prose lands in.
PROSE = capacity_of(TABLE_DICTIONARY.column("description"))
#: Where a serialised column set lands.
LIST = capacity_of(TABLE_DICTIONARY.column("not_null_columns"))
#: An ordinary identifier column.
IDENTIFIER = capacity_of(TABLE_DICTIONARY.column("object_name"))


def _project(root: Path) -> Path:
    fixture = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
    copytree(fixture, root)
    return root


def _table(
    root: Path,
    *,
    description: str = "Customers.",
    extra: str = "",
    columns: tuple[str, ...] = ("CustomerId", "CustomerName"),
) -> Path:
    """The fixture's own table, rewritten. Its consumers still reference it."""

    schema = "".join(f"  {name}: string\n" for name in columns)
    document = root / "Lakehouse" / "Raw" / "Tables" / "DWG__Customer.py"
    document.write_text(
        f'"""\nTable ID: DWG.Customer\n\nDescription: {description}\n'
        "\nLineage: The sales system.\n\nPrimary key: CustomerId\n"
        f"{extra}"
        f"\nSchema:\n{schema}"
        '"""\n\nfrom weaver import Table\n\n\n'
        "class DWG__Customer(Table):\n    def read(self):\n        return None\n",
        encoding="utf-8",
    )
    return document


# --- the widths themselves -----------------------------------------------------


@weaver_test()
def test_capacity_is_read_from_the_column_that_would_store_the_value():
    assert PROSE == 4000
    assert LIST == 1000
    assert IDENTIFIER == 128
    assert capacity_of(TABLE_DICTIONARY.column("is_incremental")) is None


@weaver_test()
def test_an_unbounded_column_has_no_capacity_to_check():
    """A named comparison set is as wide as the author made it.

    Narrowing one to fit storage would change what a load treats as a change,
    so the column is ``varchar(max)`` and there is nothing here to refuse.
    """

    column = TABLE_DICTIONARY.column("comparison_columns")

    assert column.warehouse_type == "varchar(max)"
    assert capacity_of(column) is None


@weaver_test()
def test_a_value_is_measured_in_the_bytes_the_column_counts():
    """``varchar(n)`` is a byte capacity in a Fabric Warehouse's UTF-8
    collations, so a multibyte character costs more than one of its budget."""

    assert stored_size("abc") == 3
    assert stored_size("é") == 2
    assert stored_size("日本語") == 9
    assert stored_size("naïve café") == 12


@weaver_test()
def test_an_apostrophe_costs_what_it_stores_and_not_what_it_is_written_as():
    """Doubling it is how a T-SQL literal is spelled. The column never sees it."""

    assert stored_size("it's") == 4


@weaver_test()
def test_a_newline_is_one_byte_like_any_other():
    assert stored_size("a\nb") == 3


# --- just below, exactly at, and just above ------------------------------------


@weaver_test()
@pytest.mark.parametrize("size", [PROSE - 1, PROSE])
def test_a_description_that_fits_is_accepted(tmp_path, size):
    root = _project(tmp_path / "project")
    _table(root, description="d" * size)

    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_description_one_byte_over_is_refused(tmp_path):
    """Rather than a cast that stores the first 4,000 bytes of it."""

    root = _project(tmp_path / "project")
    _table(root, extra="\nRevision notes:\n  - " + "d" * (PROSE + 1) + "\n")
    _table(root, description="d" * (PROSE + 1), columns=("CustomerId",))
    (root / "Lakehouse" / "Raw" / "Tables" / "DWG.ActiveCustomer.sql").unlink()
    (root / "Lakehouse" / "Raw" / "Tables" / "DWG.ActiveCustomerSummary.sql").unlink()

    with pytest.raises(DiscoveryError) as refused:
        check(root)

    assert "DWG__Customer.py" in str(refused.value)
    assert "Description" in str(refused.value)
    assert f"{PROSE + 1} bytes" in str(refused.value)
    assert f"stores {PROSE}" in str(refused.value)


@weaver_test()
def test_a_description_that_fits_in_characters_but_not_in_bytes_is_refused(tmp_path):
    """The failure a character count would miss: every character is three bytes,
    so a third of the column's characters fill all of its bytes."""

    root = _project(tmp_path / "project")
    _table(root, description="日" * (PROSE // 3 + 1))

    with pytest.raises(DiscoveryError, match="bytes"):
        check(root)


@weaver_test()
def test_an_exactly_fitting_value_survives_projection_and_rendering(tmp_path):
    """Accepted offline and written out, so the boundary is usable rather than
    merely not refused."""

    from weaver.build_bundle.workflow import prepare_repository
    from weaver.catalogue.projection import project_item_catalogue
    from weaver.catalogue.render import InstallationScope, render_merge
    from weaver.catalogue.tables import TABLE_DICTIONARY as DICTIONARY
    from weaver.declaration.model import WeaverItemId
    from weaver.locations import Location
    from weaver.store import FilesystemStore

    description = "d" * PROSE
    root = _project(tmp_path / "project")
    _table(root, description=description)
    item = WeaverItemId.parse("Lakehouse/Raw")

    with prepare_repository(
        Location(root.as_posix()), source_store=FilesystemStore()
    ) as prepared:
        repository = prepared.repository
        owned = next(one for one in repository.items if one.identity == item)
        projection = project_item_catalogue(
            repository, item=item, retained=owned.documents
        )
        rows = projection.for_table(DICTIONARY)
        statement = render_merge(
            DICTIONARY, rows, scope=InstallationScope("Lakehouse", "Raw")
        )

    assert any(row["description"] == description for row in rows)
    assert description in statement


# --- other bounded columns -----------------------------------------------------


@weaver_test()
def test_a_serialised_column_set_that_overflows_its_column_is_refused(tmp_path):
    """A declared column set is stored as one comma-separated value."""

    root = _project(tmp_path / "project")
    columns = tuple(f"Column{index:04d}" for index in range(120))
    _table(
        root,
        extra="\nNot null:\n" + "".join(f"  - {name}\n" for name in columns),
        columns=("CustomerId", *columns),
    )

    with pytest.raises(DiscoveryError) as refused:
        check(root)

    assert "Not null columns" in str(refused.value)
    assert f"stores {LIST}" in str(refused.value)


@weaver_test()
def test_a_named_comparison_set_is_not_bounded_by_the_catalogue(tmp_path):
    """Past every bounded width, and accepted.

    The comparison set drives update detection, so a capacity that forced an
    author to narrow one would change the load rather than the record of it.
    """

    root = _project(tmp_path / "unbounded")
    columns = tuple(f"Column{index:04d}" for index in range(700))
    _table(
        root,
        extra="\nComparison columns: " + ", ".join(columns) + "\n",
        columns=("CustomerId", *columns),
    )

    assert stored_size(", ".join(columns)) > PROSE
    assert check(root).project_folder == root.as_posix()


@weaver_test()
def test_a_column_note_too_long_for_its_column_is_refused(tmp_path):
    root = _project(tmp_path / "project")
    _table(
        root,
        description="Customers.",
        extra="\nColumn notes:\n  CustomerName: " + "n" * (PROSE + 1) + "\n",
    )

    with pytest.raises(DiscoveryError, match="bytes"):
        check(root)


@weaver_test()
def test_a_referenced_description_is_measured_after_it_resolves(tmp_path):
    """What the catalogue stores is the resolved text, not the reference."""

    root = _project(tmp_path / "project")
    _table(root, description="d" * (PROSE + 1))

    with pytest.raises(DiscoveryError) as refused:
        check(root)

    # The fixture's view takes its Lineage from this description, so the row
    # that overflows is the view's and the repair is in what it points at.
    assert "DWG.ActiveCustomer.sql" in str(refused.value)
    assert "Lineage" in str(refused.value)
    assert "from $DWG.Customer" in str(refused.value)


@weaver_test()
def test_a_valid_project_and_the_shipped_catalogue_declaration_still_pass(tmp_path):
    """Weaver's own ``_weaver`` item is projected by the same check."""

    root = _project(tmp_path / "project")

    assert check(root).project_folder == root.as_posix()


# --- the offline path reaches nothing -----------------------------------------


@weaver_test()
def test_the_capacity_check_reaches_no_fabric_capability(tmp_path, monkeypatch):
    import weaver.resolution as resolution

    root = _project(tmp_path / "project")
    _table(root, description="d" * (PROSE + 1))
    monkeypatch.setattr(
        resolution,
        "resolver_for",
        lambda workspace: pytest.fail("an offline check must reach no resolver"),
    )
    monkeypatch.setattr(
        resolution,
        "store_for",
        lambda workspace: pytest.fail("an offline check must reach no target store"),
    )

    with pytest.raises(DiscoveryError):
        check(root)


# --- and the renderer refuses what reaches it anyway ---------------------------


@weaver_test()
def test_the_renderer_refuses_a_value_its_cast_would_narrow():
    """Defence in depth: a build-derived value reaches no offline check."""

    from weaver.catalogue.render import InstallationScope, render_merge
    from weaver.errors import BuildError

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "schema_name": "DWG",
        "object_name": "Customer",
        "object_type": "table",
        "description": "d" * (PROSE + 1),
        "signature": "s",
    }

    with pytest.raises(BuildError, match="bytes"):
        render_merge(
            TABLE_DICTIONARY, [row], scope=InstallationScope("Lakehouse", "Raw")
        )


@weaver_test()
def test_the_renderer_leaves_what_a_run_records_alone():
    """Retention and truncation of a run's own log is its own policy."""

    from weaver.catalogue.render import (
        InstallationScope,
        InstallationScopes,
        render_merge,
    )
    from weaver.catalogue.tables import LOG

    row = {
        "item_type": "Lakehouse",
        "item_name": "Raw",
        "log_sk": "1",
        "workflow_id": "w",
        "task_type": "load",
        "message": "m" * 6000,
    }

    statement = render_merge(
        LOG, [row], scope=InstallationScopes((InstallationScope("Lakehouse", "Raw"),))
    )

    assert statement


@weaver_test()
def test_a_schema_description_names_the_schema_file_it_came_from(tmp_path, capsys):
    """A schema row repeats its schema as its object, so its provenance is the
    file that declared it rather than an object's."""

    root = _project(tmp_path / "project")
    schema = root / "Lakehouse" / "Raw" / "schemas" / "DWG.yml"
    schema.write_text(
        "Schema ID: DWG\n\nDescription: " + "d" * (PROSE + 1) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(DiscoveryError) as refused:
        check(root)

    assert "schemas/DWG.yml" in str(refused.value)
    assert "Description" in str(refused.value)
