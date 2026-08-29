"""What a Catalogue is responsible for, now that it owns catalogue I/O.

The ``_`` schema is the catalogue and every table in it is a catalogue table, so
one object reads it and one object writes it. Four claims about that object:

* it is selectively materialised. It holds what it was asked for, and
  ``_.Log`` is not asked for, being history nothing consults;
* it answers which installed object a physical name is, or refuses to guess;
* runtime rows go through it, appended or merged, and a merged row is visible to
  a later read of the same catalogue at once;
* a write that did not land is raised by ``flush`` and nowhere else.

Pure Python. Every input is constructible, and the rows are in the shape the
``_`` schema holds them.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from support.catalogues import LOADED_AT, Recording, identity, loaded, never
from support.weaver_test import weaver_test

from weaver.catalogue.claims import bookmark_row
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL,
    CATALOGUE_TABLES,
    CURRENT_STATE_TABLES,
    HISTORY_TABLES,
    LOG,
    PROJECTED_TABLES,
    READABLE_TABLES,
    RUNTIME_TABLES,
)
from weaver.errors import CommandError, ConfigError


class _Connection:
    """A catalogue connection over canned rows, keyed the way Python holds them."""

    def __init__(self, rows=None) -> None:
        self._rows = rows or {}
        self.read: list[str] = []

    def columns_of(self, table):
        return {name.casefold(): name for name in table.public_columns}

    def rows(self, statement: str):
        name = next(
            table.name for table in CATALOGUE_TABLES if f"[{table.name}]" in statement
        )
        self.read.append(name)
        return self._rows.get(name, [])


# --- selective materialisation -------------------------------------------------


@weaver_test()
def test_a_run_reads_the_bookmark_and_writes_the_rest():
    """The asymmetry worth knowing about the current-state tables.

    A run writes a load status and a test status and never asks what they were,
    so reading them would be round trips for an answer nothing uses. It does read
    bookmarks, because an incremental load asks how far it got.
    """

    assert set(READABLE_TABLES) == set(PROJECTED_TABLES) | {BOOKMARK}
    assert not set(READABLE_TABLES) & set(HISTORY_TABLES)


@weaver_test()
def test_a_build_reads_every_current_state_table_it_can_invalidate():
    """It decides obsolete rows from the rows it holds, so it holds all of them.

    A tripwire: a current-state table the build could name in an invalidation
    but could not see would keep a row describing an incarnation that no longer
    exists.
    """

    from weaver.catalogue.state import READ_FOR_BUILD

    assert set(CURRENT_STATE_TABLES) <= set(READ_FOR_BUILD)
    assert not set(READ_FOR_BUILD) & set(HISTORY_TABLES)


@weaver_test()
def test_the_runtime_tables_are_one_of_the_two_kinds_and_no_other():
    assert set(HISTORY_TABLES) | set(CURRENT_STATE_TABLES) == set(RUNTIME_TABLES)
    assert not set(HISTORY_TABLES) & set(CURRENT_STATE_TABLES)
    assert not set(RUNTIME_TABLES) & set(PROJECTED_TABLES)
    assert set(CATALOGUE_TABLES) == set(PROJECTED_TABLES) | set(RUNTIME_TABLES)


@weaver_test()
def test_an_installed_read_does_not_read_the_history():
    from weaver.catalogue.state import read_installed_catalogue

    connection = _Connection()

    catalogue = read_installed_catalogue(connection)

    for table in HISTORY_TABLES:
        assert table.name not in connection.read
        assert table.name not in catalogue.materialised
    assert BOOKMARK.name in connection.read


@weaver_test()
def test_materialised_says_what_was_loaded_and_not_what_exists():
    """Two different questions, and the catalogue answers only one of them.

    A table that exists and holds no rows was still loaded, so it is materialised.
    Whether a table is physically there is a target's, and a target's inventory
    answers it. See `tests/targeted/test_bookmark_build_install.py`.
    """

    from weaver.catalogue.state import read_installed_catalogue
    from weaver.catalogue.tables import INSTALLATION, REGISTRY

    connection = _Connection({REGISTRY.name: []})

    catalogue = read_installed_catalogue(connection, tables=(INSTALLATION, REGISTRY))

    assert catalogue.materialised == {INSTALLATION.name, REGISTRY.name}
    assert catalogue.table_rows(REGISTRY) == ()
    # And nothing on it claims to know what the Warehouse physically holds.
    assert not hasattr(catalogue, "present_tables")


@weaver_test()
def test_a_read_materialises_what_it_was_asked_for_and_no_more():
    from weaver.catalogue.state import read_installed_catalogue
    from weaver.catalogue.tables import INSTALLATION, REGISTRY

    connection = _Connection()

    catalogue = read_installed_catalogue(connection, tables=(INSTALLATION, REGISTRY))

    assert connection.read == [INSTALLATION.name, REGISTRY.name]
    assert catalogue.materialised == {INSTALLATION.name, REGISTRY.name}


@weaver_test()
def test_a_session_gives_a_catalogue_that_can_be_written():
    """One construction: what an operation reads, and the way back."""

    from support.sessions import given_session
    from support.workspaces import given_workspace

    workspace = given_workspace(catalogue="Warehouse/Weaver_LH")
    with given_session(workspace=workspace) as session:
        from weaver.catalogue.state import catalogue_for

        catalogue = catalogue_for(session, workspace)
        catalogue.submit(LOG, {"log_sk": "a", "task_type": "load"})
        catalogue.flush()

    assert LOG.name not in catalogue.materialised
    assert BOOKMARK.name in catalogue.materialised
    assert [
        statement
        for call in session.calls
        if call.kind == "tsql"
        for statement in call.body
        if "INSERT INTO [_].[Log]" in statement
    ]


# --- which installed object a name is ------------------------------------------


@weaver_test()
def test_a_physical_name_resolves_through_installation_and_registry():
    """Not through the target's name: a physical name is not a logical identity."""

    catalogue = loaded("DWG.Customer")

    resolved = catalogue.installed_object(
        target_kind="Lakehouse",
        target_name="Sales_LH",
        schema="DWG",
        object="Customer",
        is_files=False,
    )

    assert resolved == identity("DWG.Customer")


@weaver_test()
def test_a_folder_resolves_under_its_files_identity():
    """A Folder and a Table of the same name are two objects."""

    catalogue = never("Raw.CustomerCsv", files=True)

    resolved = catalogue.installed_object(
        target_kind="Lakehouse",
        target_name="Sales_LH",
        schema="Raw",
        object="CustomerCsv",
        is_files=True,
    )

    assert resolved == identity("Raw.CustomerCsv", files=True)


@weaver_test()
def test_an_object_the_catalogue_does_not_record_is_refused():
    with pytest.raises(ConfigError) as raised:
        never("DWG.Customer").installed_object(
            target_kind="Lakehouse",
            target_name="Sales_LH",
            schema="DWG",
            object="Absent",
            is_files=False,
        )

    assert "not an object the Weaver catalogue records as installed" in str(
        raised.value
    )


@weaver_test()
def test_a_target_is_a_kind_and_a_name():
    """``Lakehouse/Shared`` and ``Warehouse/Shared`` are two Fabric items.

    A row's kind is its item's, because a Lakehouse item deploys to a Lakehouse.
    """

    from types import MappingProxyType

    from weaver.declaration.model import WeaverItemId

    def rows(kind):
        owner = WeaverItemId(kind, kind)
        return owner, MappingProxyType(
            {
                "Installation": (
                    {
                        "item_type": kind,
                        "item_name": kind,
                        "target_name": "Shared",
                        "weaver_version": "0.1",
                        "signature": "s",
                    },
                )
            }
        )

    catalogue = Catalogue(
        MappingProxyType(dict([rows("Lakehouse"), rows("Warehouse")]))
    )

    assert catalogue.bound_to(kind="Lakehouse", name="Shared") == {
        WeaverItemId("Lakehouse", "Lakehouse")
    }
    assert catalogue.bound_to(kind="Warehouse", name="Shared") == {
        WeaverItemId("Warehouse", "Warehouse")
    }


@weaver_test()
def test_a_name_two_bound_items_both_claim_is_refused():
    """Anything that guessed would act on the wrong object."""

    from types import MappingProxyType

    from weaver.declaration.model import WeaverItemId

    def rows(item_name):
        owner = WeaverItemId("Lakehouse", item_name)
        return owner, MappingProxyType(
            {
                "Installation": (
                    {
                        "item_type": "Lakehouse",
                        "item_name": item_name,
                        "target_name": "Sales_LH",
                        "weaver_version": "0.1",
                        "signature": "s",
                    },
                ),
                "Registry": (
                    {
                        "item_type": "Lakehouse",
                        "item_name": item_name,
                        "schema_name": "DWG",
                        "object_name": "Customer",
                        "object_type": "table",
                        "object_role": "data",
                        "signature": "s",
                        "build_datetime": None,
                    },
                ),
            }
        )

    catalogue = Catalogue(MappingProxyType(dict([rows("Sales"), rows("Archive")])))

    with pytest.raises(ConfigError) as raised:
        catalogue.installed_object(
            target_kind="Lakehouse",
            target_name="Sales_LH",
            schema="DWG",
            object="Customer",
            is_files=False,
        )

    assert "more than one installed object" in str(raised.value)


@weaver_test()
def test_a_runtime_artefact_is_not_the_object_it_loads():
    """The module that does the loading is not the thing being loaded."""

    from types import MappingProxyType

    from weaver.declaration.model import WeaverItemId

    owner = WeaverItemId("Lakehouse", "Sales")
    catalogue = Catalogue(
        MappingProxyType(
            {
                owner: MappingProxyType(
                    {
                        "Installation": (
                            {
                                "item_type": "Lakehouse",
                                "item_name": "Sales",
                                "target_name": "Sales_LH",
                                "weaver_version": "0.1",
                                "signature": "s",
                            },
                        ),
                        "Registry": (
                            {
                                "item_type": "Lakehouse",
                                "item_name": "Sales",
                                "schema_name": "DWG",
                                "object_name": "Customer",
                                "object_type": "table",
                                "object_role": "load",
                                "signature": "s",
                                "build_datetime": None,
                            },
                        ),
                    }
                )
            }
        )
    )

    with pytest.raises(ConfigError):
        catalogue.installed_object(
            target_kind="Lakehouse",
            target_name="Sales_LH",
            schema="DWG",
            object="Customer",
            is_files=False,
        )


# --- how far an object has been loaded -----------------------------------------


@weaver_test()
def test_a_bookmark_row_reads_back_as_an_aware_instant():
    assert loaded("DWG.Customer").bookmark(identity("DWG.Customer")) == LOADED_AT


@weaver_test()
def test_no_bookmark_row_reads_as_the_sentinel():
    """Absence is the answer, not a missing one: nothing has loaded cleanly."""

    catalogue = never("DWG.Customer")

    assert catalogue.bookmark(identity("DWG.Customer")) == BOOKMARK_SENTINEL
    assert catalogue.bookmark(identity("DWG.Customer")) == datetime(
        1900, 1, 1, tzinfo=timezone.utc
    )


@weaver_test()
def test_a_stored_instant_with_no_zone_is_read_as_utc():
    """``datetime2`` carries none, and every instant Weaver writes there is UTC."""

    naive = datetime(2026, 8, 20, 6, 0)

    at = loaded("DWG.Customer", at=naive).bookmark(identity("DWG.Customer"))

    assert at == naive.replace(tzinfo=timezone.utc)
    assert at.tzinfo is not None


@weaver_test()
def test_another_objects_bookmark_is_not_this_ones():
    catalogue = loaded("DWG.Order")

    assert catalogue.bookmark(identity("DWG.Order")) == LOADED_AT
    assert catalogue.bookmark(identity("DWG.Customer")) == BOOKMARK_SENTINEL


# --- writing through it --------------------------------------------------------


@weaver_test()
def test_a_submitted_row_is_appended():
    catalogue = never("DWG.Customer")

    catalogue.submit(LOG, {"log_sk": "a", "task_type": "load"})

    assert catalogue.writer.submitted == [("Log", {"log_sk": "a", "task_type": "load"})]
    assert catalogue.writer.updated == []


@weaver_test()
def test_an_updated_row_is_merged():
    catalogue = never("DWG.Customer")
    row = bookmark_row(identity("DWG.Customer"), LOADED_AT)

    catalogue.update(BOOKMARK, row)

    assert catalogue.writer.updated == [("Bookmark", row)]
    assert catalogue.writer.submitted == []


@weaver_test()
def test_an_updated_bookmark_is_visible_to_a_reader_at_once():
    """A run that just advanced a bookmark and then asks is asking about its own load.

    Before the Warehouse has it: the write is queued and the flush
    is later, and a read in between must not see what the row replaced.
    """

    catalogue = never("DWG.Customer")
    assert catalogue.bookmark(identity("DWG.Customer")) == BOOKMARK_SENTINEL

    catalogue.update(BOOKMARK, bookmark_row(identity("DWG.Customer"), LOADED_AT))

    assert catalogue.bookmark(identity("DWG.Customer")) == LOADED_AT
    assert catalogue.writer.flushes == 0


@weaver_test()
def test_an_updated_bookmark_replaces_the_one_that_was_read():
    later = datetime(2026, 8, 25, 9, 0, tzinfo=timezone.utc)
    catalogue = loaded("DWG.Customer")

    catalogue.update(BOOKMARK, bookmark_row(identity("DWG.Customer"), later))

    assert catalogue.bookmark(identity("DWG.Customer")) == later


@weaver_test()
def test_a_write_that_did_not_land_is_raised_by_flush():
    """The one place a failure surfaces, because the queue is what came before it."""

    catalogue = never("DWG.Customer", writer=Recording(failing=RuntimeError("refused")))
    catalogue.update(BOOKMARK, bookmark_row(identity("DWG.Customer"), LOADED_AT))

    with pytest.raises(RuntimeError, match="refused"):
        catalogue.flush()


@weaver_test()
def test_a_catalogue_is_live_state_and_says_so():
    """Rows are read into it, written through it and read back from it.

    Held as one object rather than replaced by a new one on every write, because
    a run advancing a bookmark and then asking for it is asking about the load it
    just did.
    """

    from dataclasses import fields, is_dataclass

    catalogue = never("DWG.Customer")

    assert is_dataclass(catalogue)
    assert not getattr(catalogue, "__dataclass_params__").frozen
    assert {field.name for field in fields(catalogue)} == {
        "rows",
        "registered",
        "materialised",
    }


@weaver_test()
def test_no_table_has_a_write_method_of_its_own():
    """Two verbs, whatever the table. A tripwire for the next table added.

    ``advance_bookmark()``, ``set_load_status()``, ``record_test_status()``: each
    would be a place for one table's rules to diverge from the rest, and the
    reason the mechanism is generic is that a table declares how its rows are
    maintained and everything reads that declaration.
    """

    writing = {
        name
        for name in vars(Catalogue)
        if not name.startswith("_")
        and callable(getattr(Catalogue, name, None))
        and any(
            verb in name
            for verb in ("write", "record", "advance", "set_", "merge", "append")
        )
    }

    assert writing == set()
    # `bookmark()` is a read and stays: absence coalescing to the sentinel is a
    # meaning the caller would otherwise have to know, and it writes nothing.
    assert callable(Catalogue.bookmark)


@weaver_test()
def test_a_catalogue_with_nowhere_to_write_says_so():
    """What a catalogue reconstructed from a payload has: it crossed as data."""

    catalogue = Catalogue({})

    with pytest.raises(CommandError) as raised:
        catalogue.submit(LOG, {"log_sk": "a"})

    assert "cannot be written here" in str(raised.value)


@weaver_test()
def test_a_catalogue_survives_the_crossing_that_carries_it():
    """A bookmark is a catalogue row, so it travels the way every row travels."""

    crossed = Catalogue.from_mapping(loaded("DWG.Customer").to_mapping())

    assert crossed.bookmark(identity("DWG.Customer")) == LOADED_AT
    assert crossed.installed_object(
        target_kind="Lakehouse",
        target_name="Sales_LH",
        schema="DWG",
        object="Customer",
        is_files=False,
    ) == identity("DWG.Customer")
