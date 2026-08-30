"""What a deployed shortcut reads, and what a logical one additionally answers.

A logical shortcut reads its data through the physical shortcut in the item that
declares it, and answers Weaver questions from the source document its
declaration named. A physical shortcut names a Fabric location, so it reads and
answers nothing about Weaver.

The OneLake shortcut is a symbolic link here: a window in the consumer item onto
the producer's directory, under a different path. Everything else is the real
Folder load, the real change documents and the real
:class:`~weaver.catalogue.state.Catalogue`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType

import pytest
from support.catalogues import Recording, installed
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Folder, Table
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import BOOKMARK_SENTINEL
from weaver.errors import LoadError
from weaver.shortcuts import FolderShortcut, TableShortcut

#: The producer and the consumer, and the Fabric items each is bound to.
LANDING = "Lakehouse/Landing"
LANDING_TARGET = "Landing_LH"
CURATED = "Lakehouse/Curated"
CURATED_TARGET = "Curated_LH"

#: What the source Folder is called on each side of the shortcut.
SOURCE = "Lakehouse/Landing/Files/LAND.SourceEvents"
SOURCE_SCHEMA, SOURCE_OBJECT = "LAND", "SourceEvents"
LOCAL_SCHEMA, LOCAL_OBJECT = "SRC", "SourceEvents"

#: When the source Folder was last cleanly loaded, and when the consumer was.
#: Two values, so a bookmark read through the shortcut says which one it took.
SOURCE_LOADED = datetime(2026, 8, 20, 6, 0, tzinfo=timezone.utc)
CONSUMER_LOADED = datetime(2026, 8, 21, 7, 0, tzinfo=timezone.utc)


class LAND__SourceEvents(Folder):
    """The managed source Folder, loaded through Weaver on the producer side."""

    files: dict[str, str] = {}
    deletes: tuple[str, ...] = ()

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        return parse_document(
            """
Folder ID: LAND.SourceEvents

Description: Incoming event files.

Lineage: A foreign event drop.

File key: "*.json"

Incremental: true
""".strip(),
            language=PYTHON,
        )

    def read(self):
        staging = self.staging_folder()
        for name, content in self.files.items():
            (staging.path / name).write_text(content, encoding="utf-8")
        return staging, self.deletes


class CUR__Event(Table):
    """The consumer. It owns the catalogue anchor a shortcut reads through."""

    def read(self):  # pragma: no cover - the load itself is not under test
        raise NotImplementedError


def _merged(*catalogues: Catalogue) -> Catalogue:
    """Several single-item catalogues as one, as a run reads the whole estate's."""

    rows: dict = {}
    for catalogue in catalogues:
        for item, tables in catalogue.rows.items():
            held = rows.setdefault(item, {})
            for name, entries in tables.items():
                held[name] = tuple(held.get(name, ())) + tuple(entries)
    return Catalogue(
        MappingProxyType(
            {item: MappingProxyType(tables) for item, tables in rows.items()}
        ),
        writer=Recording(),
    )


def _catalogue(*, consumer_bookmark: datetime | None = CONSUMER_LOADED) -> Catalogue:
    """The producer and the consumer, in one catalogue."""

    return _merged(
        installed(
            f"{SOURCE_SCHEMA}.{SOURCE_OBJECT}",
            at=SOURCE_LOADED,
            item=LANDING,
            target=LANDING_TARGET,
            files=True,
        ),
        installed(
            "CUR.Event", at=consumer_bookmark, item=CURATED, target=CURATED_TARGET
        ),
    )


class _Estate:
    """The producer Folder, the consumer, and the shortcut between them."""

    def __init__(self, source: Folder, consumer: Table) -> None:
        self.source = source
        self.consumer = consumer

    def shortcut(self, source: str | None = SOURCE):
        return FolderShortcut(schema=LOCAL_SCHEMA, object=LOCAL_OBJECT, source=source)(
            self.consumer
        )

    def place(self, name: str, content: str = "unmanaged") -> Path:
        """Put a file in the source Folder without recording a change."""

        target = self.source.path() / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return target

    def local(self, name: str = "") -> Path:
        """One file, as the consumer's shortcut path spells it."""

        return self.consumer.lakehouse.folder_path(LOCAL_SCHEMA, LOCAL_OBJECT) / name


@pytest.fixture
def estate(tmp_path):
    """A loaded source Folder and a consumer whose shortcut opens onto it."""

    LAND__SourceEvents.files = {}
    LAND__SourceEvents.deletes = ()
    catalogue = _catalogue()

    landing = mounted_lakehouse(LANDING_TARGET, tmp_path / "landing")
    curated = mounted_lakehouse(CURATED_TARGET, tmp_path / "curated")
    source = LAND__SourceEvents(object(), lakehouse=landing).with_catalogue(catalogue)
    consumer = CUR__Event(object(), lakehouse=curated).with_catalogue(catalogue)

    source.files = {"event-001.json": "one", "event-002.json": "two"}
    source.load()

    # The OneLake shortcut: a window in Curated onto the directory Landing owns,
    # reached by a path of the consumer's own.
    window = curated.folder_path(LOCAL_SCHEMA, LOCAL_OBJECT)
    window.parent.mkdir(parents=True, exist_ok=True)
    window.symlink_to(source.path(), target_is_directory=True)
    return _Estate(source, consumer)


def _relative(paths, root: Path) -> set[str]:
    return {str(Path(path).relative_to(root)) for path in paths}


# --- what a logical Folder shortcut reads ------------------------------------


@weaver_test()
def test_a_logical_folder_shortcut_reports_the_sources_history_locally(estate):
    """
    Intent: A consumer reads the producer's Folder change history through its own
    shortcut, and receives paths it can open.

    Proof: the same relative files and the same change datetimes the producer
    reports, rooted at the consumer's shortcut path.
    """

    shortcut = estate.shortcut()

    changed = shortcut.files_since(BOOKMARK_SENTINEL)
    direct = estate.source.files_since(BOOKMARK_SENTINEL)

    assert set(changed) == {
        estate.local("event-001.json"),
        estate.local("event-002.json"),
    }
    assert _relative(changed, estate.local("")) == _relative(
        direct, estate.source.path()
    )
    assert sorted(changed.values()) == sorted(direct.values())
    assert all(path.read_text(encoding="utf-8") for path in changed)


@weaver_test()
def test_a_logical_folder_shortcut_reports_the_newest_delivery(estate):
    estate.source.files = {"event-003.json": "three"}
    estate.source.load()

    latest = estate.shortcut().latest_files()

    assert set(latest) == {estate.local("event-003.json")}


@weaver_test()
def test_a_logical_folder_shortcut_reports_what_the_source_retired(estate):
    boundary = min(estate.source.files_since(BOOKMARK_SENTINEL).values())
    estate.source.files = {}
    estate.source.deletes = ("event-001.json",)
    estate.source.load()

    shortcut = estate.shortcut()

    assert set(shortcut.deleted_since(boundary)) == {estate.local("event-001.json")}
    assert set(shortcut.files_since(boundary)) == set()
    assert not estate.local("event-001.json").exists()


@weaver_test()
def test_a_file_no_change_document_records_is_not_in_the_shortcuts_history(estate):
    """Managed history is what ``_changes`` holds, on either side of a shortcut."""

    estate.place("event-999.json")

    shortcut = estate.shortcut()

    assert estate.local("event-999.json").is_file()
    assert estate.local("event-999.json") not in shortcut.files_since(BOOKMARK_SENTINEL)
    assert estate.local("event-999.json") not in shortcut.latest_files()


@weaver_test()
def test_a_folder_shortcut_addresses_its_own_item_for_data(estate):
    shortcut = estate.shortcut()

    assert shortcut.path() != estate.source.path()
    assert shortcut.path() == estate.local("")
    assert shortcut.spark_path().endswith(f"/{CURATED_TARGET}/Files/SRC/SourceEvents")


# --- the bookmark a shortcut answers with ------------------------------------


@weaver_test()
def test_a_logical_shortcut_reads_the_sources_bookmark(estate):
    """
    Intent: ``bookmark()`` on a shortcut is the source object's, so a consumer
    can ask how far the producer got.

    Proof: the instant the producer's own load recorded, and not the consumer's
    own bookmark.
    """

    assert estate.shortcut().bookmark() == estate.source.bookmark()
    # The fixture loaded the source, so the recorded instant has moved past the
    # one the catalogue was seeded with.
    assert estate.source.bookmark() > SOURCE_LOADED
    assert estate.consumer.bookmark() == CONSUMER_LOADED


@weaver_test()
def test_a_shortcut_bookmark_comes_from_the_owners_catalogue(tmp_path):
    """The shortcut opens none of its own: the owner is already anchored."""

    later = SOURCE_LOADED + timedelta(days=1)
    answers = []
    for at in (SOURCE_LOADED, later):
        consumer = CUR__Event(
            _Spark(),
            lakehouse=mounted_lakehouse(CURATED_TARGET, tmp_path / f"curated-{at:%d}"),
        ).with_catalogue(
            _merged(
                installed(
                    f"{SOURCE_SCHEMA}.{SOURCE_OBJECT}",
                    at=at,
                    item=LANDING,
                    target=LANDING_TARGET,
                    files=True,
                ),
                installed("CUR.Event", item=CURATED, target=CURATED_TARGET),
            )
        )
        shortcut = FolderShortcut(
            schema=LOCAL_SCHEMA, object=LOCAL_OBJECT, source=SOURCE
        )(consumer)
        answers.append(shortcut.bookmark())

    assert answers == [SOURCE_LOADED, later]


@weaver_test()
def test_an_unanchored_owner_fails_where_every_catalogue_read_fails(tmp_path):
    curated = mounted_lakehouse(CURATED_TARGET, tmp_path / "curated")
    consumer = CUR__Event(object(), lakehouse=curated)
    shortcut = FolderShortcut(schema=LOCAL_SCHEMA, object=LOCAL_OBJECT, source=SOURCE)(
        consumer
    )

    with pytest.raises(LoadError, match="not anchored to the Weaver catalogue"):
        shortcut.bookmark()


@weaver_test()
def test_a_source_no_clean_load_has_run_for_reads_as_the_sentinel(tmp_path):
    """The ordinary no-bookmark answer, so a first incremental read asks for all."""

    catalogue = _merged(
        installed(
            f"{SOURCE_SCHEMA}.{SOURCE_OBJECT}",
            item=LANDING,
            target=LANDING_TARGET,
            files=True,
        ),
        installed("CUR.Event", item=CURATED, target=CURATED_TARGET),
    )
    consumer = CUR__Event(
        object(), lakehouse=mounted_lakehouse(CURATED_TARGET, tmp_path / "curated")
    ).with_catalogue(catalogue)

    shortcut = FolderShortcut(schema=LOCAL_SCHEMA, object=LOCAL_OBJECT, source=SOURCE)(
        consumer
    )

    assert shortcut.bookmark() == BOOKMARK_SENTINEL


# --- what a physical shortcut answers ----------------------------------------


@weaver_test()
def test_a_physical_folder_shortcut_reads_files_and_fails_weaver_semantics(estate):
    """
    Intent: A physical shortcut names a Fabric location, so it stays a physical
    reader and says so when asked a Weaver question.

    Proof: both locations still answer, and each of the four Weaver-semantic
    methods fails naming the target type.
    """

    shortcut = estate.shortcut(source=None)

    assert shortcut.path() == estate.local("")
    assert shortcut.spark_path()

    for call in (
        shortcut.bookmark,
        shortcut.latest_files,
        lambda: shortcut.files_since(BOOKMARK_SENTINEL),
        lambda: shortcut.deleted_since(BOOKMARK_SENTINEL),
    ):
        with pytest.raises(LoadError, match="physical target"):
            call()


# --- what a Table shortcut reads ---------------------------------------------


class _Frame:
    def __init__(self, path: str) -> None:
        self.path = path
        self.rows = None

    def limit(self, rows: int) -> "_Frame":
        self.rows = rows
        return self


class _Spark:
    def __init__(self) -> None:
        self.loaded: list[str] = []
        self.read = self

    def format(self, _name):
        return self

    def load(self, path) -> _Frame:
        self.loaded.append(str(path))
        return _Frame(str(path))


@pytest.fixture
def table_owner(tmp_path):
    curated = mounted_lakehouse(CURATED_TARGET, tmp_path / "curated")
    return CUR__Event(_Spark(), lakehouse=curated).with_catalogue(_catalogue())


@weaver_test()
def test_a_logical_table_shortcut_reads_locally_and_bookmarks_from_the_source(
    table_owner,
):
    shortcut = TableShortcut(schema=LOCAL_SCHEMA, object=LOCAL_OBJECT, source=SOURCE)(
        table_owner
    )

    assert shortcut.dataframe().path.endswith(f"/Tables/{LOCAL_SCHEMA}/{LOCAL_OBJECT}")
    assert shortcut.empty_dataframe().rows == 0
    assert shortcut.bookmark() == SOURCE_LOADED


@weaver_test()
def test_a_physical_table_shortcut_reads_locally_and_fails_a_bookmark(table_owner):
    shortcut = TableShortcut(schema=LOCAL_SCHEMA, object=LOCAL_OBJECT)(table_owner)

    assert shortcut.dataframe().path
    assert shortcut.empty_dataframe().rows == 0
    with pytest.raises(LoadError, match="physical target"):
        shortcut.bookmark()


# --- a source that is a Folder and a Table at once ---------------------------


@weaver_test()
def test_a_folder_source_and_a_table_source_of_one_name_are_two_bookmarks(tmp_path):
    """The ``Files/`` prefix is part of the identity a shortcut carries.

    A Folder and a Table of one name are two objects with two bookmarks, so the
    source a shortcut names has to say which it means.
    """

    table_at = SOURCE_LOADED + timedelta(hours=3)
    folder_rows = installed(
        f"{SOURCE_SCHEMA}.{SOURCE_OBJECT}",
        at=SOURCE_LOADED,
        item=LANDING,
        target=LANDING_TARGET,
        files=True,
    )
    table_rows = installed(
        f"{SOURCE_SCHEMA}.{SOURCE_OBJECT}",
        at=table_at,
        item=LANDING,
        target=LANDING_TARGET,
    )
    consumer = CUR__Event(
        _Spark(), lakehouse=mounted_lakehouse(CURATED_TARGET, tmp_path / "curated")
    ).with_catalogue(
        _merged(
            folder_rows,
            table_rows,
            installed("CUR.Event", item=CURATED, target=CURATED_TARGET),
        )
    )

    folder = FolderShortcut(schema=LOCAL_SCHEMA, object=LOCAL_OBJECT, source=SOURCE)(
        consumer
    )
    table = TableShortcut(
        schema=LOCAL_SCHEMA,
        object=LOCAL_OBJECT,
        source=f"{LANDING}/{SOURCE_SCHEMA}.{SOURCE_OBJECT}",
    )(consumer)

    assert folder.bookmark() == SOURCE_LOADED
    assert table.bookmark() == table_at
