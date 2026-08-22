"""Folder publication evidence and the downstream change-feed contract."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from support.bookmarks import loaded, never
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Folder
from weaver.errors import LoadError
from weaver.runtime.folder_load import CHANGES_DIRECTORY, managed_relative_files


class Sales__Landing(Folder):
    files: dict[str, str] = {}
    deletes: tuple[str, ...] = ()
    incremental = True
    static = False

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        return parse_document(
            f"""
Folder ID: Sales.Landing

Description: Incoming sales files.

Lineage: Sales source.

File key: "**/*.csv"

Incremental: {str(self.incremental).lower()}

Static: {str(self.static).lower()}
""".strip(),
            language=PYTHON,
        )

    def read(self):
        staging = self.staging_folder()
        for name, content in self.files.items():
            target = staging.path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        if self.incremental:
            return staging, self.deletes
        return staging


@pytest.fixture
def landing(tmp_path):
    Sales__Landing.files = {}
    Sales__Landing.deletes = ()
    Sales__Landing.incremental = True
    Sales__Landing.static = False
    return Sales__Landing(
        object(), lakehouse=mounted_lakehouse("Sales", tmp_path)
    ).with_bookmarks(never())


def _documents(folder: Folder) -> list[Path]:
    root = folder.path() / CHANGES_DIRECTORY
    return sorted(root.glob("*.json")) if root.exists() else []


def _document(folder: Folder, index: int = -1) -> dict:
    return json.loads(_documents(folder)[index].read_text(encoding="utf-8"))


def _reject(folder: Folder) -> Path:
    return folder.path().with_name(f"{folder.path().name}_Reject")


def _write_document(folder: Folder, at: datetime, **changes) -> Path:
    root = folder.path() / CHANGES_DIRECTORY
    root.mkdir(parents=True, exist_ok=True)
    name = at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S.%fZ.json")
    path = root / name
    path.write_text(
        json.dumps(
            {
                "inserts": changes.get("inserts", []),
                "updates": changes.get("updates", []),
                "deletes": changes.get("deletes", []),
            }
        ),
        encoding="utf-8",
    )
    return path


@weaver_test()
def test_a_first_load_records_inserted_relative_paths(landing, monkeypatch):
    import weaver.runtime.folder_load as runtime

    changed_at = datetime(2026, 8, 20, 1, 22, 41, 123456, tzinfo=timezone.utc)
    monkeypatch.setattr(runtime, "_utc_now", lambda: changed_at)
    landing.files = {"June.csv": "june", "nested/July.csv": "july"}

    result = landing.load()

    assert result.rows_inserted == 2
    assert [path.name for path in _documents(landing)] == [
        "2026-08-20T01-22-41.123456Z.json"
    ]
    assert _document(landing) == {
        "inserts": ["June.csv", "nested/July.csv"],
        "updates": [],
        "deletes": [],
    }


@weaver_test()
def test_a_mixed_load_writes_one_document_with_actual_changes(landing):
    landing.files = {"June.csv": "old", "remove.csv": "old"}
    landing.load()
    before = len(_documents(landing))

    landing.files = {"June.csv": "new", "nested/July.csv": "new"}
    landing.deletes = ("remove.csv",)
    result = landing.load()

    assert (result.rows_inserted, result.rows_updated, result.rows_deleted) == (1, 1, 1)
    assert len(_documents(landing)) == before + 1
    assert _document(landing) == {
        "inserts": ["nested/July.csv"],
        "updates": ["June.csv"],
        "deletes": ["remove.csv"],
    }


@weaver_test()
def test_wholesale_omission_records_a_delete(landing):
    landing.incremental = False
    landing.files = {"keep.csv": "same", "remove.csv": "old"}
    landing.load()

    landing.files = {"keep.csv": "same"}
    result = landing.load()

    assert result.rows_deleted == 1
    assert _document(landing)["deletes"] == ["remove.csv"]


@weaver_test()
def test_an_identical_reload_adds_no_change_document(landing):
    landing.files = {"same.csv": "same"}
    landing.load()
    before = _documents(landing)

    result = landing.load()

    assert (result.rows_inserted, result.rows_updated, result.rows_deleted) == (0, 0, 0)
    assert _documents(landing) == before


@weaver_test()
def test_timestamp_collisions_do_not_replace_an_immutable_event(landing, monkeypatch):
    import weaver.runtime.folder_load as runtime

    fixed = datetime(2026, 8, 20, 1, 2, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(runtime, "_utc_now", lambda: fixed)
    landing.files = {"one.csv": "one"}
    landing.load()
    landing.files = {"two.csv": "two"}
    landing.load()

    assert [path.name for path in _documents(landing)] == [
        "2026-08-20T01-02-03.000000Z.json",
        "2026-08-20T01-02-03.000001Z.json",
    ]


@weaver_test()
def test_an_intolerant_rejection_preserves_evidence_and_publishes_nothing(landing):
    landing.files = {"good.csv": "good", "nested/bad.txt": "bad"}

    with pytest.raises(LoadError, match="rejected"):
        landing.load(fault_tolerant=False)

    assert not (landing.path() / "good.csv").exists()
    assert (_reject(landing) / "nested/bad.txt").read_text(encoding="utf-8") == "bad"
    assert not _documents(landing)
    assert (landing._staging_path() / "nested/bad.txt").exists()


@weaver_test()
def test_a_tolerant_rejection_publishes_only_matching_files(landing):
    landing.files = {"good.csv": "good", "nested/bad.txt": "bad"}

    result = landing.load(fault_tolerant=True)

    assert result.succeeded is False
    assert result.rows_rejected == 1
    assert (landing.path() / "good.csv").exists()
    assert not (landing.path() / "nested/bad.txt").exists()
    assert (_reject(landing) / "nested/bad.txt").exists()
    assert _document(landing) == {
        "inserts": ["good.csv"],
        "updates": [],
        "deletes": [],
    }


@weaver_test()
def test_a_clean_load_removes_stale_reject_evidence(landing):
    landing.files = {"bad.txt": "bad"}
    landing.load(fault_tolerant=True)
    assert (_reject(landing) / "bad.txt").exists()

    landing.files = {"good.csv": "good"}
    landing.load()

    assert not _reject(landing).exists()


@weaver_test()
def test_manual_unmanaged_files_are_left_alone(landing):
    landing.incremental = False
    landing.path().mkdir(parents=True)
    (landing.path() / "manual.txt").write_text("external", encoding="utf-8")
    landing.files = {"managed.csv": "managed"}

    landing.load()

    assert (landing.path() / "manual.txt").read_text(encoding="utf-8") == "external"
    assert "manual.txt" not in _document(landing)["inserts"]


@weaver_test()
def test_object_code_cannot_stage_inside_changes(landing):
    landing.files = {"_changes/spoof.csv": "not Weaver"}

    with pytest.raises(LoadError, match="cannot be staged"):
        landing.load()

    assert not _documents(landing)


@weaver_test()
def test_object_code_cannot_explicitly_delete_changes(landing):
    landing.deletes = ("_changes/existing.csv",)

    with pytest.raises(LoadError, match="cannot be deleted"):
        landing.load()


@weaver_test()
def test_wholesale_reconciliation_never_inventories_changes(landing):
    landing.incremental = False
    metadata = landing.path() / CHANGES_DIRECTORY / "keep.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text("metadata", encoding="utf-8")
    landing.files = {"current.csv": "current"}

    landing.load()

    assert metadata.read_text(encoding="utf-8") == "metadata"
    assert "_changes/keep.json" not in managed_relative_files(landing.path(), ("**/*",))


@weaver_test()
def test_a_bookmarked_static_folder_creates_no_later_event(landing):
    landing.static = True
    landing.files = {"seed.csv": "seed"}
    landing.load()
    before = _documents(landing)

    already = Sales__Landing(object(), lakehouse=landing.lakehouse).with_bookmarks(
        loaded("Sales.Landing", files=True)
    )
    already.static = True
    already.files = {"seed.csv": "changed"}
    already.load()

    assert _documents(landing) == before


@weaver_test()
def test_a_change_document_failure_reports_failure_after_publication(
    landing, monkeypatch
):
    import weaver.runtime.folder_load as runtime

    monkeypatch.setattr(
        runtime,
        "_safe_write_text",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("metadata failed")),
    )
    landing.files = {"published.csv": "published"}

    with pytest.raises(OSError, match="metadata failed"):
        landing.load()

    assert (landing.path() / "published.csv").exists()
    assert not _documents(landing)
    assert (landing._staging_path() / "published.csv").exists()


#: Every change-helper test measures from this one bookmark.
BOOKMARK = datetime(2026, 8, 20, tzinfo=timezone.utc)


def _at(seconds: int | float) -> datetime:
    return BOOKMARK + timedelta(seconds=seconds)


def _place(folder: Folder, *names: str) -> None:
    """Put current files in the destination without recording a change."""

    for name in names:
        target = folder.path() / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("current", encoding="utf-8")


@weaver_test()
def test_a_load_is_reportable_through_every_change_helper(landing, monkeypatch):
    import weaver.runtime.folder_load as runtime

    changed_at = datetime(2026, 8, 20, 1, 22, 41, 123456, tzinfo=timezone.utc)
    monkeypatch.setattr(runtime, "_utc_now", lambda: changed_at)
    landing.files = {"June.csv": "june"}
    landing.load()
    before = changed_at - timedelta(seconds=1)

    assert landing.files_since(before) == {landing.path() / "June.csv": changed_at}
    assert landing.latest_files() == {landing.path() / "June.csv": changed_at}
    assert landing.deleted_since(before) == {}


@weaver_test()
def test_files_since_maps_each_inserted_file_to_its_change_datetime(landing):
    _write_document(landing, _at(1), inserts=["June.csv", "nested/July.csv"])
    _place(landing, "June.csv", "nested/July.csv")

    changed = landing.files_since(BOOKMARK)

    assert changed == {
        landing.path() / "June.csv": _at(1),
        landing.path() / "nested/July.csv": _at(1),
    }
    assert all(isinstance(path, Path) and path.is_absolute() for path in changed)
    assert all(at.utcoffset() == timedelta(0) for at in changed.values())


@weaver_test()
def test_files_since_iterates_as_paths_a_consumer_can_read(landing):
    _write_document(landing, _at(1), inserts=["June.csv"])
    _place(landing, "June.csv")

    for path in landing.files_since(BOOKMARK):
        assert isinstance(path, Path)
        assert path.read_text(encoding="utf-8") == "current"


@weaver_test()
def test_files_since_collapses_repeated_changes_to_the_latest_datetime(landing):
    _write_document(landing, _at(1), inserts=["June.csv"])
    _write_document(landing, _at(2), updates=["June.csv"])
    _write_document(landing, _at(3), updates=["June.csv"])
    _place(landing, "June.csv")

    assert landing.files_since(BOOKMARK) == {landing.path() / "June.csv": _at(3)}


@weaver_test()
def test_files_since_omits_a_file_deleted_after_its_update(landing):
    _write_document(landing, _at(1), updates=["June.csv"])
    _write_document(landing, _at(2), deletes=["June.csv"])

    assert landing.files_since(BOOKMARK) == {}


@weaver_test()
def test_files_since_reports_a_file_reinserted_after_a_delete(landing):
    _write_document(landing, _at(1), deletes=["June.csv"])
    _write_document(landing, _at(2), inserts=["June.csv"])
    _place(landing, "June.csv")

    assert landing.files_since(BOOKMARK) == {landing.path() / "June.csv": _at(2)}


@weaver_test()
def test_files_since_omits_a_file_removed_outside_weaver(landing):
    _write_document(landing, _at(1), inserts=["externally-removed.csv"])

    assert landing.files_since(BOOKMARK) == {}


@weaver_test()
def test_files_since_uses_a_strict_boundary_without_reading_older_documents(landing):
    for at in (_at(-1), BOOKMARK):
        _write_document(landing, at, updates=["earlier.csv"]).write_text(
            "not json", encoding="utf-8"
        )
    _write_document(landing, _at(0.000001), inserts=["new.csv"])
    _place(landing, "earlier.csv", "new.csv")

    assert landing.files_since(BOOKMARK) == {landing.path() / "new.csv": _at(0.000001)}


@weaver_test()
def test_files_since_reports_nothing_when_no_document_is_later(landing):
    _write_document(landing, _at(-1), inserts=["old.csv"]).write_text(
        "not json", encoding="utf-8"
    )
    _place(landing, "old.csv")

    assert landing.files_since(BOOKMARK) == {}


@weaver_test()
def test_files_since_refuses_a_malformed_later_document(landing):
    _write_document(landing, _at(1), inserts=["new.csv"]).write_text(
        "not json", encoding="utf-8"
    )

    with pytest.raises(LoadError, match="cannot read Folder change document"):
        landing.files_since(BOOKMARK)


@weaver_test()
def test_deleted_since_maps_each_deletion_to_its_datetime(landing):
    _write_document(landing, _at(1), updates=["June.csv"])
    _write_document(landing, _at(2), deletes=["June.csv", "May.csv"])

    deleted = landing.deleted_since(BOOKMARK)

    assert deleted == {
        landing.path() / "June.csv": _at(2),
        landing.path() / "May.csv": _at(2),
    }
    assert not any(path.exists() for path in deleted)


@weaver_test()
def test_deleted_since_omits_a_file_reinserted_after_its_delete(landing):
    _write_document(landing, _at(1), deletes=["June.csv"])
    _write_document(landing, _at(2), inserts=["June.csv"])
    _place(landing, "June.csv")

    assert landing.deleted_since(BOOKMARK) == {}


@weaver_test()
def test_deleted_since_uses_a_strict_boundary_without_reading_older_documents(landing):
    _write_document(landing, BOOKMARK, deletes=["earlier.csv"]).write_text(
        "not json", encoding="utf-8"
    )
    _write_document(landing, _at(0.000001), deletes=["June.csv"])

    assert landing.deleted_since(BOOKMARK) == {
        landing.path() / "June.csv": _at(0.000001)
    }


@weaver_test()
def test_a_change_bookmark_must_be_timezone_aware(landing):
    for helper in (landing.files_since, landing.deleted_since):
        with pytest.raises(LoadError, match="timezone-aware"):
            helper(datetime(2026, 8, 20))


@weaver_test()
def test_latest_files_reports_every_surviving_file_from_the_newest_change(landing):
    _write_document(landing, _at(1), inserts=["a.csv"])
    _write_document(landing, _at(2), inserts=["b.csv"], updates=["a.csv"])
    _place(landing, "a.csv", "b.csv")

    assert landing.latest_files() == {
        landing.path() / "a.csv": _at(2),
        landing.path() / "b.csv": _at(2),
    }


@weaver_test()
def test_latest_files_reads_no_document_older_than_the_newest_survivor(landing):
    _write_document(landing, _at(1), inserts=["old.csv"]).write_text(
        "not json", encoding="utf-8"
    )
    _write_document(landing, _at(2), inserts=["new.csv"])
    _place(landing, "new.csv", "old.csv")

    assert landing.latest_files() == {landing.path() / "new.csv": _at(2)}


@weaver_test()
def test_latest_files_skips_newer_deletion_only_changes(landing):
    _write_document(landing, _at(1), inserts=["a.csv", "b.csv"])
    _write_document(landing, _at(2), deletes=["other.csv"])
    _write_document(landing, _at(3), deletes=["old.csv"])
    _place(landing, "a.csv", "b.csv")

    assert landing.latest_files() == {
        landing.path() / "a.csv": _at(1),
        landing.path() / "b.csv": _at(1),
    }


@weaver_test()
def test_latest_files_suppresses_a_file_a_newer_change_deleted(landing):
    _write_document(landing, _at(1), inserts=["a.csv", "b.csv"])
    _write_document(landing, _at(2), deletes=["b.csv"])
    _place(landing, "a.csv")

    assert landing.latest_files() == {landing.path() / "a.csv": _at(1)}


@weaver_test()
def test_latest_files_keeps_scanning_when_a_newer_change_left_nothing(landing):
    _write_document(landing, _at(1), inserts=["kept.csv"])
    _write_document(landing, _at(2), inserts=["retired.csv"])
    _write_document(landing, _at(3), deletes=["retired.csv"])
    _place(landing, "kept.csv")

    assert landing.latest_files() == {landing.path() / "kept.csv": _at(1)}


@weaver_test()
def test_latest_files_keeps_scanning_past_a_file_removed_outside_weaver(landing):
    _write_document(landing, _at(1), inserts=["kept.csv"])
    _write_document(landing, _at(2), inserts=["removed.csv"])
    _place(landing, "kept.csv")

    assert landing.latest_files() == {landing.path() / "kept.csv": _at(1)}


@weaver_test()
def test_latest_files_refuses_a_malformed_newest_document(landing):
    _write_document(landing, _at(1), inserts=["new.csv"]).write_text(
        "not json", encoding="utf-8"
    )

    with pytest.raises(LoadError, match="cannot read Folder change document"):
        landing.latest_files()


@weaver_test()
def test_change_metadata_is_empty_for_a_folder_weaver_has_not_written(landing):
    assert landing.files_since(BOOKMARK) == {}
    assert landing.deleted_since(BOOKMARK) == {}
    assert landing.latest_files() == {}


@weaver_test()
def test_change_metadata_is_empty_when_no_file_matches_the_file_key(landing):
    landing.path().mkdir(parents=True)
    (landing.path() / "manual.txt").write_text("outside the key", encoding="utf-8")

    assert landing.files_since(BOOKMARK) == {}
    assert landing.deleted_since(BOOKMARK) == {}
    assert landing.latest_files() == {}


@weaver_test()
def test_change_metadata_is_refused_for_managed_files_with_no_history(landing):
    _place(landing, "manual.csv")
    expected = "Sales.Landing: Folder change metadata is unavailable"

    for call in (
        lambda: landing.files_since(BOOKMARK),
        lambda: landing.deleted_since(BOOKMARK),
        landing.latest_files,
    ):
        with pytest.raises(LoadError, match=expected):
            call()
