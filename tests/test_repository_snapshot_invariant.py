"""Every incoming repository is copied before it is parsed.

The contract is one sentence — a build reads a temporary snapshot and never the
caller's own tree — and it is worth proving rather than reading, because the
failure it prevents is silent. A build that parsed the source directly would
describe a repository that never existed as a whole the moment anyone edited a
file while it ran, and the bundle would be internally inconsistent with nothing
to show for it.

The tests below therefore assert the *observable* consequences: the parse reads
a path that is not the source, mutating the source afterwards changes neither
the parsed repository nor the generated bundle, and a filesystem source gets no
shortcut around any of it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver.build_bundle.workflow import (
    _snapshot_name,
    _temp_copy,
    prepare_repository,
)
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.store import FilesystemStore

from test_item_repository import _estate


def test_a_filesystem_source_is_copied_rather_than_read_in_place(tmp_path):
    """The bypass this remediation removed: same filesystem is not a shortcut."""

    root = _estate(tmp_path)

    with _temp_copy(
        Location(str(root)), FilesystemStore(), prefix="weaver-test-"
    ) as snapshot:
        assert snapshot != root.resolve()
        assert snapshot.is_dir()
        assert (snapshot / "Lakehouse/Raw/Sales__Customer.py").is_file()


def test_the_snapshot_is_removed_when_the_operation_completes(tmp_path):
    root = _estate(tmp_path)

    with _temp_copy(
        Location(str(root)), FilesystemStore(), prefix="weaver-test-"
    ) as snapshot:
        taken = snapshot

    assert not taken.exists()
    assert root.is_dir(), "the source itself is never touched"


def test_the_snapshot_is_removed_when_the_operation_fails(tmp_path):
    root = _estate(tmp_path)
    taken = None

    with pytest.raises(RuntimeError):
        with _temp_copy(
            Location(str(root)), FilesystemStore(), prefix="weaver-test-"
        ) as snapshot:
            taken = snapshot
            raise RuntimeError("the build failed partway")

    assert taken is not None and not taken.exists()


def test_parsing_reads_the_snapshot_and_not_the_original(tmp_path):
    root = _estate(tmp_path)

    with prepare_repository(
        Location(str(root)), source_store=FilesystemStore()
    ) as prepared:
        parsed_root = Path(prepared.repository.root.value)

    assert parsed_root != root.resolve()
    assert str(root.resolve()) not in str(parsed_root)


def test_editing_the_source_after_the_snapshot_does_not_change_the_repository(
    tmp_path,
):
    """The whole point: the build's view of the source is fixed at snapshot time."""

    root = _estate(tmp_path)
    document = root / "Lakehouse/Raw/Sales__Customer.py"

    with prepare_repository(
        Location(str(root)), source_store=FilesystemStore()
    ) as prepared:
        before = prepared.repository.signature

        document.write_text("# edited while the build was running\n", encoding="utf-8")
        (root / "Lakehouse/Raw/Sales__Injected.py").write_text(
            "# a document that appeared mid-build\n", encoding="utf-8"
        )

        assert prepared.repository.signature == before
        identities = {str(identity) for identity in prepared.repository.source_documents}
        assert "Lakehouse/Raw/Sales.Injected" not in identities


def test_a_repository_removed_mid_build_is_still_readable_from_the_snapshot(tmp_path):
    """A snapshot the source can outlive is a snapshot in more than name."""

    import shutil

    root = _estate(tmp_path)

    with prepare_repository(
        Location(str(root)), source_store=FilesystemStore()
    ) as prepared:
        shutil.rmtree(root)

        assert not root.exists()
        assert prepared.store.exists(prepared.repository.root)
        assert prepared.repository.items


def test_a_missing_source_is_refused_before_anything_is_copied(tmp_path):
    with pytest.raises(BuildError, match="source does not exist"):
        with _temp_copy(
            Location(str(tmp_path / "absent")),
            FilesystemStore(),
            prefix="weaver-test-",
        ):
            pass


def test_a_file_source_is_refused_as_not_a_directory(tmp_path):
    document = tmp_path / "not-a-repository.py"
    document.write_text("# a file\n", encoding="utf-8")

    with pytest.raises(BuildError, match="not a directory"):
        with _temp_copy(
            Location(str(document)), FilesystemStore(), prefix="weaver-test-"
        ):
            pass


# --- naming the snapshot ------------------------------------------------------
#
# `.` is how a desktop caller names the repository they are standing in, and it
# is the default the public build applies outside Fabric. Joined onto the
# temporary root unresolved it would name the root itself, so the copy would
# fail or land a level too high — which is exactly the bug the filesystem bypass
# used to hide.


@pytest.mark.parametrize("spelling", [".", "./", "../Estate"])
def test_a_relative_source_is_resolved_before_it_names_the_snapshot(
    tmp_path, monkeypatch, spelling
):
    root = _estate(tmp_path)
    monkeypatch.chdir(root)

    assert _snapshot_name(Location(spelling)) == "Estate"


def test_a_relative_source_actually_snapshots(tmp_path, monkeypatch):
    root = _estate(tmp_path)
    monkeypatch.chdir(root)

    with prepare_repository(
        Location("."), source_store=FilesystemStore()
    ) as prepared:
        assert prepared.repository.name == "Estate"
        assert Path(prepared.repository.root.value) != root.resolve()


def test_a_source_with_no_final_segment_still_names_a_directory():
    assert _snapshot_name(Location("/")) == "repository"


def test_a_url_source_keeps_its_own_final_segment():
    assert _snapshot_name(Location("abfss://ws@onelake.example.com/Files/Estate")) == (
        "Estate"
    )
