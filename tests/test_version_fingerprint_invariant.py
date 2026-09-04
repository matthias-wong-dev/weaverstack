"""One authored version, and what a checkout is allowed to build from it.

`VERSION` names the release line. A tag grants permission to drop the `.dev`
suffix and never changes which line the code is on, so these tests pin both
halves: the fingerprint's source sensitivity, and the release tag's agreement
with the file.
"""

from __future__ import annotations

import re
import subprocess
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

ROOT = Path(__file__).resolve().parents[1]


def _load_hatch_build():
    """Load Hatch's root-only version source without relying on ``sys.path``."""

    source = ROOT / "hatch_build.py"
    spec = spec_from_file_location("weaver_hatch_build", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load Weaver's version source from {source}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hatch_build = _load_hatch_build()

#: What the fixture repository declares, so a test can tag against it.
DECLARED = "0.9.0"


def _git(repository: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def version_repository(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A repository declaring ``DECLARED``, committed and untagged."""

    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init")
    _git(repository, "config", "user.name", "Weaver Tests")
    _git(repository, "config", "user.email", "weaver@example.invalid")
    _git(repository, "config", "core.autocrlf", "false")
    (repository / ".gitattributes").write_text("*.txt text eol=lf\n", encoding="utf-8")
    (repository / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    (repository / "VERSION").write_text(f"{DECLARED}\n", encoding="utf-8")
    (repository / "tracked.txt").write_text("original\n", encoding="utf-8")
    (repository / "binary.dat").write_bytes(b"\x00\x01original\xff")
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", "initial")
    monkeypatch.setattr(hatch_build, "PROJECT_ROOT", repository)
    return repository


# --- the authored version ------------------------------------------------------


@weaver_test()
def test_the_repository_declares_a_usable_version():
    """This repository's own VERSION parses, so a release can be cut from it."""

    from packaging.version import Version

    declared = hatch_build.declared_version()

    assert Version(declared)
    assert (ROOT / "VERSION").read_text(encoding="utf-8").strip() == declared


@weaver_test()
def test_the_declared_version_is_read_from_the_file(version_repository: Path):
    assert hatch_build.declared_version() == DECLARED


@weaver_test()
def test_a_missing_version_file_is_refused(version_repository: Path):
    (version_repository / "VERSION").unlink()

    with pytest.raises(RuntimeError, match="VERSION is missing"):
        hatch_build.declared_version()


@pytest.mark.parametrize("written", ["not-a-version", "", "0.9.x"])
@weaver_test()
def test_an_unparseable_version_is_refused(version_repository: Path, written: str):
    (version_repository / "VERSION").write_text(f"{written}\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="PEP 440"):
        hatch_build.declared_version()


@pytest.mark.parametrize("written", ["v0.9.0", "0.09.0", "1.0.0-alpha1", "1.0.0.RC1"])
@weaver_test()
def test_a_non_canonical_version_is_refused(version_repository: Path, written: str):
    """A tag built from a non-canonical spelling would not match the wheel."""

    (version_repository / "VERSION").write_text(f"{written}\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="canonical spelling"):
        hatch_build.declared_version()


@pytest.mark.parametrize("written", ["0.10.0", "1.0.0", "2.1.3", "1.0.0rc1"])
@weaver_test()
def test_ordinary_future_versions_are_accepted(version_repository: Path, written: str):
    (version_repository / "VERSION").write_text(f"{written}\n", encoding="utf-8")

    assert hatch_build.declared_version() == written


# --- what a checkout builds ----------------------------------------------------


@weaver_test()
def test_an_untagged_checkout_is_a_development_build(version_repository: Path):
    version = hatch_build.compute_version()

    assert re.fullmatch(rf"{re.escape(DECLARED)}\.dev\d+", version)


@weaver_test()
def test_a_clean_checkout_at_the_matching_tag_is_the_release(
    version_repository: Path,
):
    """Tagging changes no source, so the tagged tree is the tested tree."""

    before = hatch_build.compute_version()
    _git(version_repository, "tag", f"v{DECLARED}")

    assert before.startswith(f"{DECLARED}.dev")
    assert hatch_build.compute_version() == DECLARED


@pytest.mark.parametrize("tag", ["v0.9.1", "v0.8.9", "v1.0.0"])
@weaver_test()
def test_a_tag_that_does_not_match_the_file_is_refused(
    version_repository: Path, tag: str
):
    """A tag must never move the release line the source says it is on."""

    _git(version_repository, "tag", tag)

    with pytest.raises(RuntimeError, match=f"would be v{re.escape(DECLARED)}"):
        hatch_build.compute_version()


@weaver_test()
def test_the_matching_tag_wins_when_the_commit_carries_others(
    version_repository: Path,
):
    _git(version_repository, "tag", "v0.9.1")
    _git(version_repository, "tag", f"v{DECLARED}")

    assert hatch_build.compute_version() == DECLARED


@weaver_test()
def test_a_dirty_checkout_at_the_matching_tag_is_not_the_release(
    version_repository: Path,
):
    """Edited source is not what the tag covered, whatever the tag says."""

    _git(version_repository, "tag", f"v{DECLARED}")
    (version_repository / "tracked.txt").write_text("edited\n", encoding="utf-8")

    version = hatch_build.compute_version()

    assert version != DECLARED
    assert re.fullmatch(rf"{re.escape(DECLARED)}\.dev\d+", version)


@weaver_test()
def test_a_dirty_checkout_at_a_mismatched_tag_is_a_development_build(
    version_repository: Path,
):
    """The tag only has to agree where it would authorise a release."""

    _git(version_repository, "tag", "v0.9.1")
    (version_repository / "tracked.txt").write_text("edited\n", encoding="utf-8")

    assert re.fullmatch(
        rf"{re.escape(DECLARED)}\.dev\d+", hatch_build.compute_version()
    )


@weaver_test()
def test_changing_the_declared_line_moves_the_development_version(
    version_repository: Path,
):
    """The next patch is one ordinary source change to VERSION."""

    (version_repository / "VERSION").write_text("0.9.1\n", encoding="utf-8")

    assert hatch_build.compute_version().startswith("0.9.1.dev")


# --- the source fingerprint ----------------------------------------------------


@weaver_test()
def test_fingerprint_is_stable_and_staging_does_not_change_it(
    version_repository: Path,
):
    _git(version_repository, "tag", f"v{DECLARED}")
    tracked = version_repository / "tracked.txt"
    tracked.write_text("changed\n", encoding="utf-8")

    before_staging = hatch_build.compute_version()
    assert before_staging == hatch_build.compute_version()
    assert re.fullmatch(rf"{re.escape(DECLARED)}\.dev\d+", before_staging)

    _git(version_repository, "add", "tracked.txt")
    assert hatch_build.compute_version() == before_staging

    # Even an index that differs from both HEAD and the working source is not
    # source identity. The working source is exactly the tagged tree again.
    tracked.write_text("original\n", encoding="utf-8")
    assert hatch_build.compute_version() == DECLARED


@weaver_test()
def test_tracked_text_and_binary_changes_move_the_fingerprint(
    version_repository: Path,
):
    tracked = version_repository / "tracked.txt"
    tracked.write_text("first\n", encoding="utf-8")
    first = hatch_build.compute_version()
    tracked.write_text("second\n", encoding="utf-8")
    second = hatch_build.compute_version()

    binary = version_repository / "binary.dat"
    binary.write_bytes(b"\x00\x01changed\xff")
    third = hatch_build.compute_version()

    assert len({first, second, third}) == 3


@weaver_test()
def test_untracked_paths_and_contents_move_the_fingerprint(
    version_repository: Path,
):
    untracked = version_repository / "new.txt"
    untracked.write_text("first\n", encoding="utf-8")
    first = hatch_build.compute_version()
    untracked.write_text("second\n", encoding="utf-8")
    second = hatch_build.compute_version()

    assert second != first

    _git(version_repository, "add", "new.txt")
    assert hatch_build.compute_version() == second


@weaver_test()
def test_ignored_files_do_not_move_the_version(version_repository: Path):
    before = hatch_build.compute_version()
    ignored = version_repository / "ignored"
    ignored.mkdir()
    (ignored / "build.log").write_text("different on every machine\n", encoding="utf-8")

    assert hatch_build.compute_version() == before


@weaver_test()
def test_clean_filters_make_crlf_and_lf_the_same_source(version_repository: Path):
    untracked = version_repository / "new.txt"
    untracked.write_bytes(b"same\r\nsource\r\n")
    windows = hatch_build.compute_version()
    untracked.write_bytes(b"same\nsource\n")
    unix = hatch_build.compute_version()

    assert windows == unix
