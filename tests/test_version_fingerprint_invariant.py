from __future__ import annotations

import re
import subprocess
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from support.weaver_test import weaver_test


def _load_hatch_build():
    """Load Hatch's root-only version source without relying on ``sys.path``."""

    source = Path(__file__).resolve().parents[1] / "hatch_build.py"
    spec = spec_from_file_location("weaver_hatch_build", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load Weaver's version source from {source}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hatch_build = _load_hatch_build()


def _git(repository: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def version_repository(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init")
    _git(repository, "config", "user.name", "Weaver Tests")
    _git(repository, "config", "user.email", "weaver@example.invalid")
    _git(repository, "config", "core.autocrlf", "false")
    (repository / ".gitattributes").write_text("*.txt text eol=lf\n", encoding="utf-8")
    (repository / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    (repository / "tracked.txt").write_text("original\n", encoding="utf-8")
    (repository / "binary.dat").write_bytes(b"\x00\x01original\xff")
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", "initial")
    _git(repository, "tag", "v0.1.0")
    monkeypatch.setattr(hatch_build, "PROJECT_ROOT", repository)
    return repository


@weaver_test()
def test_clean_tag_is_the_release_version(version_repository: Path):
    assert hatch_build.compute_version() == "0.1.0"


@weaver_test()
def test_fingerprint_is_stable_and_staging_does_not_change_it(
    version_repository: Path,
):
    tracked = version_repository / "tracked.txt"
    tracked.write_text("changed\n", encoding="utf-8")

    before_staging = hatch_build.compute_version()
    assert before_staging == hatch_build.compute_version()
    assert re.fullmatch(r"0\.1\.1\.dev\d+", before_staging)

    _git(version_repository, "add", "tracked.txt")
    assert hatch_build.compute_version() == before_staging

    # Even an index that differs from both HEAD and the working source is not
    # source identity. The working source is exactly the tagged tree again.
    tracked.write_text("original\n", encoding="utf-8")
    assert hatch_build.compute_version() == "0.1.0"


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

    assert first != "0.1.0"
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
