"""``SesFixture.disposable()``, the estate a test is allowed to edit.

Some suites have to prove that a changed document really moves the catalogue;
without that, "nothing was rebuilt" and "the build does nothing" are the same
observation. So an estate has to be editable, and the estates live in
``tests/fixtures``, which is repository source.

Editing repository source from a test is worse here than untidy.
``hatch_build.compute_version()`` fingerprints the working tree, so a suite that
leaves a fixture modified changes the version the next build believes it is:
the test run alters what it is testing.

The fix is a copy, and this asserts the two properties a copy has to have. The
edit lands, and the original is untouched, without shelling out to Git, which
would test the tool rather than the helper.
"""

from __future__ import annotations

from support.build_envs import MIXED_ESTATE_FIXTURE
from support.weaver_test import weaver_test


def _documents(root):
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@weaver_test()
def test_the_copy_is_somewhere_else(tmp_path):
    """A disposable estate is not the checked-in one under another name."""

    copy = MIXED_ESTATE_FIXTURE.disposable(tmp_path)

    assert copy.path != MIXED_ESTATE_FIXTURE.path
    assert tmp_path in copy.path.parents


@weaver_test()
def test_the_copy_is_the_whole_estate(tmp_path):
    """Every document, byte for byte, a build reads all of them."""

    copy = MIXED_ESTATE_FIXTURE.disposable(tmp_path)

    assert _documents(copy.path) == _documents(MIXED_ESTATE_FIXTURE.path)


@weaver_test()
def test_the_copy_carries_the_binding(tmp_path):
    """Which items an estate binds is part of the fixture, not of its tree."""

    copy = MIXED_ESTATE_FIXTURE.disposable(tmp_path)

    assert copy.items == MIXED_ESTATE_FIXTURE.items
    assert copy.lakehouse_names == MIXED_ESTATE_FIXTURE.lakehouse_names
    assert copy.name == MIXED_ESTATE_FIXTURE.name


@weaver_test()
def test_editing_the_copy_leaves_the_original_alone(tmp_path):
    """The invariant the whole helper exists for."""

    original = _documents(MIXED_ESTATE_FIXTURE.path)
    copy = MIXED_ESTATE_FIXTURE.disposable(tmp_path)

    document = next(copy.path.rglob("*.py"))
    document.write_text("# edited by a test\n", encoding="utf-8")

    assert _documents(MIXED_ESTATE_FIXTURE.path) == original
    assert document.read_text(encoding="utf-8") == "# edited by a test\n"


@weaver_test()
def test_bytecode_caches_are_not_carried_over(tmp_path):
    """A copied ``__pycache__`` would be one run's compilation in another's estate."""

    copy = MIXED_ESTATE_FIXTURE.disposable(tmp_path)

    assert not list(copy.path.rglob("__pycache__"))
    assert not list(copy.path.rglob("*.pyc"))


@weaver_test()
def test_copying_twice_into_one_root_is_the_same_estate(tmp_path):
    """A module-scoped fixture may be resolved more than once per root."""

    first = MIXED_ESTATE_FIXTURE.disposable(tmp_path)
    document = next(first.path.rglob("*.py"))
    document.write_text("# edited by a test\n", encoding="utf-8")

    second = MIXED_ESTATE_FIXTURE.disposable(tmp_path)

    assert second.path == first.path
    assert document.read_text(encoding="utf-8") == "# edited by a test\n"
