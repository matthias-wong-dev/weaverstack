"""The test wrapper declares markers and resource necessity."""

from __future__ import annotations

import pytest

from .weaver_test import WeaverTestDeclaration, weaver_test


@weaver_test()
def test_the_wrapper_exposes_its_declaration_and_generates_scope_markers():
    @weaver_test(remote=True, resources={"tds"})
    def declared():
        pass

    assert declared.__weaver_test_declaration__ == WeaverTestDeclaration(
        scope="remote", resources=frozenset({"tds"})
    )
    assert {mark.name for mark in getattr(declared, "pytestmark", ())} == {
        "fabric",
        "remote",
        "tds",
    }


@weaver_test()
def test_invalid_declarations_are_rejected():
    with pytest.raises(ValueError, match="one scope"):
        weaver_test(remote=True, hosted=True)
    with pytest.raises(ValueError, match="unknown Weaver test resource"):
        weaver_test(resources={"spark"})


@weaver_test()
@pytest.mark.parametrize(
    ("flag", "scope", "markers"),
    [
        ("remote", "remote", {"fabric", "remote"}),
        ("hosted", "hosted", {"fabric", "hosted"}),
        ("integration", "integration", {"fabric", "full_integration"}),
        ("provision", "provision", {"fabric", "provision"}),
    ],
)
def test_each_scope_is_one_declaration_with_matching_selection_markers(
    flag, scope, markers
):
    def candidate():
        pass

    declared = weaver_test(**{flag: True})(candidate)

    assert declared.__weaver_test_declaration__.scope == scope
    assert {mark.name for mark in declared.pytestmark} == markers


@weaver_test()
def test_integration_and_provision_need_no_position_dimension():
    assert weaver_test(integration=True)
    assert weaver_test(provision=True)


@weaver_test()
def test_every_declared_resource_generates_its_selection_marker():
    def candidate():
        pass

    declared = weaver_test(resources={"tds", "livy", "onelake", "rest"})(candidate)

    assert {mark.name for mark in declared.pytestmark} == {
        "tds",
        "livy",
        "onelake",
        "rest",
    }
