"""The test wrapper declares markers and resource necessity."""

from __future__ import annotations

import pytest

from .weaver_test import WeaverTestDeclaration, weaver_test


def test_the_wrapper_exposes_its_declaration_and_generates_scope_markers():
    @weaver_test(remote=True, resources={"tds"})
    def declared():
        pass

    assert declared.__weaver_test_declaration__ == WeaverTestDeclaration(
        position="remote", resources=frozenset({"tds"})
    )
    assert {mark.name for mark in getattr(declared, "pytestmark", ())} == {
        "fabric",
        "remote",
    }


def test_invalid_declarations_are_rejected():
    with pytest.raises(ValueError, match="both remote and hosted"):
        weaver_test(remote=True, hosted=True)
    with pytest.raises(ValueError, match="unknown Weaver test resource"):
        weaver_test(resources={"spark"})
    with pytest.raises(ValueError, match="need remote=True or hosted=True"):
        weaver_test(integration=True)
