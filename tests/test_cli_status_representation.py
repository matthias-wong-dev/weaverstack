"""One semantic vocabulary colours CLI status words."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver_cli.status import GREEN, RED, YELLOW, semantic_colour, status_symbol


@pytest.mark.parametrize("word", ["green", "passed", "succeeded"])
@weaver_test()
def test_success_statuses_are_green(word):
    assert semantic_colour(word) == GREEN


@pytest.mark.parametrize(
    "word",
    [
        "amber",
        "warning",
        "pending",
        "blocked",
        "rejected",
        "partially_succeeded",
        "succeeded_with_rejects",
    ],
)
@weaver_test()
def test_attention_statuses_are_yellow(word):
    assert semantic_colour(word) == YELLOW


@pytest.mark.parametrize("word", ["red", "failed", "invalid", "error"])
@weaver_test()
def test_failure_statuses_are_red(word):
    assert semantic_colour(word) == RED


@weaver_test()
def test_symbols_are_independent_of_colour_family():
    assert status_symbol("succeeded") == "✓"
    assert status_symbol("passed") == "✓"
    assert status_symbol("failed") == "✗"
    assert status_symbol("invalid") == "✗"
    assert status_symbol("pending") == " "
    assert status_symbol("blocked") == " "
