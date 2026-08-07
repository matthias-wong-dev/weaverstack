"""Where one SQL statement ends and the next begins.

Pure lexical claims, and the whole reason they are made in one place: both
dialects and both sides of the system — repository parsing, load generation and
the deployed primitive that runs its own embedded program — ask this question,
and a second answer would be a second set of bugs about string literals.

Every case here is one a naive ``str.split(";")`` gets wrong.
"""

from __future__ import annotations

import pytest

from weaver.sql_statements import (
    first_keyword,
    is_only_trivia,
    parse_statements,
    split_statements,
    strip_terminator,
    unterminated,
)


def _texts(sql: str) -> tuple[str, ...]:
    return tuple(statement.text for statement in parse_statements(sql))


# --- what separates statements ------------------------------------------------


def test_ordinary_terminated_statements_split_on_their_separators():
    assert _texts("select 1;\nselect 2;") == ("select 1", "select 2")


def test_the_separator_is_not_part_of_the_statement():
    statement, = parse_statements("select 1;")

    assert statement.text == "select 1"
    assert statement.terminated


def test_a_semicolon_inside_a_string_literal_separates_nothing():
    assert _texts("select 'a;b' as label;") == ("select 'a;b' as label",)


def test_a_semicolon_inside_a_line_comment_separates_nothing():
    assert _texts("-- one; two\nselect 1;") == ("-- one; two\nselect 1",)


def test_a_semicolon_inside_a_block_comment_separates_nothing():
    assert _texts("/* one; two */ select 1;") == ("/* one; two */ select 1",)


def test_a_semicolon_inside_a_backtick_identifier_separates_nothing():
    assert _texts("select `odd;name` from t;") == ("select `odd;name` from t",)


def test_a_semicolon_nested_in_parentheses_separates_nothing():
    assert _texts("insert into t values (1;2);") == ("insert into t values (1;2)",)


def test_split_statements_is_the_text_only_view_of_the_same_answer():
    program = "create or replace temporary view v as select 1;\nselect * from v;"

    assert split_statements(program) == _texts(program)


# --- what is not a statement --------------------------------------------------


def test_a_trailing_comment_after_the_last_separator_is_not_a_statement():
    assert _texts("select 1;\n-- finished\n") == ("select 1",)


def test_an_empty_body_holds_no_statements():
    assert parse_statements("") == ()
    assert parse_statements("   \n\n  ") == ()


def test_a_run_of_separators_produces_no_empty_statements():
    assert _texts("select 1;;;select 2;") == ("select 1", "select 2")


def test_only_trivia_is_recognised_as_such():
    assert is_only_trivia("  -- just a note\n/* and another */  ")
    assert not is_only_trivia("-- note\nselect 1")


# --- termination --------------------------------------------------------------


def test_a_body_whose_last_statement_trails_off_reports_it():
    trailing = unterminated("select 1;\nselect 2")

    assert trailing is not None
    assert trailing.text == "select 2"


def test_a_fully_terminated_body_reports_nothing_unterminated():
    assert unterminated("select 1;\nselect 2;") is None


def test_a_body_of_nothing_but_comments_is_not_unterminated():
    assert unterminated("-- nothing here\n") is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("select 1;", "select 1"),
        ("select 1", "select 1"),
        ("select 1 ;  \n", "select 1"),
        ("", ""),
    ],
)
def test_stripping_the_terminator_leaves_the_statement(text, expected):
    assert strip_terminator(text) == expected


def test_stripping_removes_one_terminator_and_not_a_statement_before_it():
    assert strip_terminator("select 1;\nselect 2;") == "select 1;\nselect 2"


# --- what a statement leads with ----------------------------------------------


def test_the_leading_keyword_skips_comments_and_whitespace():
    assert first_keyword("\n  -- explain the query\n  select 1") == "SELECT"


def test_the_leading_keyword_is_upper_cased_whatever_was_written():
    assert first_keyword("SeLeCt 1") == "SELECT"


def test_a_parenthesised_query_leads_with_its_bracket():
    assert first_keyword("(select 1) union (select 2)") == "("


def test_a_body_of_only_comments_leads_with_nothing():
    assert first_keyword("-- nothing to see") == ""


def test_a_statement_carries_its_own_leading_keyword():
    setup, query = parse_statements(
        "create or replace temporary view v as select 1;\nwith c as (select 1) select * from c;"
    )

    assert (setup.keyword, query.keyword) == ("CREATE", "WITH")


# --- offsets ------------------------------------------------------------------


def test_a_statement_can_be_sliced_back_out_of_the_source_it_came_from():
    program = "\ncreate temporary view v as select 1;\n\nselect * from v;\n"

    for statement in parse_statements(program):
        assert program[statement.start : statement.end] == statement.text
