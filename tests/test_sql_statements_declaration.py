"""Where one SQL statement ends and the next begins.

Pure lexical claims, and the whole reason they are made in one place: both
dialects and both sides of the system, repository parsing, load generation and
the deployed primitive that runs its own embedded program, ask this question,
and a second answer would be a second set of bugs about string literals.

Every case here is one a naive ``str.split(";")`` gets wrong.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
import sqlparse
from support.weaver_test import weaver_test

import weaver.sql_statements as sql_statements
from weaver.declaration.dependencies import locate_sql_references
from weaver.declaration.source import analyse_sql
from weaver.declaration.sql_shaping import query_spans
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


@weaver_test()
def test_ordinary_terminated_statements_split_on_their_separators():
    assert _texts("select 1;\nselect 2;") == ("select 1", "select 2")


@weaver_test()
def test_the_separator_is_not_part_of_the_statement():
    (statement,) = parse_statements("select 1;")

    assert statement.text == "select 1"
    assert statement.terminated


@weaver_test()
def test_a_semicolon_inside_a_string_literal_separates_nothing():
    assert _texts("select 'a;b' as label;") == ("select 'a;b' as label",)


@weaver_test()
def test_a_semicolon_inside_a_line_comment_separates_nothing():
    assert _texts("-- one; two\nselect 1;") == ("-- one; two\nselect 1",)


@weaver_test()
def test_a_semicolon_inside_a_block_comment_separates_nothing():
    assert _texts("/* one; two */ select 1;") == ("/* one; two */ select 1",)


@weaver_test()
def test_a_semicolon_inside_a_backtick_identifier_separates_nothing():
    assert _texts("select `odd;name` from t;") == ("select `odd;name` from t",)


@weaver_test()
def test_a_semicolon_nested_in_parentheses_separates_nothing():
    assert _texts("insert into t values (1;2);") == ("insert into t values (1;2)",)


@weaver_test()
def test_split_statements_is_the_text_only_view_of_the_same_answer():
    program = "create or replace temporary view v as select 1;\nselect * from v;"

    assert split_statements(program) == _texts(program)


# --- what is not a statement --------------------------------------------------


@weaver_test()
def test_a_trailing_comment_after_the_last_separator_is_not_a_statement():
    assert _texts("select 1;\n-- finished\n") == ("select 1",)


@weaver_test()
def test_an_empty_body_holds_no_statements():
    assert parse_statements("") == ()
    assert parse_statements("   \n\n  ") == ()


@weaver_test()
def test_a_run_of_separators_produces_no_empty_statements():
    assert _texts("select 1;;;select 2;") == ("select 1", "select 2")


@weaver_test()
def test_only_trivia_is_recognised_as_such():
    assert is_only_trivia("  -- just a note\n/* and another */  ")
    assert not is_only_trivia("-- note\nselect 1")


# --- termination --------------------------------------------------------------


@weaver_test()
def test_a_body_whose_last_statement_trails_off_reports_it():
    trailing = unterminated("select 1;\nselect 2")

    assert trailing is not None
    assert trailing.text == "select 2"


@weaver_test()
def test_a_fully_terminated_body_reports_nothing_unterminated():
    assert unterminated("select 1;\nselect 2;") is None


@weaver_test()
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
@weaver_test()
def test_stripping_the_terminator_leaves_the_statement(text, expected):
    assert strip_terminator(text) == expected


@weaver_test()
def test_stripping_removes_one_terminator_and_not_a_statement_before_it():
    assert strip_terminator("select 1;\nselect 2;") == "select 1;\nselect 2"


# --- what a statement leads with ----------------------------------------------


@weaver_test()
def test_the_leading_keyword_skips_comments_and_whitespace():
    assert first_keyword("\n  -- explain the query\n  select 1") == "SELECT"


@weaver_test()
def test_the_leading_keyword_is_upper_cased_whatever_was_written():
    assert first_keyword("SeLeCt 1") == "SELECT"


@weaver_test()
def test_a_parenthesised_query_leads_with_its_bracket():
    assert first_keyword("(select 1) union (select 2)") == "("


@weaver_test()
def test_a_body_of_only_comments_leads_with_nothing():
    assert first_keyword("-- nothing to see") == ""


@weaver_test()
def test_a_statement_carries_its_own_leading_keyword():
    setup, query = parse_statements(
        "create or replace temporary view v as select 1;\nwith c as (select 1) select * from c;"
    )

    assert (setup.keyword, query.keyword) == ("CREATE", "WITH")


@weaver_test()
def test_sql_consumers_reuse_one_parse_for_identical_text(monkeypatch):
    body = "select p.Id from [ParseCacheProbe].[Parent] as p where p.Active = 1"
    original = sqlparse.parse
    parsed = []

    def counting_parse(sql, *args, **kwargs):
        parsed.append(sql)
        return original(sql, *args, **kwargs)

    monkeypatch.setattr(sqlparse, "parse", counting_parse)

    with sql_statements.sql_parse_cache():
        assert analyse_sql(body).statement_count == 1
        assert locate_sql_references(body)
        assert query_spans(body)
    assert parsed.count(body) == 1


@weaver_test()
def test_a_parse_scope_releases_its_cached_token_trees():
    with sql_statements.sql_parse_cache() as cache:
        sql_statements.parse_sql("select 'parse-scope-contract'")
        assert cache.cache_info().currsize == 1
        assert cache.cache_info().maxsize == sql_statements.SQL_PARSE_CACHE_SIZE

    assert cache.cache_info().currsize == 0


@weaver_test()
def test_sql_text_is_not_cached_outside_a_parse_scope(monkeypatch):
    body = "select 'outside-parse-scope-contract'"
    original = sqlparse.parse
    parsed = []

    def counting_parse(sql, *args, **kwargs):
        parsed.append(sql)
        return original(sql, *args, **kwargs)

    monkeypatch.setattr(sqlparse, "parse", counting_parse)

    sql_statements.parse_sql(body)
    sql_statements.parse_sql(body)

    assert parsed.count(body) == 2


@weaver_test()
def test_nested_parse_scopes_keep_their_own_entries(monkeypatch):
    body = "select 'nested-parse-scope-contract'"
    original = sqlparse.parse
    parsed = []

    def counting_parse(sql, *args, **kwargs):
        parsed.append(sql)
        return original(sql, *args, **kwargs)

    monkeypatch.setattr(sqlparse, "parse", counting_parse)

    with sql_statements.sql_parse_cache():
        sql_statements.parse_sql(body)
        with sql_statements.sql_parse_cache():
            sql_statements.parse_sql(body)
            sql_statements.parse_sql(body)
        sql_statements.parse_sql(body)

    assert parsed.count(body) == 2


@weaver_test()
def test_concurrent_parse_scopes_do_not_share_entries(monkeypatch):
    body = "select 'concurrent-parse-scope-contract'"
    original = sqlparse.parse
    parsed = []
    barrier = Barrier(2)

    def counting_parse(sql, *args, **kwargs):
        parsed.append(sql)
        return original(sql, *args, **kwargs)

    def parse_twice_in_one_scope():
        with sql_statements.sql_parse_cache():
            barrier.wait()
            sql_statements.parse_sql(body)
            sql_statements.parse_sql(body)
            barrier.wait()

    monkeypatch.setattr(sqlparse, "parse", counting_parse)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(parse_twice_in_one_scope) for _ in range(2)]
        for future in futures:
            future.result()

    assert parsed.count(body) == 2


@weaver_test()
def test_a_parse_scope_releases_entries_after_an_exception():
    with pytest.raises(RuntimeError, match="scope failed"):
        with sql_statements.sql_parse_cache() as cache:
            sql_statements.parse_sql("select 'exception-parse-scope-contract'")
            raise RuntimeError("scope failed")

    assert cache.cache_info().currsize == 0


# --- offsets ------------------------------------------------------------------


@weaver_test()
def test_a_statement_can_be_sliced_back_out_of_the_source_it_came_from():
    program = "\ncreate temporary view v as select 1;\n\nselect * from v;\n"

    for statement in parse_statements(program):
        assert program[statement.start : statement.end] == statement.text
