"""Offset-exact T-SQL shape transforms — ported from the reference
implementation's wrangle tests, trimmed to the two transforms weaverstack
keeps (insert_where_one_eq_zero, insert_select_into) plus template rendering.
The serious-SQL fixtures exercise real, gnarly queries the way a Warehouse
build must survive.
"""

from pathlib import Path

import pytest
import sqlparse
from sqlparse import tokens as T

from weaver.ses.sql_shaping import (
    get_sql_template,
    insert_select_into,
    insert_where_one_eq_zero,
    render_sql_template,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sql"


def _fixture_sql(name):
    return (FIXTURE_DIR / name).read_text()


def _select_count(sql):
    return sum(
        1
        for statement in sqlparse.parse(sql)
        for token in statement.flatten()
        if token.ttype is T.DML and token.normalized.upper() == "SELECT"
    )


def test_get_sql_template_blocks_path_traversal():
    with pytest.raises(ValueError, match="template_name"):
        get_sql_template("../requirements")


def test_render_sql_template_fills_a_ddl_template():
    rendered = render_sql_template(
        "ddl/metadata_column_validation",
        temp_object_literal="N'tempdb..#weaver_shape'",
        metadata_columns_cte="    select N'Primary key' as metadata_kind, N'Id' as column_name",
    )
    assert "object_id(N'tempdb..#weaver_shape')" in rendered
    assert "N'Primary key' as metadata_kind" in rendered
    assert "$temp_object_literal" not in rendered


@pytest.mark.parametrize(
    "fixture_name",
    [
        "customer_retention_cohort.sql",
        "inventory_replenishment_forecast.sql",
        "order_fulfillment_pipeline.sql",
        "revenue_reconciliation_month_end.sql",
        "security_access_audit.sql",
    ],
)
def test_insert_where_one_eq_zero_guards_every_select_in_serious_sql_fixtures(fixture_name):
    sql = _fixture_sql(fixture_name)
    transformed = insert_where_one_eq_zero(sql)

    assert transformed.count("1=0") == _select_count(sql)
    assert "SELECT * from dbo.Nope" not in transformed


@pytest.mark.parametrize(
    "fixture_name",
    [
        "customer_retention_cohort.sql",
        "inventory_replenishment_forecast.sql",
        "order_fulfillment_pipeline.sql",
        "revenue_reconciliation_month_end.sql",
        "security_access_audit.sql",
    ],
)
def test_insert_select_into_wraps_one_query_in_serious_sql_fixtures(fixture_name):
    sql = _fixture_sql(fixture_name)
    transformed = insert_select_into(sql, "dbo.select_into_result")

    assert transformed.count("into dbo.select_into_result") == 1
    assert transformed.count("create table dbo.select_into_result as") == 0
    assert _select_count(transformed) == _select_count(sql)


def test_insert_select_into_modifies_simple_select():
    assert (
        insert_select_into("SELECT * from dbo.Users", "dbo.UsersCopy")
        == "SELECT * into dbo.UsersCopy from dbo.Users"
    )


def test_insert_select_into_modifies_only_last_standalone_select():
    sql = """SELECT *
FROM dbo.Users;

DECLARE @x int = 1;

SELECT *
FROM dbo.Teams
"""

    assert (
        insert_select_into(sql, "dbo.LastResult")
        == """SELECT *
FROM dbo.Users;

DECLARE @x int = 1;

SELECT *
into dbo.LastResult
FROM dbo.Teams
"""
    )


def test_insert_select_into_modifies_outer_cte_select():
    sql = """DECLARE @cutoff date = '2026-01-01';

WITH recent_users as (
    select
        u.Id
    from dbo.Users as u
    where
        u.CreatedAt >= @cutoff
)
select
    ru.Id
FROM recent_users as ru
"""

    assert (
        insert_select_into(sql, "dbo.RecentUsers")
        == """DECLARE @cutoff date = '2026-01-01';

WITH recent_users as (
    select
        u.Id
    from dbo.Users as u
    where
        u.CreatedAt >= @cutoff
)
select
    ru.Id
into dbo.RecentUsers
FROM recent_users as ru
"""
    )


def test_insert_select_into_modifies_first_branch_of_union_query():
    sql = """select
    Id
FROM dbo.Users
union all
select
    Id
FROM dbo.ArchivedUsers
"""

    assert (
        insert_select_into(sql, "dbo.AllUsers")
        == """select
    Id
into dbo.AllUsers
FROM dbo.Users
union all
select
    Id
FROM dbo.ArchivedUsers
"""
    )


def test_insert_select_into_ignores_insert_select_and_uses_last_result_select():
    sql = """INSERT into #UserIds (Id)
select
    u.Id
FROM dbo.Users as u

select
    t.Id
FROM dbo.Teams as t
"""

    assert (
        insert_select_into(sql, "dbo.TeamResult")
        == """INSERT into #UserIds (Id)
select
    u.Id
FROM dbo.Users as u

select
    t.Id
into dbo.TeamResult
FROM dbo.Teams as t
"""
    )


def test_insert_select_into_handles_declarations_before_final_select_without_semicolon():
    sql = """DECLARE @cutoff date = '2026-01-01'
set @cutoff = DATEADD(day, -7, @cutoff)
select
    u.Id
FROM dbo.Users as u
where
    u.CreatedAt >= @cutoff
"""

    assert (
        insert_select_into(sql, "dbo.RecentUsers")
        == """DECLARE @cutoff date = '2026-01-01'
set @cutoff = DATEADD(day, -7, @cutoff)
select
    u.Id
into dbo.RecentUsers
FROM dbo.Users as u
where
    u.CreatedAt >= @cutoff
"""
    )


def test_insert_select_into_handles_select_without_from():
    assert (
        insert_select_into("SELECT 1 as One", "dbo.OneRow")
        == "SELECT 1 as One into dbo.OneRow"
    )


def test_mixed_case_keywords_work_across_transformers():
    sql = "sEleCt u.Id FrOm dbo.Users as u wHeRe u.IsActive = 1 oRdEr bY u.Id"

    assert (
        insert_where_one_eq_zero(sql)
        == "sEleCt u.Id FrOm dbo.Users as u wHeRe (u.IsActive = 1) and 1=0 oRdEr bY u.Id"
    )
    assert (
        insert_select_into(sql, "dbo.MixedCase")
        == "sEleCt u.Id into dbo.MixedCase FrOm dbo.Users as u wHeRe u.IsActive = 1 oRdEr bY u.Id"
    )


def test_adds_where_to_simple_select():
    assert insert_where_one_eq_zero("SELECT * from dbo.Users") == "SELECT * from dbo.Users where 1=0"


def test_wraps_existing_where_condition():
    assert (
        insert_where_one_eq_zero("SELECT * from dbo.Users where IsActive = 1")
        == "SELECT * from dbo.Users where (IsActive = 1) and 1=0"
    )


def test_inserts_before_order_by():
    assert (
        insert_where_one_eq_zero("SELECT * from dbo.Users order by CreatedAt DESC")
        == "SELECT * from dbo.Users where 1=0 order by CreatedAt DESC"
    )


def test_preserves_group_by_after_existing_where():
    assert (
        insert_where_one_eq_zero("SELECT TeamId, COUNT(*) from dbo.Users where IsActive = 1 GROUP BY TeamId")
        == "SELECT TeamId, COUNT(*) from dbo.Users where (IsActive = 1) and 1=0 GROUP BY TeamId"
    )


def test_adds_where_after_join():
    sql = "SELECT u.Id from dbo.Users u INNER join dbo.Teams t on t.Id = u.TeamId"
    assert (
        insert_where_one_eq_zero(sql)
        == "SELECT u.Id from dbo.Users u INNER join dbo.Teams t on t.Id = u.TeamId where 1=0"
    )


def test_transforms_cte_and_outer_select():
    sql = "WITH cte as (SELECT Id from dbo.Users) select * from cte"
    assert (
        insert_where_one_eq_zero(sql)
        == "WITH cte as (SELECT Id from dbo.Users where 1=0) select * from cte where 1=0"
    )


def test_transforms_subquery_in_from_clause():
    sql = "SELECT * from (SELECT Id from dbo.Users where IsActive = 1) u"
    assert (
        insert_where_one_eq_zero(sql)
        == "SELECT * from (SELECT Id from dbo.Users where (IsActive = 1) and 1=0) u where 1=0"
    )


def test_transforms_subquery_inside_existing_where():
    sql = "SELECT * from dbo.Teams where Id IN (SELECT TeamId from dbo.Users where IsActive = 1)"
    assert (
        insert_where_one_eq_zero(sql)
        == "SELECT * from dbo.Teams where (Id IN (SELECT TeamId from dbo.Users where (IsActive = 1) and 1=0)) and 1=0"
    )


def test_transforms_each_side_of_union():
    sql = "SELECT Id from dbo.Users union all select Id from dbo.ArchivedUsers where DeletedAt IS NOT NULL"
    assert (
        insert_where_one_eq_zero(sql)
        == "SELECT Id from dbo.Users where 1=0 union all select Id from dbo.ArchivedUsers where (DeletedAt IS NOT NULL) and 1=0"
    )


def test_transforms_multiple_statements():
    sql = "SELECT 1; select 2 where 2 = 2;"
    assert insert_where_one_eq_zero(sql) == "SELECT 1 where 1=0; select 2 where (2 = 2) and 1=0;"


def test_handles_tsql_bracketed_identifiers_and_top():
    sql = "SELECT TOP (10) [User Id] from [dbo].[Users] where [Status] = 'A'"
    assert (
        insert_where_one_eq_zero(sql)
        == "SELECT TOP (10) [User Id] from [dbo].[Users] where ([Status] = 'A') and 1=0"
    )


def test_adds_where_to_multiline_select_before_order_by():
    sql = """select
    u.Id,
    u.Name
FROM dbo.Users as u
LEFT join dbo.Teams as t
    on t.Id = u.TeamId
order by
    u.Name;
"""

    assert (
        insert_where_one_eq_zero(sql)
        == """select
    u.Id,
    u.Name
FROM dbo.Users as u
LEFT join dbo.Teams as t
    on t.Id = u.TeamId where 1=0
order by
    u.Name;
"""
    )


def test_wraps_existing_multiline_where_before_group_by():
    sql = """select
    t.Id,
    COUNT(*) as UserCount
FROM dbo.Teams as t
where
    t.IsActive = 1
    and t.Region IN ('AU', 'NZ')
GROUP BY
    t.Id;
"""

    assert (
        insert_where_one_eq_zero(sql)
        == """select
    t.Id,
    COUNT(*) as UserCount
FROM dbo.Teams as t
where (t.IsActive = 1
    and t.Region IN ('AU', 'NZ')) and 1=0
GROUP BY
    t.Id;
"""
    )


def test_transforms_long_script_with_comments_temp_table_and_go():
    sql = """-- Build active user extract.
select
    u.Id,
    u.Email
into #ActiveUsers
FROM dbo.Users as u
where
    u.IsActive = 1;

GO

select
    au.Id,
    p.PlanName
FROM #ActiveUsers as au
join dbo.Plans as p
    on p.UserId = au.Id
order by
    au.Id;
"""

    assert (
        insert_where_one_eq_zero(sql)
        == """-- Build active user extract.
select
    u.Id,
    u.Email
into #ActiveUsers
FROM dbo.Users as u
where (u.IsActive = 1) and 1=0;

GO

select
    au.Id,
    p.PlanName
FROM #ActiveUsers as au
join dbo.Plans as p
    on p.UserId = au.Id where 1=0
order by
    au.Id;
"""
    )


def test_transforms_multiline_cte_and_nested_exists():
    sql = """WITH recent_users as (
    select
        u.Id,
        u.TeamId
    from dbo.Users as u
    where
        u.CreatedAt >= '2026-01-01'
)
select
    t.Id
FROM dbo.Teams as t
where
    EXISTS (
        select
            1
        from recent_users as ru
        where
            ru.TeamId = t.Id
    );
"""

    assert (
        insert_where_one_eq_zero(sql)
        == """WITH recent_users as (
    select
        u.Id,
        u.TeamId
    from dbo.Users as u
    where (u.CreatedAt >= '2026-01-01') and 1=0
)
select
    t.Id
FROM dbo.Teams as t
where (EXISTS (
        select
            1
        from recent_users as ru
        where (ru.TeamId = t.Id) and 1=0
    )) and 1=0;
"""
    )


def test_treats_go_as_batch_separator_without_semicolons():
    sql = """SELECT *
FROM dbo.Users
GO
SELECT *
FROM dbo.Teams
"""

    assert (
        insert_where_one_eq_zero(sql)
        == """SELECT *
FROM dbo.Users where 1=0
GO
SELECT *
FROM dbo.Teams where 1=0
"""
    )


def test_handles_irrelevant_statements_and_adjacent_selects_without_semicolons():
    sql = """DECLARE @cutoff date
set @cutoff = '2026-01-01'
PRINT 'this string says select * from dbo.Nope'
-- This comment has select * from dbo.CommentOnly
SELECT @cutoff as CutoffDate
select
    u.Id
FROM dbo.Users as u
where
    u.CreatedAt >= @cutoff
if EXISTS (
    select
        1
    from dbo.Teams as t
    where
        t.IsActive = 1
)
begin
    INSERT into #UserIds (Id)
    select
        u.Id
    from dbo.Users as u
    update dbo.Users set Seen = 1
end
"""

    assert (
        insert_where_one_eq_zero(sql)
        == """DECLARE @cutoff date
set @cutoff = '2026-01-01'
PRINT 'this string says select * from dbo.Nope'
-- This comment has select * from dbo.CommentOnly
SELECT @cutoff as CutoffDate where 1=0
select
    u.Id
FROM dbo.Users as u
where (u.CreatedAt >= @cutoff) and 1=0
if EXISTS (
    select
        1
    from dbo.Teams as t
    where (t.IsActive = 1) and 1=0
)
begin
    INSERT into #UserIds (Id)
    select
        u.Id
    from dbo.Users as u where 1=0
    update dbo.Users set Seen = 1
end
"""
    )


def test_does_not_treat_tsql_table_hint_with_as_statement_boundary():
    sql = "SELECT * from dbo.Users WITH (NOLOCK)"

    assert insert_where_one_eq_zero(sql) == "SELECT * from dbo.Users WITH (NOLOCK) where 1=0"
