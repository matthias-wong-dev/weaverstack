"""Addressing a named Fabric Lakehouse — no session, no JVM.

The behaviour the whole multi-target arrangement rests on: a statement says
which Lakehouse it means, instead of inheriting one from whatever the session
is attached to.

    ``Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer``
"""

from __future__ import annotations

import pytest

from weaver.errors import IdentityError, InstallError
from weaver.spark import FabricSparkTarget, SparkCatalogue


@pytest.fixture
def fabric():
    return FabricSparkTarget(workspace="Weaver", lakehouse="Play_Lakehouse_1")


# --- the native namespace -----------------------------------------------------


def test_an_object_is_qualified_with_all_four_parts(fabric):
    assert (
        fabric.qualify("Sales", "Customer")
        == "`Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`"
    )


def test_a_schema_is_qualified_with_the_three_that_name_it(fabric):
    assert fabric.qualified_schema("Sales") == "`Weaver`.`Play_Lakehouse_1`.`Sales`"


def test_creating_a_schema_names_the_lakehouse_and_no_path(fabric):
    """A schema-enabled Fabric Lakehouse pins its own storage and refuses one."""

    statement = fabric.create_schema_statement("Sales")

    assert statement == (
        "CREATE SCHEMA IF NOT EXISTS `Weaver`.`Play_Lakehouse_1`.`Sales`"
    )
    assert "LOCATION" not in statement


def test_two_lakehouses_are_different_places(fabric):
    other = FabricSparkTarget(workspace="Weaver", lakehouse="Play_Lakehouse_2")

    assert fabric.qualify("Sales", "Customer") != other.qualify("Sales", "Customer")


# --- what a name may contain --------------------------------------------------


def test_a_backtick_in_a_name_is_doubled_not_dropped():
    """Otherwise a crafted name would end the identifier and start syntax."""

    target = FabricSparkTarget(workspace="Weaver", lakehouse="odd`name")

    assert target.qualify("Sales", "Customer") == (
        "`Weaver`.`odd``name`.`Sales`.`Customer`"
    )


@pytest.mark.parametrize("name", ["Contoso Data", "Contoso.Data", "Sales-Reporting"])
def test_a_workspace_name_may_carry_what_fabric_allows(name):
    """Fabric workspace names hold spaces, dots and dashes, and quoting is enough.

    Weaver adds no naming rule of its own here: a back-tick quoted identifier
    is unambiguous, and refusing a workspace Fabric is serving would make
    Weaver unusable against it.
    """

    target = FabricSparkTarget(workspace=name, lakehouse="Sales")

    assert target.qualify("Sales", "Customer") == (
        f"`{name}`.`Sales`.`Sales`.`Customer`"
    )


@pytest.mark.parametrize("empty", ["", "   "])
def test_an_empty_name_is_refused(empty):
    with pytest.raises(IdentityError):
        FabricSparkTarget(workspace=empty, lakehouse="Sales")


def test_a_name_that_is_not_a_string_is_refused():
    with pytest.raises(IdentityError):
        FabricSparkTarget(workspace=object(), lakehouse="Sales")


# --- catalogue operations, which run statements rather than name them ---------


class _Row:
    def __init__(self, **data):
        self._data = data

    def asDict(self):
        return dict(self._data)


class _Frame:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _Spark:
    """Records what it was asked, and answers listings from a fixed inventory."""

    def __init__(self, listings=None, tables=()):
        self.executed = []
        self._listings = listings or {}
        self._tables = set(tables)

    def sql(self, statement):
        self.executed.append(statement)
        if statement in self._listings:
            return _Frame(self._listings[statement])
        if statement.startswith(("SHOW VIEWS", "SHOW TABLES")):
            raise RuntimeError("[SCHEMA_NOT_FOUND] no such schema")
        if statement.startswith("DESCRIBE TABLE"):
            if statement.split(None, 2)[2] not in self._tables:
                raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] no such table")
            return _Frame([])
        return None


def test_a_statement_runs_exactly_as_it_was_given(fabric):
    """Payloads arrive addressed, so the catalogue resolves nothing."""

    spark = _Spark()
    statement = "DROP TABLE IF EXISTS `Weaver`.`Play_Lakehouse_1`.`Sales`.`Old`"
    SparkCatalogue(spark, fabric).sql(statement)

    assert spark.executed == [statement]


def test_listing_views_asks_the_destination_not_the_session(fabric):
    spark = _Spark(
        listings={
            "SHOW VIEWS IN `Weaver`.`Play_Lakehouse_1`.`Sales`": [
                _Row(viewName="active", isTemporary=False),
                _Row(viewName="scratch", isTemporary=True),
            ]
        }
    )

    assert SparkCatalogue(spark, fabric).views("Sales") == ("active",)


def test_listing_tables_takes_the_views_back_out(fabric):
    """``SHOW TABLES`` returns views as well — confirmed in a real workspace."""

    spark = _Spark(
        listings={
            "SHOW TABLES IN `Weaver`.`Play_Lakehouse_1`.`Sales`": [
                _Row(tableName="customer", isTemporary=False),
                _Row(tableName="active", isTemporary=False),
            ],
            "SHOW VIEWS IN `Weaver`.`Play_Lakehouse_1`.`Sales`": [
                _Row(viewName="active", isTemporary=False)
            ],
        }
    )

    assert SparkCatalogue(spark, fabric).tables("Sales") == ("customer",)


def test_a_schema_that_is_not_there_holds_nothing(fabric):
    """Fabric raises for an absent schema; an inventory wants "empty"."""

    assert SparkCatalogue(_Spark(), fabric).views("Sales") == ()


def test_a_failure_that_is_not_absence_still_propagates(fabric):
    class _Broken(_Spark):
        def sql(self, statement):
            raise RuntimeError("AnalysisException: capacity is paused")

    with pytest.raises(RuntimeError, match="capacity is paused"):
        SparkCatalogue(_Broken(), fabric).views("Sales")


def test_existence_is_asked_of_the_qualified_name(fabric):
    spark = _Spark(tables={"`Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`"})
    catalogue = SparkCatalogue(spark, fabric)

    assert catalogue.exists("Sales", "Customer")
    assert not catalogue.exists("Sales", "Absent")


def test_a_catalogue_without_a_session_is_refused_at_construction(fabric):
    with pytest.raises(InstallError):
        SparkCatalogue(None, fabric)


def test_a_catalogue_can_run_statements_without_a_session(fabric):
    """From a desktop the statements cross and the reading stays here."""

    submitted = []

    def run(statement):
        submitted.append(statement)
        return [{"tableName": "customer"}]

    catalogue = SparkCatalogue.over_sql(run, fabric)

    assert catalogue.rows("SHOW TABLES") == [{"tableName": "customer"}]
    assert submitted == ["SHOW TABLES"]
