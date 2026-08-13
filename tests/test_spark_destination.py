"""Addressing a *named* Spark destination — no session, no JVM.

The behaviour under test is the one the whole multi-target change rests on: a
statement says which Lakehouse it means, instead of inheriting one from whatever
the session is attached to.

The two workspaces answer differently, and the difference is data:

===========  ==================================================
Fabric       ```Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer```
local        ```sales_lh__sales`.`Customer```
===========  ==================================================

Fabric's shape was confirmed against a real workspace before it was built on:
four-part DDL, DML, ``MERGE``, cross-Lakehouse views and drops all work from one
session, and a bare two-part name lands in the attached Lakehouse. The local
shape is a proxy, not an imitation — local Spark cannot be given a catalogue per
Lakehouse, because Delta's catalogue can only be the session catalogue.
"""

from __future__ import annotations

import pytest

from weaver.errors import IdentityError, InstallError
from weaver.spark import (
    SparkCatalogue,
    SparkNaming,
    expand,
    fabric_destination,
    local_destination,
    object_token,
    schema_token,
)


@pytest.fixture
def fabric():
    return fabric_destination(workspace="Weaver", lakehouse="Play_Lakehouse_1")


@pytest.fixture
def local():
    return local_destination(item="Sales_LH", tables_root="/root/Sales_LH/Tables")


# --- Fabric: the native namespace ---------------------------------------------


def test_fabric_qualifies_an_object_with_all_four_parts(fabric):
    assert fabric.qualify("Sales", "Customer") == (
        "`Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`"
    )


def test_fabric_qualifies_a_schema_with_the_three_that_name_it(fabric):
    """A Fabric schema *is* a three-level name under ``spark_catalog``."""

    assert fabric.qualified_schema("Sales") == "`Weaver`.`Play_Lakehouse_1`.`Sales`"


def test_a_fabric_schema_needs_no_location(fabric):
    """A schema-enabled Lakehouse pins its own managed tables.

    Which is as well: a location would be an ``abfss://`` root carrying workspace
    and item ids, and that cannot go anywhere near a payload.
    """

    assert fabric.schema_location("Sales") is None


def test_two_fabric_lakehouses_are_different_places(fabric):
    other = fabric_destination(workspace="Weaver", lakehouse="Play_Lakehouse_2")
    assert fabric.qualify("Sales", "Customer") != other.qualify("Sales", "Customer")


# --- local: the proxy ----------------------------------------------------------


def test_local_folds_the_lakehouse_into_the_one_namespace_level_it_has(local):
    assert local.qualify("Sales", "Customer") == "`sales_lh__sales`.`Customer`"
    assert local.qualified_schema("Sales") == "`sales_lh__sales`"


def test_a_local_schema_is_pinned_under_the_lakehouse_tables_area(local):
    """The fold is in the name only — storage still mirrors OneLake."""

    assert local.schema_location("Sales") == "/root/Sales_LH/Tables/Sales"


def test_two_local_lakehouses_sharing_a_schema_name_stay_apart(local):
    """The defect the fold exists to remove.

    Before it, both destinations declared a database called ``Sales`` and
    ``CREATE SCHEMA IF NOT EXISTS`` meant the first one to register it won —
    silently, taking the second Lakehouse's tables with it.
    """

    other = local_destination(item="Inventory_LH", tables_root="/root/Inventory_LH/Tables")

    assert local.qualify("Sales", "Customer") != other.qualify("Sales", "Customer")
    assert local.schema_location("Sales") != other.schema_location("Sales")


def test_a_lakehouse_name_local_spark_cannot_hold_is_refused():
    """Refused rather than sanitised: a silently altered name could collide again."""

    with pytest.raises(IdentityError, match="letters, digits and underscores"):
        local_destination(item="Sales LH", tables_root="/root/x")


@pytest.mark.parametrize("bad", ["", "   ", "a.b", "a/b", "a\\b"])
def test_a_name_carrying_a_separator_is_refused(local, bad):
    with pytest.raises(IdentityError):
        local.qualify(bad, "Customer")
    with pytest.raises(IdentityError):
        local.qualify("Sales", bad)


def test_a_backtick_in_a_name_is_doubled_not_dropped():
    destination = local_destination(item="Sales_LH", tables_root="/root")
    assert destination.qualify("Sales", "Odd`Name") == "`sales_lh__sales`.`Odd``Name`"


# --- tokens: what a payload carries instead of a destination -------------------


def test_a_payload_names_the_object_and_nothing_about_where_it_lives():
    assert object_token("Sales", "Customer") == "{{object:Sales.Customer}}"
    assert schema_token("Sales") == "{{schema:Sales}}"


def test_the_same_payload_resolves_differently_per_destination(fabric, local):
    """The property that keeps a bundle comparable between environments.

    One set of bytes, generated once; the destination decides how it reads. Had
    the qualified name been frozen instead, two bundles of the same repository
    would differ in every payload merely for having been generated in different
    workspaces (how-does-build-work §15).
    """

    payload = (
        "CREATE OR REPLACE VIEW {{object:Sales.ActiveCustomer}} AS\n"
        "SELECT * FROM {{object:Sales.Customer}} WHERE IsActive"
    )

    assert expand(payload, fabric) == (
        "CREATE OR REPLACE VIEW `Weaver`.`Play_Lakehouse_1`.`Sales`.`ActiveCustomer` AS\n"
        "SELECT * FROM `Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer` WHERE IsActive"
    )
    assert expand(payload, local) == (
        "CREATE OR REPLACE VIEW `sales_lh__sales`.`ActiveCustomer` AS\n"
        "SELECT * FROM `sales_lh__sales`.`Customer` WHERE IsActive"
    )


def test_a_schema_token_resolves_too(fabric):
    assert expand("DROP SCHEMA IF EXISTS {{schema:Legacy}} CASCADE", fabric) == (
        "DROP SCHEMA IF EXISTS `Weaver`.`Play_Lakehouse_1`.`Legacy` CASCADE"
    )


def test_an_unknown_token_is_refused_rather_than_passed_through(fabric):
    """It would reach Spark as either a syntax error or, worse, a valid name."""

    with pytest.raises(InstallError, match=r"\{\{lakehouse:Other\}\}"):
        expand("SELECT * FROM {{lakehouse:Other}}", fabric)


def test_text_with_no_tokens_is_returned_unchanged(fabric):
    assert expand("SELECT 1", fabric) == "SELECT 1"


# --- catalogue operations, against a fake session ------------------------------


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


class _Catalog:
    def __init__(self, databases=(), tables=()):
        self.databases = set(databases)
        self.tables = set(tables)

    def databaseExists(self, name):
        return name in self.databases

    def tableExists(self, name):
        return name in self.tables


class _Spark:
    """Records what it was asked, and answers listings from a fixed inventory."""

    def __init__(self, listings=None, catalog=None):
        self.executed = []
        self._listings = listings or {}
        self.catalog = catalog or _Catalog()
        self.conf = _Conf()

    def sql(self, statement):
        self.executed.append(statement)
        if statement in self._listings:
            return _Frame(self._listings[statement])
        if statement.startswith(("SHOW VIEWS", "SHOW TABLES")):
            raise RuntimeError("[SCHEMA_NOT_FOUND] no such schema")
        if statement.startswith("DESCRIBE SCHEMA"):
            name = statement.split(None, 2)[2]
            if name not in self.catalog.databases:
                raise RuntimeError("[SCHEMA_NOT_FOUND] no such schema")
            return _Frame([])
        if statement.startswith("DESCRIBE TABLE"):
            name = statement.split(None, 2)[2]
            if name not in self.catalog.tables:
                raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] no such table")
            return _Frame([])
        return None


class _Conf:
    def __init__(self):
        self.values = {"spark.sql.caseSensitive": "false"}

    def set(self, key, value):
        self.values[key] = str(value)


def test_creating_a_schema_locally_pins_its_storage(local):
    spark = _Spark()
    statement = SparkCatalogue(spark, local).create_schema("Sales")

    assert statement == (
        "CREATE SCHEMA IF NOT EXISTS `sales_lh__sales` "
        "LOCATION '/root/Sales_LH/Tables/Sales'"
    )
    assert spark.executed == [statement]


def test_creating_a_schema_on_fabric_names_the_lakehouse_and_no_path(fabric):
    spark = _Spark()
    statement = SparkCatalogue(spark, fabric).create_schema("Sales")

    assert statement == "CREATE SCHEMA IF NOT EXISTS `Weaver`.`Play_Lakehouse_1`.`Sales`"
    assert "LOCATION" not in statement


def test_a_statement_run_through_the_catalogue_is_addressed_first(local):
    spark = _Spark()
    SparkCatalogue(spark, local).sql("DROP TABLE IF EXISTS {{object:Sales.Old}}")

    assert spark.executed == ["DROP TABLE IF EXISTS `sales_lh__sales`.`Old`"]


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
    """Both workspaces raise for an absent schema; an inventory wants "empty"."""

    assert SparkCatalogue(_Spark(), fabric).views("Sales") == ()


def test_a_failure_that_is_not_absence_still_propagates(fabric):
    class Angry(_Spark):
        def sql(self, statement):
            raise RuntimeError("the cluster is on fire")

    with pytest.raises(RuntimeError, match="on fire"):
        SparkCatalogue(Angry(), fabric).views("Sales")


def test_absence_is_recognised_by_spark_s_error_class_not_its_message(fabric):
    """A reworded message must not turn a real failure into an empty inventory.

    So an exception carrying an error class is judged on that class alone —
    including one whose *text* looks like absence but whose class says otherwise.
    """

    class Classified(RuntimeError):
        def __init__(self, error_class, message):
            super().__init__(message)
            self._error_class = error_class

        def getErrorClass(self):
            return self._error_class

    class Raising(_Spark):
        def __init__(self, exception):
            super().__init__()
            self._exception = exception

        def sql(self, statement):
            raise self._exception

    absent = Raising(Classified("SCHEMA_NOT_FOUND", "no such schema"))
    assert SparkCatalogue(absent, fabric).views("Sales") == ()

    lying = Raising(Classified("DELTA_LOG_CORRUPTED", "SCHEMA_NOT_FOUND"))
    with pytest.raises(RuntimeError):
        SparkCatalogue(lying, fabric).views("Sales")


def test_existence_is_asked_of_the_qualified_name(fabric):
    spark = _Spark(
        catalog=_Catalog(
            databases={"`Weaver`.`Play_Lakehouse_1`.`Sales`"},
            tables={"`Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`"},
        )
    )
    catalogue = SparkCatalogue(spark, fabric)

    assert catalogue.schema_exists("Sales")
    assert catalogue.exists("Sales", "Customer")
    assert not catalogue.exists("Sales", "Missing")
    assert not catalogue.schema_exists("Missing")


def test_a_catalogue_without_a_session_is_refused_at_construction(fabric):
    with pytest.raises(InstallError, match="Play_Lakehouse_1"):
        SparkCatalogue(None, fabric)


def test_a_local_catalogue_establishes_the_emulators_case_policy(local, fabric):
    """The emulator's exact-case analysis is a session policy, not a scope.

    Local Spark's session catalogue cannot find a PascalCase table again once
    analysis returns to case-insensitive, so the setting stays on for the life of
    the session. Fabric's catalogue can, so nothing is imposed there.
    """

    spark = _Spark()
    SparkCatalogue(spark, local)
    assert spark.conf.values["spark.sql.caseSensitive"] == "true"

    other = _Spark()
    SparkCatalogue(other, fabric)
    assert other.conf.values["spark.sql.caseSensitive"] == "false"


def test_naming_a_destination_needs_no_session(fabric):
    """The half a desktop executor holds: names and statement text, no Spark."""

    names = SparkNaming(fabric)

    assert names.qualify("Sales", "Customer") == (
        "`Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`"
    )
    assert names.qualified_schema("Sales") == "`Weaver`.`Play_Lakehouse_1`.`Sales`"
    assert names.expand("SELECT * FROM {{object:Sales.Customer}}") == (
        "SELECT * FROM `Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`"
    )
    assert names.exact_case is True


def test_a_rendered_schema_create_matches_the_one_the_catalogue_runs(local, fabric):
    for destination in (local, fabric):
        spark = _Spark()
        catalogue = SparkCatalogue(spark, destination)

        assert SparkNaming(destination).create_schema_statement(
            "Sales", if_not_exists=False
        ) == catalogue.create_schema("Sales", if_not_exists=False)


def test_local_wipe_drops_only_the_destination_folded_namespaces():
    from weaver.spark import drop_local_destination_catalogue, local_destination

    class Row:
        def __init__(self, namespace):
            self.namespace = namespace

        def asDict(self):
            return {"namespace": self.namespace}

    class Result:
        def collect(self):
            return [
                Row("play_lh__sales"),
                Row("play_lh__inventory"),
                Row("other_lh__sales"),
            ]

    class Spark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)
            return Result()

    spark = Spark()
    statements = drop_local_destination_catalogue(
        spark, local_destination(item="Play_LH", tables_root="/tmp/Play_LH/Tables")
    )

    assert statements == (
        "DROP SCHEMA IF EXISTS `play_lh__inventory` CASCADE",
        "DROP SCHEMA IF EXISTS `play_lh__sales` CASCADE",
    )
    assert spark.statements == ["SHOW DATABASES", *statements]
