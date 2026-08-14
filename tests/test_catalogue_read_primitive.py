"""One read per physical catalogue table, however many items a build binds.

The catalogue is a fixed set of physical tables holding rows for every logical
item installed against them. A build that read per item asked the same table the
same question once per scope, so its cost was

.. code-block:: text

    catalogue tables × bound items

round trips — and the multiplier was the number of items, which is the thing a
growing estate increases. One predicate per table answers all of it at once.

What must not change with it is the *shape* of the answer: planning consumes an
item-oriented catalogue, so the rows are grouped back by installation in Python.
Nor may the scope widen — a build that could see an unrelated installation's
rows could publish over them.

The fake catalogue here counts reads and records predicates, so both halves are
provable with no session: how many times each table was asked, and what it was
asked for.
"""

from __future__ import annotations

import pytest

from weaver.catalogue.render import InstallationScope, InstallationScopes
from weaver.catalogue.state import read_catalogue_state
from weaver.catalogue.tables import CATALOGUE_TABLES, REGISTRY
from weaver.declaration.model import WeaverItemId
from weaver.errors import BuildError
from weaver.spark import FabricSparkTarget


class CountingCatalogue:
    """Every catalogue table present, holding the rows it is given."""

    def __init__(self, rows_by_table=None):
        self.rows_by_table = rows_by_table or {}
        self.reads: list[str] = []
        self.statements: list[str] = []

    # --- the SparkCatalogue surface the reader uses ---------------------------

    #: The Weaver Lakehouse this catalogue is addressed to. A statement names
    #: its object in full, exactly as the reader will send it.
    destination = FabricSparkTarget(workspace="Demo", lakehouse="Weaver")

    def qualify(self, schema: str, name: str) -> str:
        return self.destination.qualify(schema, name)

    def columns_of(self, name: str) -> tuple[str, ...]:
        table = {t.name: t for t in CATALOGUE_TABLES}[name.rsplit(".", 1)[1].strip("`")]
        self.reads.append(table.name)
        return tuple(table.physical_columns)

    def rows(self, statement: str) -> list[dict]:
        self.statements.append(statement)
        return [dict(row) for row in self.rows_by_table.get(_table_of(statement), ())]

    def read_count(self, table_name: str) -> int:
        """How many times this table's *rows* were scanned.

        Not how often a table handle was taken: resolving a table to check its
        columns is a metadata lookup that happens once per table either way, and
        counting it would make the fake disagree with what the scaling rule is
        about.
        """

        return sum(
            1 for statement in self.statements if _table_of(statement) == table_name
        )

    def statement_for(self, table_name: str) -> str:
        return next(
            statement
            for statement in self.statements
            if _table_of(statement) == table_name
        )


def _table_of(statement: str) -> str:
    return next(
        table.name
        for table in CATALOGUE_TABLES
        if f".`{table.name}`" in statement
    )


def _items(*names):
    return tuple(WeaverItemId.parse(name) for name in names)


def _registry_row(item: WeaverItemId, object_name: str):
    return {
        "item_type": item.item_type,
        "item_name": item.item_name,
        "schema_name": "DWG",
        "object_name": object_name,
        "object_type": "table",
        "object_role": "data",
        "signature": f"sig-{object_name}",
        "build_epoch": None,
    }


# --- the scaling rule ---------------------------------------------------------


def test_two_items_cause_one_read_per_table_not_two():
    catalogue = CountingCatalogue()

    read_catalogue_state(catalogue, _items("Lakehouse/Sales", "Lakehouse/Inventory"))

    for table in CATALOGUE_TABLES:
        assert catalogue.read_count(table.name) == 1, (
            f"{table.name} was read {catalogue.read_count(table.name)} times"
        )


def test_twenty_items_still_cause_one_read_per_table():
    """Adding items lengthens the predicate; it must not add round trips."""

    catalogue = CountingCatalogue()

    read_catalogue_state(
        catalogue, _items(*[f"Lakehouse/Item{index}" for index in range(20)])
    )

    assert len(catalogue.statements) == len(CATALOGUE_TABLES)


def test_the_read_count_does_not_change_between_one_item_and_many():
    one = CountingCatalogue()
    read_catalogue_state(one, _items("Lakehouse/Sales"))

    many = CountingCatalogue()
    read_catalogue_state(many, _items(*[f"Lakehouse/Item{i}" for i in range(12)]))

    assert len(many.statements) == len(one.statements)


# --- and the answer keeps its shape ------------------------------------------


def test_rows_are_grouped_back_by_logical_item():
    sales, inventory = _items("Lakehouse/Sales", "Lakehouse/Inventory")
    catalogue = CountingCatalogue(
        {
            REGISTRY.name: (
                _registry_row(sales, "Customer"),
                _registry_row(inventory, "Product"),
                _registry_row(sales, "Order"),
            )
        }
    )

    state = read_catalogue_state(catalogue, (sales, inventory))

    assert {
        row["object_name"] for row in state.rows[sales][REGISTRY.name]
    } == {"Customer", "Order"}
    assert {
        row["object_name"] for row in state.rows[inventory][REGISTRY.name]
    } == {"Product"}


def test_an_item_with_no_rows_is_still_present_in_the_catalogue():
    """An unbuilt item is empty, not absent — everything downstream iterates these."""

    sales, inventory = _items("Lakehouse/Sales", "Lakehouse/Inventory")
    catalogue = CountingCatalogue({REGISTRY.name: (_registry_row(sales, "Customer"),)})

    state = read_catalogue_state(catalogue, (sales, inventory))

    assert inventory in state.rows
    assert state.rows[inventory][REGISTRY.name] == ()
    assert set(state.rows[inventory]) == {table.name for table in CATALOGUE_TABLES}


def test_the_registered_documents_still_derive_from_the_grouped_rows():
    sales = WeaverItemId.parse("Lakehouse/Sales")
    catalogue = CountingCatalogue({REGISTRY.name: (_registry_row(sales, "Customer"),)})

    state = read_catalogue_state(catalogue, (sales,))

    assert len(state.registered) == 1
    document = next(iter(state.registered.values()))
    assert document.signature == "sig-Customer"


# --- the scope stays bounded --------------------------------------------------


def test_the_predicate_names_every_requested_scope_and_no_others():
    catalogue = CountingCatalogue()

    read_catalogue_state(catalogue, _items("Lakehouse/Sales", "Warehouse/Reporting"))

    registry_sql = catalogue.statement_for(REGISTRY.name)

    assert "'Sales'" in registry_sql and "'Reporting'" in registry_sql
    assert "'Inventory'" not in registry_sql


def test_a_row_outside_the_requested_scopes_is_a_failure_not_a_silent_drop():
    """A read that ignored its predicate must not quietly become build state."""

    sales = WeaverItemId.parse("Lakehouse/Sales")
    stranger = WeaverItemId.parse("Lakehouse/SomeoneElse")
    catalogue = CountingCatalogue(
        {REGISTRY.name: (_registry_row(sales, "Customer"), _registry_row(stranger, "X"))}
    )

    with pytest.raises(BuildError, match="did not ask for"):
        read_catalogue_state(catalogue, (sales,))


def test_reading_for_no_items_reads_no_rows_at_all():
    """An empty predicate would be every row in the catalogue."""

    catalogue = CountingCatalogue()

    state = read_catalogue_state(catalogue, ())

    assert not state.rows
    assert catalogue.statements == []


# --- the predicate itself -----------------------------------------------------


def test_one_scope_renders_as_a_plain_conjunction():
    scopes = InstallationScopes((InstallationScope("Lakehouse", "Sales"),))

    assert scopes.predicate == InstallationScope("Lakehouse", "Sales").predicate


def test_several_scopes_render_as_a_disjunction_of_conjunctions():
    scopes = InstallationScopes(
        (
            InstallationScope("Lakehouse", "Sales"),
            InstallationScope("Warehouse", "Reporting"),
        )
    )

    assert scopes.predicate.count(" OR ") == 1
    assert "'Sales'" in scopes.predicate and "'Reporting'" in scopes.predicate


def test_a_multi_scope_predicate_is_parenthesised_so_it_survives_composition():
    """`AND` binds tighter than `OR`, and every use of this composes with `AND`.

    Unparenthesised, ``WHERE <scopes> AND NOT (<keep>)`` reassociates to
    ``(a AND b) OR ((c AND d) AND NOT (<keep>))`` — the first scope's rows are
    deleted regardless of the keep-list — and a ``MERGE`` ``ON`` clause matches
    every source row to every target row in the later scopes. This cost a real
    Delta failure to find, and it is invisible until a second installation
    exists, so it is pinned here where it costs nothing to check.
    """

    scopes = InstallationScopes(
        (
            InstallationScope("Lakehouse", "Sales"),
            InstallationScope("Lakehouse", "Inventory"),
        )
    )

    rendered = scopes.predicate

    assert rendered.startswith("(") and rendered.endswith(")")
    # The whole disjunction is inside one group, not merely each conjunct.
    depth = 0
    for character in rendered[:-1]:
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        assert depth > 0, "the outer group closes before the predicate ends"


def test_composing_a_multi_scope_predicate_with_and_keeps_every_scope_guarded():
    """The composition itself, rather than its punctuation."""

    scopes = InstallationScopes(
        (
            InstallationScope("Lakehouse", "Sales"),
            InstallationScope("Lakehouse", "Inventory"),
        )
    )

    composed = f"WHERE {scopes.predicate} AND NOT (`object_name` = 'X')"

    # Every scope must sit inside the group that the trailing AND applies to.
    guarded = composed[composed.index("(") : composed.rindex(") AND NOT")]
    assert "'Sales'" in guarded and "'Inventory'" in guarded


def test_a_repeated_scope_is_addressed_once():
    scopes = InstallationScopes(
        (
            InstallationScope("Lakehouse", "Sales"),
            InstallationScope("Lakehouse", "Sales"),
        )
    )

    assert len(scopes) == 1
    assert " OR " not in scopes.predicate


def test_no_scopes_refuses_to_render_a_predicate():
    with pytest.raises(BuildError, match="whole catalogue"):
        InstallationScopes(()).predicate


def test_a_qualified_predicate_qualifies_every_scope():
    scopes = InstallationScopes(
        (
            InstallationScope("Lakehouse", "Sales"),
            InstallationScope("Lakehouse", "Inventory"),
        )
    )

    rendered = scopes.predicate_for("target")

    assert rendered.count("target.") == 4
