"""Which schemas an item needs, and what creating one looks like on each side.

Reached until now only through `plan_item_build`, which meant "a schema was
created" and "the right statement was created" failed the same test the same way.
The interesting parts are invisible from up there: the alias namespace nothing
declares a document in, the bracket escaping, and the fact that a Lakehouse
schema and a Warehouse schema are different payloads for different executors.

All of it is a set difference plus a rendering, so none of it needs an engine.
"""

from __future__ import annotations

from factories import bound_target, document_id, item_id, target_inventory

from weaver.build_bundle.physical import item_schema_stage

CUSTOMER = "DWG.Customer"


def stage(*names, inventory=None, target=None, extra_schemas=()):
    return item_schema_stage(
        {document_id(name) for name in names},
        item=item_id(),
        target=target or bound_target(),
        inventory=inventory if inventory is not None else target_inventory(),
        extra_schemas=extra_schemas,
    )


def actions(planned):
    if planned is None:
        return []
    return [action for batch in planned.batches for action in batch.actions]


def payload_of(planned, action):
    return planned.payloads[action.payload]


# --- which schemas ------------------------------------------------------------


def test_a_schema_the_target_lacks_is_created():
    planned = stage(CUSTOMER)

    assert [action.kind for action in actions(planned)] == ["create_schema"]


def test_a_schema_the_target_already_holds_is_not():
    planned = stage(CUSTOMER, inventory=target_inventory(schemas=("DWG",)))

    assert planned is None


def test_the_comparison_ignores_case():
    """The physical name's case is the workspace's to choose.

    Fabric and the local metastore both fold identifiers, so comparing exactly
    would ask for a schema that is already there — and `CREATE SCHEMA` without
    `IF NOT EXISTS` fails on it.
    """

    planned = stage(CUSTOMER, inventory=target_inventory(schemas=("dwg",)))

    assert planned is None


def test_nothing_needed_is_no_stage_rather_than_an_empty_one():
    """An empty barrier is still a barrier the installer runs and reports."""

    assert stage() is None


def test_a_files_document_needs_no_catalogue_schema():
    """A folder lives under Files, which has no schema to create."""

    planned = stage("Lakehouse/Sales/Files/Raw.CustomerCsv")

    assert planned is None


def test_another_items_documents_are_not_this_items_schemas():
    planned = stage("Lakehouse/Other/DWG.Thing")

    assert planned is None


def test_an_alias_namespace_is_created_though_no_document_lives_in_it():
    """The case that cannot be seen from the documents alone.

    An alias destination lands in one of the item's own schemas, but no
    *document* of the item need be declared there. A build that created only the
    schemas its documents needed would leave the alias homeless — and the alias
    action would fail against a namespace that was never made.
    """

    planned = stage(extra_schemas=("Curated",))

    assert [action.id for action in actions(planned)] == [
        "schema-Lakehouse--Sales-Curated"
    ]


def test_a_schema_wanted_twice_is_created_once():
    """Two documents in one schema, and an alias in it too, is still one create."""

    planned = stage(CUSTOMER, "DWG.Order", extra_schemas=("DWG",))

    assert len(actions(planned)) == 1


def test_schemas_are_ordered_so_the_payload_is_stable():
    """A bundle's identity is its bytes, so set iteration must not reach them."""

    first = stage(CUSTOMER, extra_schemas=("Archive", "Curated"))
    second = stage(CUSTOMER, extra_schemas=("Curated", "Archive"))

    assert [a.id for a in actions(first)] == [a.id for a in actions(second)]
    assert first.payloads == second.payloads


# --- what a create looks like on each side ------------------------------------


def test_a_lakehouse_schema_is_finished_spark_sql():
    """No instruction for the installer to complete: a Fabric Lakehouse pins
    its own storage, so the statement is known when the bundle is generated."""

    planned = stage(CUSTOMER)
    (action,) = actions(planned)

    assert action.executor == "spark_sql"
    assert action.payload.endswith(".spark.sql")
    assert payload_of(planned, action).decode() == (
        "CREATE SCHEMA IF NOT EXISTS `Demo`.`Sales_LH`.`DWG`\n"
    )


def test_a_warehouse_schema_is_t_sql():
    planned = stage(CUSTOMER, target=bound_target(kind="warehouse"))
    (action,) = actions(planned)

    assert action.executor == "tsql"
    assert action.payload.endswith(".sql")
    assert payload_of(planned, action).decode() == "create schema [DWG];\n"


def test_a_warehouse_schema_name_has_its_brackets_escaped():
    """A `]` in an identifier ends the quoting unless it is doubled.

    Weaver's own names could not contain one, but the schema comes from a
    declaration and the statement is generated — so the escaping is Weaver's to
    get right, not the author's to avoid.
    """

    planned = stage(
        "Lakehouse/Sales/Od]d.Customer", target=bound_target(kind="warehouse")
    )
    (action,) = actions(planned)

    assert payload_of(planned, action).decode() == "create schema [Od]]d];\n"


def test_the_payload_hash_matches_what_the_action_carries():
    """The installer verifies this before executing."""

    import hashlib

    planned = stage(CUSTOMER)
    (action,) = actions(planned)

    assert (
        action.payload_sha256 == hashlib.sha256(payload_of(planned, action)).hexdigest()
    )


def test_the_stage_is_bound_to_the_target_it_was_planned_for():
    """A batch names one target; a schema created against another is a schema in
    the wrong Lakehouse, which reads back correctly and is still wrong."""

    planned = stage(CUSTOMER, target=bound_target(id="other-target"))

    assert [batch.target_id for batch in planned.batches] == ["other-target"]
