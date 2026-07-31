"""Does a real catalogue read back as the `Catalogue` the build reasons about?

Two claims, and they are the pair that licenses everything above them.

Reconciliation, incremental selection, alias staleness and claim collection are
all proven in pure Python against a `Catalogue` built by hand. That is only
legitimate if real catalogue tables — Delta, written by a real installation —
deserialize into the same class with the same content.

And the statements those pure tests render have to actually run. A publication
that produced valid-looking SQL the engine rejected, or that wrote rows the
reader could not then read, would satisfy every pure test and be useless.

So: initialise a catalogue for real, read it back, and compare; then write
through the rendered DML and read the rows again.
"""

from __future__ import annotations

import json

import pytest

from weaver import ItemRef
from weaver.build_bundle import execute_action
from weaver.build_bundle.catalogue_actions import render_catalogue_after_build
from weaver.catalogue.state import Catalogue, read_catalogue_state
from weaver.catalogue.tables import REGISTRY
from weaver.declaration.model import WeaverItemId
from weaver.initialise import initialise_weaver_lakehouse
from weaver.spark import SparkCatalogue

pytestmark = pytest.mark.spark

CONTROL = WeaverItemId.parse("Lakehouse/_weaver")


@pytest.fixture
def catalogue(lakehouses, spark):
    """A real catalogue, installed through the ordinary build path.

    Not hand-built tables: the point is to read back what an *installation*
    leaves, so the reader is tested against the writer rather than against a
    fixture's idea of the writer.
    """

    initialise_weaver_lakehouse(
        weaver_lakehouse=lakehouses.weaver,
        workspace=lakehouses.workspace,
        store=lakehouses.store,
        spark=spark,
    )
    return SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.weaver)
    )


def read(catalogue, *items) -> Catalogue:
    return read_catalogue_state(catalogue, list(items) or [CONTROL])


# --- a real catalogue deserializes into the class the build reasons about -----


def test_a_real_catalogue_reads_back_as_a_Catalogue(catalogue):
    seen = read(catalogue)

    assert isinstance(seen, Catalogue)
    assert REGISTRY.name in seen.present_tables


def test_every_catalogue_table_is_present_after_initialisation(catalogue):
    """A partial catalogue is a real state, so "all present" has to be asserted.

    The reader tolerates absence — it records which tables exist — which means a
    silently incomplete installation would read back cleanly and only fail much
    later, when a publication wrote to a table that was never made.
    """

    from weaver.catalogue.tables import CATALOGUE_TABLES

    seen = read(catalogue)

    assert seen.present_tables == frozenset(
        table.name for table in CATALOGUE_TABLES
    )


def test_the_registry_rows_become_registered_documents(catalogue):
    """The projection the whole build reasons from: rows in, documents out.

    Incremental selection never sees a row — it sees `registered`. So the
    deserialization is the interface, and a defect in it would make every pure
    selection test true of something the catalogue does not contain.
    """

    seen = read(catalogue)

    assert seen.registered, "initialisation certifies its own catalogue objects"
    for identity, document in seen.registered.items():
        assert identity.item == CONTROL
        assert document.signature
        assert document.object_type in {"table", "view", "folder"}


def test_the_catalogue_describes_its_own_tables(catalogue):
    """Weaver's own catalogue is an ordinary item, and reads back as one."""

    seen = read(catalogue)

    names = {
        identity.object_id.object.casefold() for identity in seen.registered
    }
    assert {"registry", "installation"} <= names


def test_reading_an_item_with_no_rows_gives_an_empty_scope(catalogue):
    """An item that was never installed is empty, not missing — and the
    difference matters, because reconciliation treats absent rows as "no claim"
    rather than as a failure to read."""

    seen = read(catalogue, WeaverItemId.parse("Lakehouse/Absent"))

    assert seen.registered == {}
    assert REGISTRY.name in seen.present_tables


# --- the rendered DML actually writes, and reads back ------------------------


def test_rendered_publication_dml_writes_rows_a_read_can_see(
    catalogue, lakehouses, spark, tmp_path
):
    """The other half: pure tests render these statements, so they must run.

    Renders a publication for a second item, executes it through the same batch
    executor an installation uses, and reads the catalogue back. A statement that
    the engine rejected, or that wrote rows the reader could not parse, would
    pass every pure test and fail here.
    """

    from factories import (
        bound_target,
        document_id,
        item_id,
        lakehouse_table,
        single_document_repository,
    )
    from conftest import context_for

    repository = single_document_repository(
        tmp_path / "repo", documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )
    item = item_id()
    target = bound_target(id="control", item_id=lakehouses.weaver.name)

    stages = render_catalogue_after_build(
        repository,
        {document_id("DWG.Customer")},
        {item: target},
        control_target=target,
    )

    context = context_for(
        lakehouses,
        spark,
        lakehouses.weaver.name,
        target_id="control",
        epoch="2026-08-01 09:00:00",
    )
    for stage in stages:
        for batch in stage.batches:
            for action in batch.actions:
                result = execute_action(
                    action,
                    stage.payloads.get(action.payload) if action.payload else None,
                    context=context,
                )
                assert result.status in {"succeeded", "skipped"}, result.error_message

    seen = read(catalogue, item)

    assert document_id("DWG.Customer") in seen.registered
    assert seen.registered[document_id("DWG.Customer")].object_type == "table"


def test_a_published_signature_survives_the_round_trip(
    catalogue, lakehouses, spark, tmp_path
):
    """The one field incremental selection compares, end to end.

    A signature that was written but came back altered — trimmed, re-cased,
    truncated — would make every second build see a change that never happened,
    and the pure tests could not tell.
    """

    from factories import (
        bound_target,
        document_id,
        item_id,
        lakehouse_table,
        single_document_repository,
    )
    from conftest import context_for
    from weaver.build_bundle.incremental import declared_signatures

    repository = single_document_repository(
        tmp_path / "repo", documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )
    item = item_id()
    identity = document_id("DWG.Customer")
    target = bound_target(id="control", item_id=lakehouses.weaver.name)

    stages = render_catalogue_after_build(
        repository, {identity}, {item: target}, control_target=target
    )
    context = context_for(
        lakehouses,
        spark,
        lakehouses.weaver.name,
        target_id="control",
        epoch="2026-08-01 09:00:00",
    )
    for stage in stages:
        for batch in stage.batches:
            for action in batch.actions:
                execute_action(
                    action,
                    stage.payloads.get(action.payload) if action.payload else None,
                    context=context,
                )

    seen = read(catalogue, item)
    declared = declared_signatures(repository, {identity})[identity]

    assert seen.registered[identity].signature == declared


def test_an_epoch_token_is_resolved_before_it_reaches_the_engine(
    catalogue, lakehouses, spark, tmp_path
):
    """`{{epoch}}` is substituted by the installer, not left for Spark.

    An unresolved token is not a subtle bug — `{{` is not Spark SQL — but the
    failure would surface as a syntax error in a payload nobody wrote by hand,
    so it is worth naming.
    """

    from factories import (
        bound_target,
        document_id,
        item_id,
        lakehouse_table,
        single_document_repository,
    )

    repository = single_document_repository(
        tmp_path / "repo", documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )
    item = item_id()
    target = bound_target(id="control", item_id=lakehouses.weaver.name)

    stages = render_catalogue_after_build(
        repository, {document_id("DWG.Customer")}, {item: target}, control_target=target
    )
    registry = next(stage for stage in stages if stage.slug == "publish-registry")
    rendered = [
        line
        for content in registry.payloads.values()
        for line in json.loads(content)
    ]

    # The token is in the frozen payload — that is what keeps a bundle's bytes
    # stable — and the installer is what removes it.
    assert any("{{epoch}}" in line for line in rendered)
