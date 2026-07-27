"""The same build-and-install lifecycle in Fabric and its local emulator.

Both phases run in the target environment — in-process for the local emulator,
inside Fabric over Livy for the product path — and create a Folder, an empty
declared-shape Delta table, a persistent view and a view-on-view. The desktop
only stages the repository and reads results for the Fabric fixture. The test
body is transport-neutral: it drives a ``BuildEnv`` (see ``conftest``) and
asserts through its store and query callables.

**Every assertion names the Lakehouse it is about.** The session is attached to
the Weaver Lakehouse and the build writes to a *different* one, so a query for a
bare ``DWG.Customer`` would ask the control plane. That is not a hypothetical:
these tests could not previously see a table written to the wrong Lakehouse,
because the write and the read went through the same session catalogue and the
assertion passed either way. ``build_env.query`` resolves the object tokens
against a named destination, so a read can only succeed where the write actually
landed.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

from build_envs import lakehouse_environments as build_environments

from weaver import DeltaTarget, FolderTarget


def _folder(build_env, schema, name):
    return build_env.resolver.folder_object(
        FolderTarget(lakehouse=build_env.target), schema, name
    )


def _table(build_env, schema, name):
    return build_env.resolver.delta_table(
        DeltaTarget(lakehouse=build_env.target), schema, name
    )


def _scalar(rows):
    return next(iter(rows[0].values()))


@build_environments
def test_generate_and_install_lakehouse_bundle(build_env):
    build_env.install_repo("MyRepo")
    bundle = build_env.generate()

    # Plan assertions, before installing.
    assert bundle.plan.format_version == 1
    assert bundle.plan.repository_name == "MyRepo"
    assert len(bundle.plan.targets) == 1 and bundle.plan.targets[0].kind == "lakehouse"
    assert bundle.plan.omitted_nodes == ()

    # Independence: remove the source, then install from the bundle alone.
    build_env.remove_repo("MyRepo")
    assert not build_env.store.exists(build_env.resolver.weaver_items_root)

    outcome = build_env.install(bundle)
    assert outcome.status == "succeeded"

    planned = [action.id for _, _, action in bundle.plan.actions()]
    assert list(outcome.action_order) == planned
    assert all(status == "succeeded" for status in outcome.action_status.values())
    assert outcome.bundle_id == bundle.bundle_id

    # Build creates structure, not data. A Folder is a directory Weaver makes at
    # an exact path; a Delta table is a catalog object (Fabric stores it at a
    # host-chosen path), so it is checked by name.
    assert build_env.store.exists(_folder(build_env, "Raw", "CustomerCsv"))
    assert "customer" in {
        r["tableName"].lower()
        for r in build_env.query(f"SHOW TABLES IN {build_env.schema_name('DWG')}")
    }

    # The name is only half of it. A read through the session catalogue cannot say
    # *which Lakehouse* answered, so the storage is checked too: the table's
    # directory is under the destination's own Tables area.
    #
    # Case-insensitively, and not by exact path, because the physical name is the
    # host's to choose — Fabric lowercases a managed table's directory
    # (`Tables/DWG/customer`) exactly as the local metastore does. Identity in
    # Weaver is case-insensitive, so the assertion is written to that.
    stored = {
        entry.name.lower()
        for entry in build_env.store.list(
            build_env.resolver.tables_root(build_env.target).join("DWG")
        )
        if entry.is_directory
    }
    assert "customer" in stored

    columns = {
        row["col_name"].lower()
        for row in build_env.query("DESCRIBE TABLE {{object:DWG.Customer}}")
        if row["col_name"] and not row["col_name"].startswith("#")
    }
    assert {"customerid", "customername", "isactive"} <= columns
    assert _scalar(build_env.query("SELECT count(*) AS n FROM {{object:DWG.Customer}}")) == 0

    views = {
        row["viewName"].lower()
        for row in build_env.query(f"SHOW VIEWS IN {build_env.schema_name('DWG')}")
    }
    assert {"activecustomer", "activecustomersummary"} <= views
    # The summary resolves through the first view: zero rows over an empty table.
    assert _scalar(
        build_env.query("SELECT CustomerCount FROM {{object:DWG.ActiveCustomerSummary}}")
    ) == 0


@build_environments
def test_nothing_is_built_in_the_weaver_lakehouse(build_env):
    """The control plane is not the destination, and a build must not treat it as one.

    This is the assertion the old two-part names made impossible. The session is
    attached to the Weaver Lakehouse; an unqualified ``CREATE TABLE DWG.Customer``
    lands there; and the old test then read ``DWG.Customer`` back through the same
    session and found it. Asking the *Weaver* Lakehouse directly is what closes it.
    """

    build_env.install_repo("MyRepo")
    build_env.install(build_env.generate())

    weaver = build_env.weaver_destination
    assert not build_env.schema_exists("DWG", destination=weaver)
    assert not build_env.schema_exists("Raw", destination=weaver)
    # And it did land in the destination, so the absence above is not vacuous.
    assert build_env.schema_exists("DWG")


@build_environments
def test_install_report_is_written_into_the_bundle(build_env):
    build_env.install_repo("MyRepo")
    bundle = build_env.generate()
    build_env.install(bundle)
    assert build_env.store.exists(bundle.location.join("install-report.yml"))


def _rebuild_with_broken_summary(build_env, bundle):
    """A copy of the bundle whose summary view payload is invalid, hash matching."""

    from weaver.build_bundle import compute_bundle_id, write_bundle

    store = build_env.store
    payloads = {}
    for _, _, action in bundle.plan.actions():
        if action.payload is None:
            continue
        payloads[action.payload] = store.read(bundle.location.join(*action.payload.split("/")))

    broken = (
        b"CREATE OR REPLACE VIEW {{object:DWG.ActiveCustomerSummary}} AS\n"
        b"select count(*) as CustomerCount from {{object:DWG.ActiveCustomer}} "
        b"where NoSuchColumn = 1\n"
    )

    def fix(action):
        if action.id == "view-DWG.ActiveCustomerSummary":
            payloads[action.payload] = broken
            return replace(action, payload_sha256=hashlib.sha256(broken).hexdigest())
        return action

    sequences = tuple(
        replace(
            seq,
            batches=tuple(
                replace(batch, actions=tuple(fix(a) for a in batch.actions))
                for batch in seq.batches
            ),
        )
        for seq in bundle.plan.sequences
    )
    plan = replace(bundle.plan, sequences=sequences, bundle_id="")
    plan = replace(plan, bundle_id=compute_bundle_id(plan))

    repo_root = bundle.location.join("repository")
    snapshot = {}
    for entry in store.list(repo_root, recursive=True):
        if entry.is_directory:
            continue
        snapshot[entry.location.value[len(repo_root.value) + 1 :]] = store.read(entry.location)

    return write_bundle(
        build_env.resolver.build_bundle("broken"),
        plan=plan, payloads=payloads, snapshot=snapshot, store=store,
    )


@build_environments
def test_a_failing_view_stops_the_build_and_leaves_no_final_view(build_env):
    build_env.install_repo("MyRepo")
    bundle = build_env.generate()
    broken = _rebuild_with_broken_summary(build_env, bundle)

    outcome = build_env.install(broken)

    assert outcome.status == "failed"
    # A clean target needs no prune; everything up to the summary succeeded.
    assert outcome.sequence_status[20] == "succeeded"  # create schema DWG
    assert outcome.sequence_status[40] == "succeeded"  # DWG.Customer
    assert outcome.sequence_status[50] == "succeeded"  # ActiveCustomer
    assert outcome.sequence_status[60] == "failed"     # ActiveCustomerSummary
    assert outcome.action_status["view-DWG.ActiveCustomerSummary"] == "failed"

    views = {
        row["viewName"].lower()
        for row in build_env.query(f"SHOW VIEWS IN {build_env.schema_name('DWG')}")
    }
    assert "activecustomer" in views
    assert "activecustomersummary" not in views


@build_environments
def test_build_prunes_unmanaged_objects_before_creating(build_env):
    build_env.seed_orphans()
    build_env.install_repo("MyRepo")
    bundle = build_env.generate()

    # The build froze a drop per storage-visible orphan (a catalog session also
    # sees views). The installer runs exactly these, enumerating nothing.
    prune_kinds = {a.kind for _, _, a in bundle.plan.actions() if a.kind.startswith("prune")}
    assert {"prune_table", "prune_schema", "prune_folder"} <= prune_kinds
    if build_env.generate_spark is not None:
        assert "prune_view" in prune_kinds

    outcome = build_env.install(bundle)
    assert outcome.status == "succeeded"

    tables_root = build_env.resolver.tables_root(build_env.target)
    files_root = build_env.resolver.files_root(build_env.target)

    assert not build_env.store.exists(tables_root.join("Legacy"))
    assert not build_env.store.exists(files_root.join("Legacy"))
    assert not build_env.store.exists(_folder(build_env, "Raw", "OldFolder"))

    # The orphan schema is gone from the destination — asked of the destination,
    # not of whatever the session happens to list.
    assert not build_env.schema_exists("Legacy")
    dwg_tables = {
        row["tableName"].lower()
        for row in build_env.query(f"SHOW TABLES IN {build_env.schema_name('DWG')}")
    }
    assert "oldtable" not in dwg_tables
    dwg_views = {
        row["viewName"].lower()
        for row in build_env.query(f"SHOW VIEWS IN {build_env.schema_name('DWG')}")
    }
    assert "oldview" not in dwg_views

    # The managed set is present.
    assert "customer" in dwg_tables
    assert build_env.store.exists(_folder(build_env, "Raw", "CustomerCsv"))
