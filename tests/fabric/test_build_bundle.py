"""The same build-and-install lifecycle on local and Fabric hosts.

Generation runs on the caller and writes a frozen bundle to the Weaver Lakehouse;
installation runs where the host lives — in-process for local, inside Fabric over
Livy — and creates a Folder, an empty declared-shape Delta table, a persistent
view and a view-on-view. The test body is transport-neutral: it drives a
``BuildEnv`` (see ``conftest``) and asserts through its store and query callables.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from weaver import DeltaTarget, FolderTarget, RepositoryRef

build_hosts = pytest.mark.parametrize(
    "build_env",
    [
        pytest.param("local_build_env", id="local", marks=pytest.mark.spark),
        pytest.param("fabric_build_env", id="fabric", marks=pytest.mark.fabric),
    ],
    indirect=True,
)


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


@build_hosts
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
    assert not build_env.store.exists(build_env.resolver.repository(RepositoryRef("MyRepo")))

    outcome = build_env.install(bundle)
    assert outcome.status == "succeeded"

    planned = [action.id for _, _, action in bundle.plan.actions()]
    assert list(outcome.action_order) == planned
    assert all(status == "succeeded" for status in outcome.action_status.values())
    assert outcome.bundle_id == bundle.bundle_id

    # Build creates structure, not data.
    assert build_env.store.exists(_folder(build_env, "Raw", "CustomerCsv"))
    assert build_env.store.exists(_table(build_env, "DWG", "Customer"))

    columns = {
        row["col_name"].lower()
        for row in build_env.query("DESCRIBE TABLE DWG.Customer")
        if row["col_name"] and not row["col_name"].startswith("#")
    }
    assert {"customerid", "customername", "isactive"} <= columns
    assert _scalar(build_env.query("SELECT count(*) AS n FROM DWG.Customer")) == 0

    views = {row["viewName"].lower() for row in build_env.query("SHOW VIEWS IN DWG")}
    assert {"activecustomer", "activecustomersummary"} <= views
    # The summary resolves through the first view: zero rows over an empty table.
    assert _scalar(build_env.query("SELECT CustomerCount FROM DWG.ActiveCustomerSummary")) == 0


@build_hosts
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
        b"CREATE OR REPLACE VIEW DWG.ActiveCustomerSummary AS\n"
        b"select count(*) as CustomerCount from DWG.ActiveCustomer where NoSuchColumn = 1\n"
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


@build_hosts
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

    views = {row["viewName"].lower() for row in build_env.query("SHOW VIEWS IN DWG")}
    assert "activecustomer" in views
    assert "activecustomersummary" not in views


@build_hosts
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

    databases = {_scalar([row]).lower() for row in build_env.query("SHOW DATABASES")}
    assert "legacy" not in databases
    dwg_tables = {row["tableName"].lower() for row in build_env.query("SHOW TABLES IN DWG")}
    assert "oldtable" not in dwg_tables

    # The managed set is present.
    assert build_env.store.exists(_table(build_env, "DWG", "Customer"))
    assert build_env.store.exists(_folder(build_env, "Raw", "CustomerCsv"))
