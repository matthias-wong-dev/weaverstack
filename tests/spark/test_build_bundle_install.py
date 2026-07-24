"""The first vertical slice, end to end: generate a bundle and install it.

The build reads the repository installed in the Weaver Lakehouse and writes a
fully bound Lakehouse bundle; the installer then materialises, from that bundle
alone, a real Folder, a populated Delta table, a persistent Spark view over it,
and a persistent view over that view. The source repository is deleted between
the two steps, so a passing install proves the installer runs the certified
snapshot rather than quietly re-reading the source.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from weaver import DeltaTarget, FolderTarget, RepositoryRef
from weaver.build_bundle import (
    compute_bundle_id,
    generate_build_bundle,
    install_bundle,
    load_bundle,
    write_bundle,
)

pytestmark = pytest.mark.spark


def _generate(lakehouses, bindings, name="20260724T000000"):
    # A kept bundle lives under the Weaver Lakehouse, named for the build —
    # normally a timestamp. An install-bound bundle would use a throwaway dir.
    output = lakehouses.resolver.build_bundle(name)
    bundle = generate_build_bundle(
        weaver_lakehouse=lakehouses.weaver,
        repository_name="MyRepo",
        targets=bindings,
        output=output,
        host=lakehouses.host,
        store=lakehouses.store,
    )
    return bundle, output


def test_generate_and_install_lakehouse_bundle(
    spark, lakehouses, installed_build_repository, lakehouse_only_bindings, installation_environment
):
    bundle, output = _generate(lakehouses, lakehouse_only_bindings)

    # Plan assertions, before installing.
    assert bundle.plan.format_version == 1
    assert bundle.plan.repository_name == "MyRepo"
    assert len(bundle.plan.targets) == 1 and bundle.plan.targets[0].kind == "lakehouse"
    assert bundle.plan.omitted_nodes == ()

    # The strongest independence proof: remove the source, then install.
    lakehouses.store.delete(
        lakehouses.resolver.repository(RepositoryRef("MyRepo")), recursive=True
    )
    assert not lakehouses.store.exists(lakehouses.resolver.repository(RepositoryRef("MyRepo")))

    try:
        report = install_bundle(load_bundle(output, store=lakehouses.store),
                                environment=installation_environment)

        assert report.status == "succeeded"

        # Every planned action has exactly one result, all succeeded.
        planned = [action.id for _, _, action in bundle.plan.actions()]
        reported = [r.action_id for r in report.action_results()]
        assert reported == planned
        assert all(r.status == "succeeded" for r in report.action_results())
        assert report.bundle_id == bundle.bundle_id

        # Build creates structure, not data — so these assert shape, not rows.

        # --- physical: Folder directory (staging files into it is load) ---
        folder = lakehouses.resolver.folder_object(
            FolderTarget(lakehouse=lakehouses.target), "Raw", "CustomerCsv"
        )
        assert folder.path.is_dir()

        # --- physical: an empty Delta table of the declared shape ---
        table_path = lakehouses.resolver.delta_table(
            DeltaTarget(lakehouse=lakehouses.target), "DWG", "Customer"
        ).value
        table = spark.read.format("delta").load(table_path)
        assert [f.name for f in table.schema.fields] == ["CustomerId", "CustomerName", "IsActive"]
        assert table.count() == 0
        # It binds by its two-part name too, the way a declared Fabric table does.
        assert spark.table("DWG.Customer").count() == 0

        # --- physical: persistent view, and view-on-view ---
        views = {r["viewName"].lower() for r in spark.sql("SHOW VIEWS IN DWG").collect()}
        assert {"activecustomer", "activecustomersummary"} <= views
        # The summary resolves through the first view: zero rows over an empty table.
        summary = spark.sql("SELECT CustomerCount FROM DWG.ActiveCustomerSummary").collect()
        assert summary[0]["CustomerCount"] == 0

        # The second view genuinely depends on the first, not a substituted query.
        spark.sql("DROP VIEW DWG.ActiveCustomer")
        with pytest.raises(Exception):
            spark.sql("SELECT * FROM DWG.ActiveCustomerSummary").collect()
    finally:
        spark.sql("DROP DATABASE IF EXISTS DWG CASCADE")
        spark.sql("DROP DATABASE IF EXISTS Raw CASCADE")


def _rebuild_with_broken_summary(lakehouses, bundle, output):
    """A copy of the bundle whose summary view payload is invalid, hash matching.

    Not corruption — a deliberately rebuilt bundle that passes preflight and only
    fails when Spark analyses the bad view, so the barrier can be observed.
    """

    store = lakehouses.store
    payloads = {}
    for _, _, action in bundle.plan.actions():
        if action.payload is None:  # folder and prune actions carry none
            continue
        payloads[action.payload] = store.read(output.join(*action.payload.split("/")))

    broken = (
        b"CREATE OR REPLACE VIEW DWG.ActiveCustomerSummary AS\n"
        b"select count(*) as CustomerCount from DWG.ActiveCustomer where NoSuchColumn = 1\n"
    )

    def fix_action(action):
        if action.id == "view-DWG.ActiveCustomerSummary":
            payloads[action.payload] = broken
            return replace(action, payload_sha256=hashlib.sha256(broken).hexdigest())
        return action

    sequences = tuple(
        replace(
            sequence,
            batches=tuple(
                replace(batch, actions=tuple(fix_action(a) for a in batch.actions))
                for batch in sequence.batches
            ),
        )
        for sequence in bundle.plan.sequences
    )
    plan = replace(bundle.plan, sequences=sequences, bundle_id="")
    plan = replace(plan, bundle_id=compute_bundle_id(plan))

    repo_root = output.join("repository")
    snapshot = {}
    for entry in store.list(repo_root, recursive=True):
        if entry.is_directory:
            continue
        relative = entry.location.value[len(repo_root.value) + 1 :]
        snapshot[relative] = store.read(entry.location)

    broken_location = lakehouses.location("_broken_bundle")
    return write_bundle(broken_location, plan=plan, payloads=payloads, snapshot=snapshot, store=store)


def test_a_failing_view_stops_the_build_and_leaves_no_final_view(
    spark, lakehouses, installed_build_repository, lakehouse_only_bindings, installation_environment
):
    bundle, output = _generate(lakehouses, lakehouse_only_bindings)
    broken = _rebuild_with_broken_summary(lakehouses, bundle, output)

    try:
        report = install_bundle(broken, environment=installation_environment)

        assert report.status == "failed"
        by_number = {s.number: s for s in report.sequences}
        # Everything up to the summary succeeded; the summary sequence failed.
        assert by_number[10].status == "succeeded"  # prune
        assert by_number[40].status == "succeeded"  # DWG.Customer built
        assert by_number[50].status == "succeeded"  # ActiveCustomer built
        assert by_number[60].status == "failed"     # ActiveCustomerSummary

        failing = [r for r in report.action_results() if r.status == "failed"]
        assert len(failing) == 1
        assert failing[0].action_id == "view-DWG.ActiveCustomerSummary"

        # The first view was built; the failing one was not.
        views = {r["viewName"].lower() for r in spark.sql("SHOW VIEWS IN DWG").collect()}
        assert "activecustomer" in views
        assert "activecustomersummary" not in views
    finally:
        spark.sql("DROP DATABASE IF EXISTS DWG CASCADE")
        spark.sql("DROP DATABASE IF EXISTS Raw CASCADE")


def _seed_orphans(spark, lakehouses):
    """Put unmanaged objects in the target that a build must reconcile away."""

    store = lakehouses.store
    tables_root = lakehouses.resolver.tables_root(lakehouses.target).value
    files_root = lakehouses.resolver.files_root(lakehouses.target)

    # An unmanaged table and view inside a schema the build *does* manage.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS DWG LOCATION '{tables_root}/DWG'")
    spark.sql("CREATE TABLE DWG.OldTable (x int) USING delta")
    spark.sql("CREATE OR REPLACE VIEW DWG.OldView AS SELECT 1 AS x")
    # A whole unmanaged schema.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS Legacy LOCATION '{tables_root}/Legacy'")
    spark.sql("CREATE TABLE Legacy.OldThing (x int) USING delta")
    # An unmanaged folder in a managed folder-schema, and an unmanaged schema folder.
    store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
    store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")


def test_build_prunes_unmanaged_objects_before_creating(
    spark, lakehouses, installed_build_repository, lakehouse_only_bindings, installation_environment
):
    _seed_orphans(spark, lakehouses)
    bundle, output = _generate(lakehouses, lakehouse_only_bindings)

    try:
        report = install_bundle(load_bundle(output, store=lakehouses.store),
                                environment=installation_environment)
        assert report.status == "succeeded"

        tables_root = lakehouses.resolver.tables_root(lakehouses.target)
        files_root = lakehouses.resolver.files_root(lakehouses.target)

        # Orphans are gone — physically and from the catalog.
        assert not lakehouses.store.exists(tables_root.join("DWG", "OldTable"))
        assert not lakehouses.store.exists(tables_root.join("Legacy"))
        assert not lakehouses.store.exists(files_root.join("Raw", "OldFolder"))
        assert not lakehouses.store.exists(files_root.join("Legacy"))

        databases = {r[0].lower() for r in spark.sql("SHOW DATABASES").collect()}
        assert "legacy" not in databases
        dwg_tables = {r["tableName"].lower() for r in spark.sql("SHOW TABLES IN DWG").collect()}
        assert "oldtable" not in dwg_tables
        dwg_views = {r["viewName"].lower() for r in spark.sql("SHOW VIEWS IN DWG").collect()}
        assert "oldview" not in dwg_views

        # The managed set is present.
        assert lakehouses.store.exists(tables_root.join("DWG", "Customer"))
        assert lakehouses.store.exists(files_root.join("Raw", "CustomerCsv"))
        assert {"activecustomer", "activecustomersummary"} <= dwg_views | {
            r["viewName"].lower() for r in spark.sql("SHOW VIEWS IN DWG").collect()
        }
    finally:
        spark.sql("DROP DATABASE IF EXISTS DWG CASCADE")
        spark.sql("DROP DATABASE IF EXISTS Legacy CASCADE")


def test_install_report_is_written_into_the_bundle(
    spark, lakehouses, installed_build_repository, lakehouse_only_bindings, installation_environment
):
    bundle, output = _generate(lakehouses, lakehouse_only_bindings)
    try:
        install_bundle(load_bundle(output, store=lakehouses.store),
                       environment=installation_environment)
        assert lakehouses.store.exists(output.join("install-report.yml"))
    finally:
        spark.sql("DROP DATABASE IF EXISTS DWG CASCADE")
        spark.sql("DROP DATABASE IF EXISTS Raw CASCADE")
