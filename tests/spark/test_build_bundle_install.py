"""The first vertical slice, end to end: generate a bundle and install it.

The build reads the repository installed in the Weaver Lakehouse and writes a
fully bound Lakehouse bundle; the installer then materialises, from that bundle
alone, a real Folder, a populated Delta table, a persistent Spark view over it,
and a persistent view over that view. The source repository is deleted between
the two steps, so a passing install proves the installer runs the certified
snapshot rather than quietly re-reading the source.
"""

from __future__ import annotations

import pytest

from weaver import DeltaTarget, FolderTarget, RepositoryRef
from weaver.build import generate_build_bundle, install_bundle, load_bundle

pytestmark = pytest.mark.spark


def _generate(lakehouses, bindings):
    output = lakehouses.location("_bundle_output")
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

        # --- physical: Folder ---
        folder = lakehouses.resolver.folder_object(
            FolderTarget(lakehouse=lakehouses.target), "Raw", "CustomerCsv"
        )
        assert (folder.path / "customers.csv").exists()

        # --- physical: Delta table ---
        table_path = lakehouses.resolver.delta_table(
            DeltaTarget(lakehouse=lakehouses.target), "DWG", "Customer"
        ).value
        rows = spark.read.format("delta").load(table_path).collect()
        assert len(rows) == 4
        assert {r["CustomerName"] for r in rows} == {"Ada", "Bo", "Cy", "Di"}

        # --- physical: persistent view, and view-on-view ---
        views = {r["viewName"].lower() for r in spark.sql("SHOW VIEWS IN DWG").collect()}
        assert {"activecustomer", "activecustomersummary"} <= views

        active = spark.sql("SELECT CustomerName FROM DWG.ActiveCustomer").collect()
        assert {r["CustomerName"] for r in active} == {"Ada", "Cy", "Di"}

        # The summary resolves *through* the first view: three active customers.
        summary = spark.sql("SELECT CustomerCount FROM DWG.ActiveCustomerSummary").collect()
        assert summary[0]["CustomerCount"] == 3

        # The second view genuinely depends on the first, not a substituted query.
        spark.sql("DROP VIEW DWG.ActiveCustomer")
        with pytest.raises(Exception):
            spark.sql("SELECT * FROM DWG.ActiveCustomerSummary").collect()
    finally:
        spark.sql("DROP DATABASE IF EXISTS DWG CASCADE")
        spark.sql("DROP DATABASE IF EXISTS Raw CASCADE")


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
