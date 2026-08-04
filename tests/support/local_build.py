"""One build environment on the local Spark emulator.

The in-process half of `tests/support/build_env.py`: everything a build needs
when the resources are a `tmp_path` and the code runs here. No Fabric import, no
session, no credentials — which is the point, because this is what `-m spark`
runs against and it must not reach for a workspace to do it.
"""

from __future__ import annotations

import shutil
from contextlib import contextmanager
from pathlib import Path

import pytest

from weaver.targets import ItemRef
from weaver.resolution import LocalResolver
from weaver.store import LocalStore
from weaver.workspaces import LocalWorkspace
from weaver.locations import Location
from weaver.spark import SparkCatalogue

from .build_env import BuildEnv, InstallOutcome, _bindings_for, _outcome_from_report, _upload_tree


_LOCAL_SCHEMAS = ("DWG", "Raw", "Legacy", "Sales", "Reporting", "Wh", "Rpt", "_")


def _local_lakehouse_setup(root, extra=()):
    from weaver.targets import ItemRef
    from weaver.workspaces import LocalWorkspace
    from weaver.resolution import LocalResolver
    from weaver.store import LocalStore

    workspace = LocalWorkspace(workspace=root, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(workspace)
    weaver, target = ItemRef("Weaver"), ItemRef("Sales_LH")
    for item in (weaver, target, *(ItemRef(name) for name in extra)):
        store.make_directory(resolver.files_root(item))
        store.make_directory(resolver.tables_root(item))
    return workspace, weaver, target, resolver, store


@contextmanager
def _local_build_context(root, spark, weaver_repo_fixture):
    """A local Spark BuildEnv over a fresh Lakehouse root. Used by both the
    function-scoped fixture and the module-scoped estate."""

    from weaver.build_bundle import (
        InstallationEnvironment,
        LakehouseBinding,
        effective_item_bindings,
        install_bundle,
        load_bundle,
    )
    from weaver.build_bundle.workflow import (
        read_reconciled_catalogue,
        read_target_inventories,
    )
    from weaver.build_bundle.planner import generate_item_build_bundle
    from weaver.declaration import parse_item_repository

    workspace, weaver, target, resolver, store = _local_lakehouse_setup(
        root, extra=weaver_repo_fixture.extra_lakehouses
    )
    destination = resolver.spark_destination(target)
    weaver_destination = resolver.spark_destination(weaver)
    repository_root = Location(str(Path(root) / "repository"))
    from weaver.targets import ItemRef as _ItemRef

    named_lakehouses = {
        item: _ItemRef(name)
        for item, name in weaver_repo_fixture.lakehouse_names.items()
    }

    def install_repo() -> None:
        destination = repository_root
        if store.exists(destination):
            store.delete(destination, recursive=True)
        _upload_tree(store, weaver_repo_fixture.path, destination)

    def remove_repo() -> None:
        store.delete(repository_root, recursive=True)

    def generate(bundle_name: str = "buildtest"):
        root_location = repository_root
        repository = parse_item_repository(root_location, store=store)
        control = LakehouseBinding(lakehouse=weaver)
        bindings = effective_item_bindings(
            _bindings_for(
                weaver_repo_fixture, lakehouse=target, lakehouses=named_lakehouses
            ),
            weaver_lakehouse=weaver.name,
        )
        environment = InstallationEnvironment(
            store=store,
            resolver=resolver,
            spark=spark,
            workspace=workspace,
        )
        inventories = read_target_inventories(bindings, environment=environment)
        reconciled = read_reconciled_catalogue(
            bindings,
            inventories=inventories,
            environment=environment,
            repository=repository,
        )
        return generate_item_build_bundle(
            repository,
            bindings=bindings,
            output=resolver.build_bundle(bundle_name),
            store=store,
            control_lakehouse=control,
            target_inventories=inventories,
            catalogue=reconciled.catalogue,
            stale_claims=reconciled.stale_claims,
        )

    def install(bundle) -> InstallOutcome:
        report = install_bundle(
            load_bundle(bundle.location, store=store),
            environment=InstallationEnvironment(store=store, resolver=resolver, spark=spark),
        )
        return _outcome_from_report(report)

    def query(sql: str) -> list:
        return [row.asDict() for row in spark.sql(sql).collect()]

    def columns(table: str) -> list:
        return [
            {"name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable}
            for f in spark.table(table).schema
        ]

    def schema_exists(qualified: str) -> bool:
        return bool(spark.catalog.databaseExists(qualified))

    def run_python(body: str, *, label: str = "shared body"):
        """Run a shared body in this process, against the local session.

        The Fabric side sends the same text to a Livy session, where ``emit`` is
        part of the bootstrap. Here it is a local closure, so a body written once
        runs unchanged either side. ``label`` exists for the Fabric side's Livy
        accounting and is ignored here, where a body costs nothing to run.
        """

        emitted = []
        namespace = {
            "spark": spark,
            "resolver": resolver,
            "target": target,
            "emit": emitted.append,
        }
        exec(compile(body, "<shared body>", "exec"), namespace)
        return emitted[-1] if emitted else None

    def seed_orphans() -> None:
        # Seeded *in the destination*, through the same addressing the build uses,
        # so what prune has to find is genuinely in the Lakehouse under test.
        catalogue = SparkCatalogue(spark, destination)
        for schema in ("DWG", "Legacy"):
            catalogue.create_schema(schema)
        catalogue.sql("CREATE TABLE {{object:DWG.OldTable}} (x int) USING delta")
        catalogue.sql("CREATE OR REPLACE VIEW {{object:DWG.OldView}} AS SELECT 1 AS x")
        catalogue.sql("CREATE TABLE {{object:Legacy.OldThing}} (x int) USING delta")
        files_root = resolver.files_root(target)
        store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
        store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")

    named_destinations = {
        item: resolver.spark_destination(ref)
        for item, ref in named_lakehouses.items()
    }
    try:
        yield BuildEnv(
            label="local", workspace=workspace, weaver=weaver, target=target,
            resolver=resolver, store=store, repository_root=repository_root,
            generate_spark=spark,
            install_repo=install_repo, remove_repo=remove_repo, generate=generate,
            install=install, run_query=query, run_columns=columns,
            seed_orphans=seed_orphans, run_schema_exists=schema_exists,
            run_python=run_python,
            destination=destination, weaver_destination=weaver_destination,
            destinations=named_destinations,
        )
    finally:
        for schema in _LOCAL_SCHEMAS:
            for place in (destination, weaver_destination, *named_destinations.values()):
                spark.sql(
                    f"DROP SCHEMA IF EXISTS {place.qualified_schema(schema)} CASCADE"
                )
