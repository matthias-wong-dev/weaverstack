"""Authored objects against a built target — the same body on local Spark and Fabric.

A three-document repository (``authored-objects-item``: two Python tables and a
Python folder) is installed into the Weaver Lakehouse and built, exactly as any
other estate. Then the objects that repository declares are constructed the way a
developer constructs them — a Spark session and a resolved Lakehouse — and asked
for what the build just made.

The body below is one string, run in whichever process the environment runs in:
in this one against local Spark, or inside a Fabric session over Livy. Its only
transport-dependent line is ``resolver_for``, which the conftest binds before the
body starts, so what is asserted is genuinely the same code on both — including
the folder paths, which hang off the Lakehouse's own root and therefore need no
mount on either side.

The classes are declared in the body rather than imported from the installed
repository because importing one is the load executor's job, and that does not
exist yet. They mirror ``Sales__Order.py`` and its two neighbours in the fixture.
"""

from __future__ import annotations

import pytest
from build_envs import AUTHORED_OBJECTS_FIXTURE

from weaver import DeltaTarget, FolderTarget

pytestmark = pytest.mark.parametrize(
    "weaver_repo_fixture", [AUTHORED_OBJECTS_FIXTURE], indirect=True
)

AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}

#: Run in the environment under test. ``spark``, ``resolver``, ``target`` and
#: ``emit`` are bound by the environment before this runs.
BODY = '''
from weaver import Folder, Table, lakehouse_for

lakehouse = lakehouse_for(resolver, target)


class Sales__OrderExport(Folder):
    def read(self):
        return self.staging_folder(), []


class Sales__Customer(Table):
    def read(self):
        return [], []


class Sales__Order(Table):
    def read(self):
        return [], []


order = Sales__Order(spark, lakehouse=lakehouse)
customer = Sales__Customer(order)
export = Sales__OrderExport(order)

emit({
    "ids": [order.object_id, customer.object_id, export.object_id],
    "spark_root": order.spark_root,
    "table_path": lakehouse.table_path(*order.identity),
    "folder_path": export.path(),
    "staging_folder": export.staging_folder(),
    "order_columns": sorted(f.name.lower() for f in order.dataframe().schema),
    "order_rows": order.dataframe().count(),
    "empty_rows": order.empty_dataframe().count(),
    "empty_columns": sorted(f.name.lower() for f in order.empty_dataframe().schema),
    "customer_columns": sorted(f.name.lower() for f in customer.dataframe().schema),
    "customer_rows": customer.dataframe().count(),
})
'''


@pytest.fixture(scope="module")
def reached(lakehouse_estate):
    """The one round trip into the environment, shared by the assertions."""

    return lakehouse_estate.env.run_python(BODY)


def _delta(env, schema, name):
    return env.resolver.delta_table(DeltaTarget(lakehouse=env.target), schema, name).value


def test_identity_comes_from_the_class_name_in_both_environments(reached):
    assert reached["ids"] == ["Sales.Order", "Sales.Customer", "Sales.OrderExport"]


def test_a_table_reads_the_delta_files_the_build_created(reached, lakehouse_estate):
    env = lakehouse_estate.env

    assert reached["table_path"] == _delta(env, "Sales", "Order")
    # Build creates structure, not data — read() is never called by a build.
    assert reached["order_rows"] == 0
    assert set(reached["order_columns"]) == {"order id", "customer id", "amount"} | AUDIT


def test_a_dependency_reads_its_own_table_through_the_same_session(reached):
    assert reached["customer_rows"] == 0
    assert set(reached["customer_columns"]) == {"customer id", "customer name"} | AUDIT


def test_an_empty_dataframe_keeps_the_built_shape(reached):
    assert reached["empty_rows"] == 0
    assert reached["empty_columns"] == reached["order_columns"]


def test_a_folder_resolves_to_the_directory_the_build_created(reached, lakehouse_estate):
    """No mount anywhere in it: the target Lakehouse is not the attached one on
    Fabric, and a folder still resolves — which is the point of addressing it
    through the Lakehouse's own root."""

    env = lakehouse_estate.env
    target = FolderTarget(lakehouse=env.target)
    folder = env.resolver.folder_object(target, "Sales", "OrderExport")

    assert reached["folder_path"] == folder.value
    assert env.store.exists(folder)


def test_staging_sits_beside_the_folder_it_belongs_to(reached, lakehouse_estate):
    env = lakehouse_estate.env

    assert reached["staging_folder"] == f"{reached['folder_path']}_Staging"
    assert reached["staging_folder"] == env.resolver.folder_staging(
        FolderTarget(lakehouse=env.target), "Sales", "OrderExport"
    ).value
