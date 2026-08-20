"""The runtime shortcut API, proven against the external estate.

The other Fabric tests prove that REST created a shortcut and that Spark SQL can
query it. Neither proves the contract a program is written against, which is
the one that matters to an author:

.. code-block:: python

    from shortcuts import Reference, Sales__ExternalCustomer, Sales__Sentinel

    Sales__ExternalCustomer(self).dataframe()
    Reference.Customer.dataframe()
    Sales__Sentinel(self).path()

So this drives the whole way through: a repository declaring physical shortcuts
into ``PYTEST_WORKSPACE_EXT``, a build that creates them, the generated runtime
module deployed beside the item's programs, and a load that reads the known
external rows through the names the author wrote.

The rows and bytes it asserts on are the ones ``provision_estate.py`` seeded, so
what this proves is that the data arrived from the external workspace rather
than from anything this suite wrote locally.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from support import external_estate
from support.build_env import _install_estate
from support.build_envs import SesFixture
from support.weaver_test import weaver_test

#: The one table the load materialises. It reads every shortcut kind and returns
#: what it found, so a single loaded table is evidence for all three.
LOADED = "DWG.ExternalEvidence"


def _repository(root: Path, *, workspace: str, item_name: str) -> Path:
    """A repository declaring physical shortcuts into the external workspace.

    Written here rather than kept on disk because a physical target names a real
    Fabric workspace, and which one is this tenant's business.
    """

    item = root / "Lakehouse" / item_name
    (item / "schemas").mkdir(parents=True)
    (item / "schemas" / "DWG.yml").write_text(
        "Schema ID: DWG\nDescription: Where the evidence lands.\n", encoding="utf-8"
    )
    (item / "shortcuts.py").write_text(
        "from weaver import Shortcut\n"
        "\n"
        "DWG__ExternalCustomer = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="physical",\n'
        f'    target="Lakehouse/{external_estate.ROLES["external"]}/'
        f'{external_estate.SCHEMA}.Customer",\n'
        f'    workspace="{workspace}",\n'
        ")\n"
        "\n"
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target_type="physical",\n'
        f'    target="Lakehouse/{external_estate.ROLES["external"]}/'
        f'{external_estate.SCHEMA}",\n'
        f'    workspace="{workspace}",\n'
        ")\n"
        "\n"
        "DWG__Sentinel = Shortcut(\n"
        '    shortcut_type="folder",\n'
        '    target_type="physical",\n'
        f'    target="Lakehouse/{external_estate.ROLES["external"]}/'
        f'Files/{external_estate.SCHEMA}",\n'
        f'    workspace="{workspace}",\n'
        ")\n",
        encoding="utf-8",
    )
    (item / "DWG__ExternalEvidence.py").write_text(
        '"""\n'
        f"Table ID: {LOADED}\n"
        "\n"
        "Description: What the runtime shortcut API returned.\n"
        "\n"
        "Lineage: The external workspace, through this item's shortcuts.\n"
        "\n"
        "Primary key: Source\n"
        "\n"
        "Schema:\n"
        "  Source: string\n"
        "  Detail: string\n"
        '"""\n'
        "\n"
        "from shortcuts import DWG__ExternalCustomer, DWG__Sentinel, Reference\n"
        "\n"
        "from weaver import Table\n"
        "\n"
        "\n"
        "class DWG__ExternalEvidence(Table):\n"
        "    def read(self):\n"
        "        # An explicit table shortcut: the local destination, addressed\n"
        "        # as any other Weaver table is.\n"
        "        customers = DWG__ExternalCustomer(self).dataframe()\n"
        "        # A schema shortcut, by attribute and by name. Its tables are\n"
        "        # the source item's and are not repository documents.\n"
        "        by_attribute = Reference(self).Customer.dataframe()\n"
        "        products = Reference(self).table('Product').dataframe()\n"
        "        # A folder shortcut, read as Spark reads a folder.\n"
        "        sentinel = self.spark.read.text(\n"
        f"            DWG__Sentinel(self).spark_path() + '/{external_estate.FILE}'\n"
        "        )\n"
        "        rows = [\n"
        "            ('table', ','.join(\n"
        "                sorted(r.CustomerName for r in customers.collect())\n"
        "            )),\n"
        "            ('schema_attribute', ','.join(\n"
        "                sorted(r.CustomerName for r in by_attribute.collect())\n"
        "            )),\n"
        "            ('schema_named', ','.join(\n"
        "                sorted(r.ProductName for r in products.collect())\n"
        "            )),\n"
        "            ('folder', sentinel.collect()[0].value),\n"
        "        ]\n"
        "        return self.spark.createDataFrame(rows, 'Source string, Detail string')\n",
        encoding="utf-8",
    )
    return root


@pytest.fixture
def external_shortcut_estate(request, tmp_path, fabric_external_lakehouse):
    """One estate whose shortcuts point at the external workspace."""

    from conftest import _fabric_build_context

    workspace = os.environ.get(
        external_estate.WORKSPACE_ENV, external_estate.DEFAULT_WORKSPACE
    )
    item = f"Lakehouse/{request.getfixturevalue('fabric_target_lakehouse').name}"
    root = _repository(
        tmp_path / "repo",
        workspace=workspace,
        item_name=item.split("/", 1)[1],
    )

    with _fabric_build_context(
        request.getfixturevalue("fabric_workspace_item"),
        request.getfixturevalue("fabric_client"),
        request.getfixturevalue("fabric_workspace"),
        request.getfixturevalue("fabric_target_lakehouse"),
        request.getfixturevalue("fabric_staging_lakehouse"),
        request.getfixturevalue("livy_session"),
        SesFixture(root, (item,)),
        before_reset=request.getfixturevalue("weaver_session").flush,
    ) as env:
        yield env, item


#: The load and the evidence in one submission, because they are one moment.
BODY = """
import weaver
from weaver.sessions.host import use_or_create_session

with use_or_create_session(None, workspace=workspace) as session:
    result = weaver.load(
        ["{item}"], dry_run=False, fault_tolerant=False, session=session
    )

rows = spark.sql(
    "SELECT Source, Detail FROM {qualified} ORDER BY Source"
).collect()

emit({{
    "status": result.to_mapping()["status"],
    "evidence": {{row.Source: row.Detail for row in rows}},
}})
"""


@weaver_test(integration=True)
def test_a_program_reads_the_external_estate_through_its_shortcuts(
    external_shortcut_estate,
):
    """The user-facing contract, end to end, against known external rows."""

    env, item = external_shortcut_estate
    _install_estate(env)

    seen = env.run_python(
        BODY.format(
            item=item,
            qualified=env.destination.qualify(*LOADED.split(".")),
        )
    )

    assert seen["status"] == "succeeded"

    expected_customers = ",".join(
        sorted(name for _id, name in external_estate.TABLES["Customer"][1])
    )
    expected_products = ",".join(
        sorted(name for _id, name in external_estate.TABLES["Product"][1])
    )
    assert seen["evidence"]["table"] == expected_customers
    assert seen["evidence"]["schema_attribute"] == expected_customers
    assert seen["evidence"]["schema_named"] == expected_products
    assert seen["evidence"]["folder"] == external_estate.FILE_BYTES.decode().strip()
