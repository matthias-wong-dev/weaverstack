"""Weaver running *inside* Fabric.

The other Fabric tests run Weaver on this machine and reach into a workspace
over HTTP. These run Weaver there — which is the product claim, and the only
thing that shows a notebook user could do the same.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]


# --- what the installed Environment provides ----------------------------------
#
# One round trip, not four. These are four claims about *one static thing* — the
# Environment `weaver install` published — so asking them separately bought four
# waits and no extra confidence: the Environment cannot have changed between
# them. That is the distinction worth keeping in mind when adding to this file.
# The protocol tests below stay separate calls, because there the call itself is
# the subject.


@pytest.fixture(scope="module")
def installed_environment(livy_session):
    """Everything the session's Weaver can be asked about itself, in one payload."""

    result = livy_session.run(
        "import weaver\n"
        "from importlib.metadata import version\n"
        "import yaml, sqlparse, mssql_python\n"
        "from weaver import FolderTarget, DeltaTarget, Location\n"
        "from weaver.declaration import parse_document\n"
        "doc = parse_document('''\n"
        "Table ID: Sales.Order\n\n"
        "Description: One row per order.\n\n"
        "Lineage: The order export.\n\n"
        "Primary key: Order id\n\n"
        "Schema:\n"
        "  Order id: string\n"
        "''', language='python')\n"
        "emit({\n"
        "  'version': {'attr': weaver.__version__, 'dist': version('weaverstack')},\n"
        "  'dependencies': sorted(['yaml', 'sqlparse', 'mssql_python']),\n"
        "  'surface': {\n"
        "    'folder': str(FolderTarget.parse('Sales_LH/Files')),\n"
        "    'delta': str(DeltaTarget.parse('Sales_LH')),\n"
        "    'joined': (Location('abfss://ws@workspace/lh') / 'Files' / 'x').value,\n"
        "  },\n"
        "  'document': {\n"
        "    'id': doc.qualified,\n"
        "    'columns': [c.name for c in doc.effective_schema],\n"
        "  },\n"
        "})\n",
        label="observe environment",
    )
    # Kept from when this was four calls: a body that printed but emitted nothing
    # would otherwise fail the tests below as a `NoneType` subscript, which says
    # nothing about the session.
    assert result.returned, "the session emitted no payload"
    return result.payload


def test_weaver_imports_inside_a_fabric_session(installed_environment):
    """The claim: a Fabric session imports the installed Weaver and uses it.

    The version is whatever ``weaver install`` published into the Environment —
    not necessarily this checkout's — so we assert a real version came back, not
    that it equals the laptop's.
    """

    version = installed_environment["version"]

    assert version["attr"] == version["dist"]
    assert version["dist"]  # a real, non-empty version string


def test_the_environment_carries_weavers_dependencies(installed_environment):
    """mssql-python and the rest resolve inside the session, from the Environment.

    The import is what proves it — it happened in the body, and a missing
    distribution would have failed the submission before anything was emitted.
    """

    assert installed_environment["dependencies"] == [
        "mssql_python",
        "sqlparse",
        "yaml",
    ]


def test_the_core_public_surface_is_importable_there(installed_environment):
    assert installed_environment["surface"] == {
        # A folder target is the area, not a path within it: a folder object
        # lands at Files/<Schema>/<Object>, so there is nothing beneath Files
        # left to configure.
        "folder": "Sales_LH/Files",
        "delta": "Sales_LH",
        "joined": "abfss://ws@workspace/lh/Files/x",
    }


def test_the_weaver_contract_parses_there(installed_environment):
    """The heart of Weaver, running in Fabric rather than described to it."""

    document = installed_environment["document"]

    assert document["id"] == "Sales.Order"
    assert document["columns"] == [
        "Order id",
        "row_insert_datetime",
        "row_update_datetime",
        "row_delete_datetime",
    ]
