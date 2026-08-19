"""What the external pytest workspace holds, described once.

``PYTEST_WORKSPACE_EXT`` is not a Weaver target workspace. It holds physical
resources a shortcut may point at and that no operation may mutate, so the tests
assert on these exact rows and bytes after a destructive operation to show
nothing propagated to the source.

Both the provisioner and the fixtures read this module, so the estate a test
expects and the estate provisioning writes cannot drift apart.
"""

from __future__ import annotations

#: The workspace, and the variable that renames it for another tenant.
WORKSPACE_ENV = "WEAVER_FABRIC_WORKSPACE_EXT"
DEFAULT_WORKSPACE = "PYTEST_WORKSPACE_EXT"

#: The one Lakehouse it holds, by role.
ROLES = {"external": "PYTEST_EXT_LH"}

#: The schema the external tables live in.
SCHEMA = "Reference"

#: Each external table, as the Spark schema and the rows it holds.
TABLES = {
    "Customer": (
        "CustomerId int, CustomerName string",
        [(1, "Northwind"), (2, "Contoso")],
    ),
    "Product": (
        "ProductId int, ProductName string",
        [(10, "Widget"), (20, "Gadget")],
    ),
}

#: The sentinel file a folder shortcut reads, and its exact bytes.
FILE = "keep.txt"
FILE_BYTES = b"external sentinel\n"


def table_path(schema_relative: str) -> str:
    """``Tables/Reference/<table>``, as the estate spells it."""

    return f"Tables/{SCHEMA}/{schema_relative}"


def file_path(name: str = FILE) -> str:
    """``Files/Reference/<name>``, as the estate spells it."""

    return f"Files/{SCHEMA}/{name}"
