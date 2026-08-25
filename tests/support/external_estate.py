"""What the external pytest workspace holds, described once.

``PYTEST_WORKSPACE_EXT`` is not a Weaver target workspace. It holds physical
resources a shortcut may point at, so tests can assert that a destructive
operation reached nothing there.

Two schemas, and the difference is load-bearing:

``Reference``
    Never mutated. Tests assert on these exact rows and bytes.

``Source``
    The acceptance journey mutates these and restores the baseline afterwards,
    so a load can be shown to move data and to leave the rest alone.

Both the provisioner and the fixtures read this module, so the estate a test
expects and the estate provisioning writes cannot drift apart.
"""

from __future__ import annotations

from decimal import Decimal

#: The workspace, and the variable that renames it for another tenant.
WORKSPACE_ENV = "WEAVER_FABRIC_WORKSPACE_EXT"
DEFAULT_WORKSPACE = "PYTEST_WORKSPACE_EXT"

#: The Lakehouse it holds, by role.
ROLES = {"external": "PYTEST_EXT_LH"}

#: The Warehouse it holds, by role. A Fabric Warehouse publishes each table as a
#: Delta directory under ``Tables/<schema>/<table>``, so a Lakehouse shortcut can
#: read one.
WAREHOUSE_ROLES = {"external_warehouse": "PYTEST_EXT_WH"}

#: The schema holding what nothing mutates.
SCHEMA = "Reference"

#: The schema holding what the acceptance journey mutates and restores.
MUTABLE_SCHEMA = "Source"

#: Each stable external Delta table, as the Spark schema and the rows it holds.
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

#: A fixed instant the mutable baseline is stamped with. A literal, so a reseed
#: writes the same rows and an incremental load's first window is predictable.
BASELINE_INSTANT = "2026-01-01 00:00:00"

#: Each mutable external Delta table, as the schema its literal rows are read
#: with, the projection that gives the stored shape, and its baseline rows. The
#: instant is read as a string and cast, so a reseed writes an identical value.
MUTABLE_TABLES = {
    "Customer": (
        "CustomerId int, CustomerName string, UpdatedAt string",
        ("CustomerId", "CustomerName", "cast(UpdatedAt as timestamp) as UpdatedAt"),
        [
            (1, "Alice", BASELINE_INSTANT),
            (2, "Bob", BASELINE_INSTANT),
            (3, "Charlie", BASELINE_INSTANT),
        ],
    ),
}

#: The sentinel file a folder shortcut reads, and its exact bytes.
FILE = "keep.txt"
FILE_BYTES = b"external sentinel\n"

#: The mutable file drop, under ``Files/Source/Events``.
EVENTS_FOLDER = "Events"

#: The baseline event files, by name. Deterministic content, so a downstream
#: count is a fact rather than a sample.
EVENT_FILES = {
    "event-001.json": b'{"EventId": 1, "CustomerId": 1, "Kind": "created"}\n',
    "event-002.json": b'{"EventId": 2, "CustomerId": 2, "Kind": "created"}\n',
}

#: The external Warehouse's tables, as T-SQL column definitions and baseline
#: rows. Keyed by schema then table, so the stable and mutable halves are
#: distinguishable without parsing a name.
WAREHOUSE_TABLES = {
    SCHEMA: {
        "Region": (
            "RegionId int not null, RegionName varchar(50) not null",
            [(1, "North"), (2, "South")],
        ),
    },
    MUTABLE_SCHEMA: {
        "Transaction": (
            "TransactionId int not null, CustomerId int not null, "
            "Amount decimal(18, 2) not null",
            [
                (10, 1, Decimal("100.00")),
                (20, 2, Decimal("200.00")),
                (30, 3, Decimal("300.00")),
            ],
        ),
    },
}


def table_path(schema_relative: str) -> str:
    """``Tables/Reference/<table>``, as the estate spells it."""

    return f"Tables/{SCHEMA}/{schema_relative}"


def mutable_table_path(schema_relative: str) -> str:
    """``Tables/Source/<table>``, as the estate spells it."""

    return f"Tables/{MUTABLE_SCHEMA}/{schema_relative}"


def file_path(name: str = FILE) -> str:
    """``Files/Reference/<name>``, as the estate spells it."""

    return f"Files/{SCHEMA}/{name}"


def events_path(name: str | None = None) -> str:
    """``Files/Source/Events[/<name>]``, as the estate spells it."""

    root = f"Files/{MUTABLE_SCHEMA}/{EVENTS_FOLDER}"
    return f"{root}/{name}" if name else root
