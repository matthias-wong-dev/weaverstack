"""Shared build-environment wiring for the transport-neutral build tests.

Both the environment fixtures (in ``conftest``) and the test bodies draw from
here, so a fixture path or the local/Fabric parametrisation is defined exactly
once. This is a plain helper module (imported like ``sql_support``), not a
second conftest.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

_FIXTURES = Path(__file__).parent.parent / "fixtures"


@dataclass(frozen=True)
class SesFixture:
    """One declaration a build env installs, and the items it binds.

    The items are part of the fixture because binding *is* the build input: an
    environment cannot derive them, and which items a fixture leaves unbound is
    sometimes the whole subject — the mixed estate binds only its Lakehouse item
    so that the Warehouse leaves must be omitted.
    """

    path: Path
    items: tuple[str, ...]

    @property
    def name(self) -> str:
        return self.path.name


#: The declaration fixtures a build env can install. One place, so no test
#: hard-codes a path and both transports draw the same source.
BUILD_FIXTURE = SesFixture(_FIXTURES / "build-lakehouse-item", ("Lakehouse/Raw",))
SQL_TABLE_FIXTURE = SesFixture(
    _FIXTURES / "sql-table-build-item", ("Lakehouse/Sales",)
)
#: Three documents and nothing else — two Python tables and a Python folder — so
#: an authored-object test builds the smallest thing that has one of each.
AUTHORED_OBJECTS_FIXTURE = SesFixture(
    _FIXTURES / "authored-objects-item", ("Lakehouse/Sales",)
)
MIXED_ESTATE_FIXTURE = SesFixture(
    _FIXTURES / "mixed-estate-item", ("Lakehouse/Sales",)
)
WAREHOUSE_ESTATE_FIXTURE = SesFixture(
    _FIXTURES / "warehouse-estate-item", ("Warehouse/Reporting",)
)

#: Run one transport-neutral build test against both local Spark and Fabric. The
#: body drives a ``BuildEnv``; only the marks and fixture differ per environment.
lakehouse_environments = pytest.mark.parametrize(
    "build_env",
    [
        pytest.param("local_build_env", id="local", marks=pytest.mark.spark),
        pytest.param("fabric_build_env", id="fabric", marks=pytest.mark.fabric),
    ],
    indirect=True,
)
