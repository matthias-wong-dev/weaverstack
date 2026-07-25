"""Shared build-environment wiring for the transport-neutral build tests.

Both the environment fixtures (in ``conftest``) and the test bodies draw from
here, so a fixture path or the local/Fabric parametrisation is defined exactly
once. This is a plain helper module (imported like ``sql_support``), not a
second conftest.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_FIXTURES = Path(__file__).parent.parent / "fixtures"

#: The SES repository fixtures a build env can install. One place, so no test
#: hard-codes a path and both transports draw the same source.
BUILD_FIXTURE = _FIXTURES / "build-lakehouse"
SQL_TABLE_FIXTURE = _FIXTURES / "sql-table-build"
MIXED_ESTATE_FIXTURE = _FIXTURES / "mixed-estate"
WAREHOUSE_ESTATE_FIXTURE = _FIXTURES / "warehouse-estate"

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
