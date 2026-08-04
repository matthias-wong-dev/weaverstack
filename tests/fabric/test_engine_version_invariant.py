"""Local engine pins match the Spark and Delta versions Fabric is running."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

ROOT = Path(__file__).resolve().parents[2]


def _bounds(distribution: str) -> tuple[tuple[int, ...], tuple[int, ...]]:
    configuration = tomllib.loads((ROOT / "pyproject.toml").read_text())
    requirements = configuration["project"]["optional-dependencies"]["spark"]
    requirement = next(
        value for value in requirements if value.startswith(f"{distribution}>=")
    )
    match = re.fullmatch(
        rf"{re.escape(distribution)}>=(\d+(?:\.\d+)*),<(\d+(?:\.\d+)*)",
        requirement,
    )
    assert match is not None, f"unsupported engine requirement shape: {requirement}"
    return tuple(map(int, match[1].split("."))), tuple(map(int, match[2].split(".")))


def _version(value: str) -> tuple[int, ...]:
    match = re.match(r"\d+(?:\.\d+)*", value)
    assert match is not None, f"Fabric reported an unrecognisable version: {value!r}"
    return tuple(map(int, match[0].split(".")))


def test_fabric_engine_versions_fall_within_the_local_engine_ranges(livy_session):
    """The engine-fidelity argument is a checked fact, not a package comment."""

    payload = livy_session.run(
        "import importlib.metadata\n"
        "delta_version = None\n"
        "try:\n"
        "    delta_version = importlib.metadata.version('delta-spark')\n"
        "except importlib.metadata.PackageNotFoundError:\n"
        "    package = spark._jvm.java.lang.Class.forName(\n"
        "        'io.delta.tables.DeltaTable'\n"
        "    ).getPackage()\n"
        "    delta_version = package.getImplementationVersion()\n"
        "emit({'spark': spark.version, 'delta': delta_version})\n",
        label="engine version fidelity",
    ).payload

    assert payload["delta"], "Fabric exposed no Delta version to compare"
    for distribution, reported in (
        ("pyspark", payload["spark"]),
        ("delta-spark", payload["delta"]),
    ):
        lower, upper = _bounds(distribution)
        actual = _version(reported)
        assert lower <= actual < upper, (
            f"Fabric {distribution} {reported} falls outside the local engine "
            f"range declared in pyproject.toml: >= {lower}, < {upper}"
        )
