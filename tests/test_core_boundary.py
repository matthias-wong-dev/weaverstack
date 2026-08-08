"""The core must not depend on the optional CLI, on PySpark or on Fabric.

`weaver_cli` is an optional extra, so a core module that imported it would
break every Fabric Environment install that did not ask for the CLI.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CORE = Path(__file__).resolve().parents[1] / "src" / "weaver"

FORBIDDEN_IN_CORE = ("weaver_cli", "pyspark", "delta")


def _core_modules() -> list[Path]:
    return sorted(CORE.rglob("*.py"))


def test_core_has_modules_to_check():
    assert _core_modules()


def test_core_source_never_names_the_cli_or_spark():
    offenders = []
    for module in _core_modules():
        source = module.read_text(encoding="utf-8")
        for name in FORBIDDEN_IN_CORE:
            if f"import {name}" in source or f"from {name}" in source:
                offenders.append(f"{module.name}: {name}")
    assert not offenders, f"core imports it must not have: {offenders}"


def test_importing_the_core_does_not_load_the_cli_or_spark():
    probe = (
        "import sys, weaver;"
        "loaded = [m for m in ('weaver_cli', 'pyspark') if m in sys.modules];"
        "print(','.join(loaded))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == ""


def test_the_cli_depends_on_the_core():
    import importlib

    cli_main = importlib.import_module("weaver_cli.main")

    assert cli_main.weaver.__version__


# --- the CLI is usable without Spark -----------------------------------------
#
# `pip install 'weaverstack[cli]'` is the one command a friend tester runs, and
# it installs no PySpark. The core is held to this above; the CLI is the surface
# that person actually touches, so it is held to it too.

CLI = Path(__file__).resolve().parents[1] / "src" / "weaver_cli"


def test_the_cli_source_never_names_spark():
    offenders = []
    for module in sorted(CLI.rglob("*.py")):
        source = module.read_text(encoding="utf-8")
        for name in ("pyspark", "delta"):
            if f"import {name}" in source or f"from {name}" in source:
                offenders.append(f"{module.name}: {name}")
    assert not offenders, f"the CLI imports Spark, which [cli] does not install: {offenders}"


def test_the_cli_builds_and_runs_with_spark_unimportable():
    """The CLI parses and dispatches on a machine where PySpark cannot import.

    Blocking the import is stronger than merely not installing it: the suite
    runs where PySpark *is* present, so absence has to be simulated. (It does
    not make `doctor` report Spark missing — that reads package metadata by
    design, and the package is still installed here. What it proves is that no
    command path reaches for the module.)
    """

    probe = (
        "import sys;"
        "sys.modules['pyspark'] = None;"  # any `import pyspark` now raises
        "sys.modules['delta'] = None;"
        "from weaver_cli.main import build_parser, main;"
        # Every subcommand's parser is constructed, not just the one dispatched.
        "build_parser();"
        "main(['doctor', '--json']);"
        "print('dispatched')"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("dispatched")
    assert '"pyspark"' in result.stdout


def test_the_two_role_vocabularies_are_one():
    """`weaver.etl` repeats the roles rather than importing them.

    It has to: the catalogue package imports `etl`, so importing back would be a
    real cycle. Repetition is the price, and this is what keeps the two copies
    from drifting — a role added to one and not the other would put a value in
    the Registry that reconciliation refuses.
    """

    from weaver import etl
    from weaver.catalogue import tables

    assert etl.ROLE_LOAD == tables.ROLE_LOAD
    assert etl.ROLE_TEST == tables.ROLE_TEST
    assert etl.ROLE_ASSUMPTION == tables.ROLE_ASSUMPTION
    assert set(etl.VALIDATION_ROLE.values()) == set(tables.VALIDATION_ROLES)
