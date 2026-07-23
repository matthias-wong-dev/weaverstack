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
