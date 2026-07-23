"""What a local Spark run needs, and whether this machine has it.

Local build and load need a JVM and a matched Spark/Delta pair. None of that is
required to *use* Weaver on Fabric, so it is optional — but when it is missing
the failure lands deep inside a Java stack trace, which is a poor way to learn
you needed a JDK. This reports the same facts up front.

Nothing here imports PySpark. Versions are read from package metadata, so the
check stays cheap and works when the pieces are absent.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

#: Delta and Spark move together — delta-spark 3.2.x expects Spark 3.5.x.
SUPPORTED_PYTHON = (3, 11)
SUPPORTED_PYSPARK = ("3.5",)
SUPPORTED_DELTA = ("3.2",)
#: Spark 3.5 runs on Java 8, 11 or 17. Later JDKs are not supported by it.
SUPPORTED_JAVA = ("17", "11")


@dataclass(frozen=True)
class Check:
    name: str
    found: str | None
    ok: bool
    hint: str = ""

    def __str__(self) -> str:
        mark = "ok  " if self.ok else "MISSING" if self.found is None else "WRONG"
        return f"{mark:8} {self.name:14} {self.found or '-'}"


@dataclass(frozen=True)
class LocalSparkReport:
    checks: tuple[Check, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    @property
    def hints(self) -> tuple[str, ...]:
        return tuple(check.hint for check in self.checks if not check.ok and check.hint)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "checks": [
                {"name": c.name, "found": c.found, "ok": c.ok, "hint": c.hint}
                for c in self.checks
            ],
        }


def find_java_home() -> str | None:
    """A JDK that Spark 3.5 can use, preferring the newest supported.

    ``JAVA_HOME`` wins when it is already set to something that exists, so a
    deliberately configured machine is never second-guessed.
    """

    existing = os.environ.get("JAVA_HOME")
    if existing and Path(existing).exists():
        return existing

    java_home_tool = Path("/usr/libexec/java_home")
    if java_home_tool.exists():
        for release in SUPPORTED_JAVA:
            try:
                found = subprocess.run(
                    [str(java_home_tool), "-v", release],
                    capture_output=True, text=True, check=True,
                ).stdout.strip()
            except (OSError, subprocess.CalledProcessError):
                continue
            if found:
                return found

    java = shutil.which("java")
    if java:
        return str(Path(java).resolve().parent.parent)
    return None


def java_version(java_home: str | None) -> str | None:
    if java_home is None:
        return None
    java = Path(java_home) / "bin" / "java"
    if not java.exists():
        return None
    try:
        result = subprocess.run(
            [str(java), "-version"], capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    # `java -version` writes to stderr: openjdk version "17.0.19" 2026-01-20
    first = (result.stderr or result.stdout).splitlines()[0] if (result.stderr or result.stdout) else ""
    for part in first.split('"'):
        if part and part[0].isdigit():
            return part
    return first or None


def _installed(package: str) -> str | None:
    try:
        return version(package)
    except PackageNotFoundError:
        return None


def check_local_spark() -> LocalSparkReport:
    """Everything a local Delta build and load needs, checked in one pass."""

    checks: list[Check] = []

    python = ".".join(str(part) for part in sys.version_info[:3])
    checks.append(
        Check(
            name="python",
            found=python,
            ok=sys.version_info[:2] >= SUPPORTED_PYTHON,
            hint=f"weaverstack needs Python {'.'.join(map(str, SUPPORTED_PYTHON))} or later",
        )
    )

    for package, supported in (("pyspark", SUPPORTED_PYSPARK), ("delta-spark", SUPPORTED_DELTA)):
        found = _installed(package)
        checks.append(
            Check(
                name=package,
                found=found,
                ok=found is not None and found.rsplit(".", 1)[0] in supported,
                hint=(
                    "install the optional extra:  pip install -e '.[spark]'"
                    if found is None
                    else f"{package} {'/'.join(supported)}.x is expected; "
                    "Spark and Delta are released in lockstep"
                ),
            )
        )

    home = find_java_home()
    found = java_version(home)
    major = (found or "").split(".")[0] if found else None
    checks.append(
        Check(
            name="java",
            found=f"{found} ({home})" if found else None,
            ok=major in SUPPORTED_JAVA,
            hint=(
                "install a JDK Spark 3.5 supports:  brew install openjdk@17"
                if found is None
                else f"Spark 3.5 runs on Java {', '.join(SUPPORTED_JAVA)}; "
                f"found {major}. Set JAVA_HOME to a supported JDK."
            ),
        )
    )

    return LocalSparkReport(checks=tuple(checks))


def platform_summary() -> str:
    return f"{platform.system()} {platform.machine()}"
