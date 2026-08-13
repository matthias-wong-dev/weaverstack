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
#: Spark 3.5 documents Java 8, 11 and 17; 21 is not documented but runs the
#: local Delta suite, so it is accepted rather than preferred. The order is the
#: discovery preference — `find_java_home` takes the first release it can find.
SUPPORTED_JAVA = ("17", "11", "21")


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
        """What to do about the failures, each said once.

        PySpark and Delta are installed by the same extra, so a machine missing
        both would otherwise be told to run the same command twice.
        """

        seen: dict[str, None] = {}
        for check in self.checks:
            if not check.ok and check.hint:
                seen.setdefault(check.hint)
        return tuple(seen)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "checks": [
                {"name": c.name, "found": c.found, "ok": c.ok, "hint": c.hint}
                for c in self.checks
            ],
        }


def find_java_home() -> str | None:
    """Return a JDK Spark 3.5 can use, preferring a configured ``JAVA_HOME``."""

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


def java_launcher(java_home: str | None) -> Path | None:
    """The ``java`` binary inside a JAVA_HOME, whatever the platform names it.

    Windows ships ``bin/java.exe``; everywhere else it is ``bin/java``. Both
    names are tried rather than branching on the platform, so the lookup cannot
    be wrong about the machine it is running on.
    """

    if java_home is None:
        return None
    bin_dir = Path(java_home) / "bin"
    for name in ("java", "java.exe"):
        candidate = bin_dir / name
        if candidate.is_file():
            return candidate
    return None


def parse_java_version(output: str) -> str | None:
    """The release from `java -version` output: openjdk version "17.0.19" ...

    The banner is not reliably the first line. A JVM started with
    JAVA_TOOL_OPTIONS or _JAVA_OPTIONS set — a proxy's truststore, a container's
    defaults — announces those first, so match the banner itself rather than
    trusting its position. Output that carries no banner is no version at all,
    not a line to be reported as one.

    Kept apart from the subprocess so the parsing can be tested on every
    platform, rather than only where a fake `java` script can be executed.
    """

    for line in output.splitlines():
        if 'version "' not in line:
            continue
        for part in line.split('"'):
            if part and part[0].isdigit():
                return part
    return None


def java_version(java_home: str | None) -> str | None:
    java = java_launcher(java_home)
    if java is None:
        return None
    try:
        # `java -version` writes to stderr.
        result = subprocess.run(
            [str(java), "-version"], capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return parse_java_version(result.stderr or result.stdout or "")


def _installed(package: str) -> str | None:
    try:
        return version(package)
    except PackageNotFoundError:
        return None


#: How to install a package, per platform. A hint naming the wrong package
#: manager is worse than no hint — it sends someone to a command they do not
#: have. Keyed by `sys.platform`, with a fallback for everything else.
INSTALL_COMMANDS = {
    "jdk": {
        "darwin": "brew install openjdk@17",
        "win32": "winget install Microsoft.OpenJDK.17",
        None: "sudo apt install openjdk-17-jdk   (or your distribution's JDK)",
    },
    "azure-cli": {
        "darwin": "brew install azure-cli",
        "win32": "winget install Microsoft.AzureCLI",
        None: "see https://learn.microsoft.com/cli/azure/install-azure-cli",
    },
}


def install_command(what: str) -> str:
    """The command that installs ``what`` on this platform."""

    choices = INSTALL_COMMANDS[what]
    return choices.get(sys.platform, choices[None])


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
                    # Not `-e '.[spark]'`: someone who installed from PyPI has
                    # no checkout for `.` to mean, and this is exactly the
                    # person who has not got Spark.
                    "install the optional extra:  pip install 'weaverstack[spark]'"
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
                f"install a JDK Spark 3.5 supports:  {install_command('jdk')}"
                if found is None
                else f"Spark 3.5 runs on Java {', '.join(SUPPORTED_JAVA)}; "
                f"found {major}. Set JAVA_HOME to a supported JDK."
            ),
        )
    )

    return LocalSparkReport(checks=tuple(checks))


def platform_summary() -> str:
    return f"{platform.system()} {platform.machine()}"
