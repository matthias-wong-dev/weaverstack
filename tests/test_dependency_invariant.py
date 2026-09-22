"""The CI pin set against what package metadata requires.

CI installs ``requirements-dev.txt`` and then installs Weaver with
``--no-deps``, so a pin that does not satisfy ``pyproject.toml`` installs and
the suite runs green against a version the package says it cannot use. This
compares the two files directly, resolving nothing and reaching no network.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.version import Version
from support.weaver_test import weaver_test

ROOT = Path(__file__).resolve().parent.parent

PYPROJECT = ROOT / "pyproject.toml"
REQUIREMENTS = ROOT / "requirements-dev.txt"


def _declared() -> dict[str, Requirement]:
    """Every requirement pyproject declares, base and dev, by canonical name."""

    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]
    declared = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        declared.extend(extra)
    return {
        _canonical(requirement.name): requirement
        for requirement in map(Requirement, declared)
    }


def _pinned() -> dict[str, Version]:
    """Every ``name==version`` pin, without its markers or its ``via`` comments."""

    pins = {}
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line or "==" not in line:
            continue
        name, _, rest = line.partition("==")
        version = rest.split(";", 1)[0].strip()
        pins[_canonical(name.strip())] = Version(version)
    return pins


def _canonical(name: str) -> str:
    return name.lower().replace("_", "-").replace(".", "-")


@weaver_test()
def test_every_pin_satisfies_the_specifier_the_package_declares():
    declared = _declared()
    pinned = _pinned()

    unsatisfied = {
        name: (str(pinned[name]), str(requirement.specifier))
        for name, requirement in declared.items()
        if name in pinned and not requirement.specifier.contains(pinned[name])
    }

    assert unsatisfied == {}, (
        "requirements-dev.txt pins a version pyproject.toml refuses. Refresh "
        "the lock; the command is in that file's header."
    )


@weaver_test()
def test_every_declared_requirement_is_pinned():
    """A missing pin is an unpinned CI install, which is what the file prevents."""

    assert set(_declared()) <= set(_pinned())
