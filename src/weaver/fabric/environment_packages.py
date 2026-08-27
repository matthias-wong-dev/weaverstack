"""Resolve and compare the Python packages a Fabric Environment supplies.

The remote package list is the published Environment state. External libraries
carry Fabric's resolved closure. Missing and custom-wheel requirements use the
binary closure Weaver resolves.
"""

from __future__ import annotations

import email
import subprocess
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ..errors import CommandError

DESKTOP_ONLY = frozenset(
    {"azure-identity", "requests", "build", "prompt-toolkit", "packaging"}
)
FABRIC_PLATFORMS = (
    "manylinux_2_28_x86_64",
    "manylinux_2_17_x86_64",
    "manylinux2014_x86_64",
)


@dataclass(frozen=True)
class FabricRuntime:
    """The Python wheel target supplied by one published Fabric runtime."""

    version: str
    python_version: str
    abi: str


SUPPORTED_FABRIC_RUNTIMES = {
    "1.3": FabricRuntime("1.3", "3.11", "cp311"),
    "2.0": FabricRuntime("2.0", "3.13", "cp313"),
}


def fabric_runtime(version: str) -> FabricRuntime:
    """Return the explicit wheel target for a supported Fabric runtime."""

    normalized = str(version).strip()
    try:
        return SUPPORTED_FABRIC_RUNTIMES[normalized]
    except KeyError as exc:
        shown = normalized or "missing"
        supported = ", ".join(sorted(SUPPORTED_FABRIC_RUNTIMES))
        raise CommandError(
            f"Fabric Runtime {shown!r} is not supported for Environment "
            f"publication. Supported runtimes: {supported}. No Environment "
            "changes were staged."
        ) from exc


def normalise_distribution(name: str) -> str:
    """Return a normalized Python distribution name."""

    from packaging.utils import canonicalize_name

    return str(canonicalize_name(name))


def runtime_requirements(root: Path) -> tuple[str, ...]:
    """Return Weaver's Fabric requirements from ``pyproject.toml``."""

    import tomllib

    from packaging.requirements import Requirement

    payload = tomllib.loads((root / "pyproject.toml").read_text("utf-8"))
    declared = payload.get("project", {}).get("dependencies", ())
    selected: list[str] = []
    for text in declared:
        requirement = Requirement(text)
        if normalise_distribution(requirement.name) not in DESKTOP_ONLY:
            selected.append(str(requirement))
    return tuple(selected)


@dataclass(frozen=True)
class ResolvedWheel:
    """One concrete wheel in Weaver's complete Fabric dependency closure."""

    name: str
    version: str
    filename: str
    path: Path
    required: str
    dependencies: tuple[str, ...] = ()
    top_level: bool = True


@dataclass(frozen=True)
class ResolvedRequirement:
    """How one Weaver requirement will be supplied to Fabric."""

    name: str
    required: str
    resolved_version: str
    source: str

    def as_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "required": self.required,
            "resolved_version": self.resolved_version,
            "source": self.source,
        }


@dataclass(frozen=True)
class RemotePackage:
    """One versioned package reported by a Fabric Environment library API."""

    name: str
    version: str
    source: str
    filename: str | None = None


@dataclass(frozen=True)
class RequirementConflict:
    """One remote package version that fails Weaver's requirement."""

    name: str
    provided: tuple[str, ...]
    required: str
    location: str


class EnvironmentPackageConflict(CommandError):
    """Raised before mutation when an Environment package is incompatible."""

    def __init__(self, conflicts: Iterable[RequirementConflict]) -> None:
        self.conflicts = tuple(conflicts)
        super().__init__("Fabric Environment packages conflict with Weaver")


@dataclass(frozen=True)
class RequirementPlan:
    """The compatible package state and wheels still needing upload."""

    requirements: tuple[ResolvedRequirement, ...]
    upload: tuple[ResolvedWheel, ...]
    reused: tuple[str, ...]
    needs_publish: bool


def resolve_wheel_closure(
    requirements: Iterable[str],
    destination: Path,
    *,
    runtime: FabricRuntime,
) -> tuple[ResolvedWheel, ...]:
    """Download a Linux binary wheel closure for the published runtime."""

    requested = tuple(requirements)
    destination.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "pip",
        "download",
        "--disable-pip-version-check",
        "--dest",
        str(destination),
        "--only-binary=:all:",
        "--implementation",
        "cp",
        "--python-version",
        runtime.python_version,
        "--abi",
        runtime.abi,
    ]
    for platform in FABRIC_PLATFORMS:
        command.extend(("--platform", platform))
    command.extend(requested)
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        detail = (result.stderr.strip() or result.stdout.strip())[-1600:]
        raise CommandError(
            "Could not resolve the Fabric-compatible wheel closure required by "
            "Weaver. No Environment changes were staged.\n" + detail
        )
    return describe_wheel_closure(destination, requested, runtime=runtime)


def describe_wheel_closure(
    directory: Path,
    top_level: Iterable[str],
    *,
    runtime: FabricRuntime,
) -> tuple[ResolvedWheel, ...]:
    """Read versions and dependency constraints from downloaded wheels."""

    from packaging.requirements import Requirement
    from packaging.utils import parse_wheel_filename

    paths: dict[str, tuple[Path, str]] = {}
    requirements_by_name: dict[str, tuple[Requirement, ...]] = {}
    for path in sorted(directory.glob("*.whl")):
        distribution, version, _build, _tags = parse_wheel_filename(path.name)
        name = normalise_distribution(str(distribution))
        paths[name] = (path, str(version))
        requirements_by_name[name] = tuple(_wheel_requirements(path))

    constraints: dict[str, list[str]] = defaultdict(list)
    top_level_requirements = tuple(Requirement(text) for text in top_level)
    top_level_names = {
        normalise_distribution(requirement.name)
        for requirement in top_level_requirements
        if _requirement_applies(requirement, runtime)
    }
    for requirement in top_level_requirements:
        _add_constraint(constraints, requirement, runtime)
    for requirements in requirements_by_name.values():
        for requirement in requirements:
            _add_constraint(constraints, requirement, runtime)

    missing = sorted(set(constraints) - set(paths))
    if missing:
        raise CommandError(
            "The Fabric wheel resolver returned an incomplete closure for: "
            + ", ".join(missing)
        )

    resolved = []
    for name, (path, version) in sorted(paths.items()):
        required = _combined_constraint(constraints.get(name, ()))
        dependencies = tuple(
            sorted(
                {
                    normalise_distribution(requirement.name)
                    for requirement in requirements_by_name.get(name, ())
                    if _requirement_applies(requirement, runtime)
                }
            )
        )
        resolved.append(
            ResolvedWheel(
                name=name,
                version=version,
                filename=path.name,
                path=path,
                required=required,
                dependencies=dependencies,
                top_level=name in top_level_names,
            )
        )
    return tuple(resolved)


def _wheel_requirements(path: Path):
    from packaging.requirements import Requirement

    with zipfile.ZipFile(path) as archive:
        metadata_names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise CommandError(f"Wheel {path.name!r} has no single METADATA file")
        message = email.message_from_bytes(archive.read(metadata_names[0]))
    return [Requirement(value) for value in message.get_all("Requires-Dist", ())]


def _add_constraint(constraints, requirement, runtime: FabricRuntime) -> None:
    if requirement.url:
        raise CommandError(
            f"Fabric wheel resolution does not support direct URL requirement {requirement}"
        )
    if not _requirement_applies(requirement, runtime):
        return
    name = normalise_distribution(requirement.name)
    specifier = str(requirement.specifier)
    if specifier and specifier not in constraints[name]:
        constraints[name].append(specifier)
    else:
        constraints[name]


def _requirement_applies(requirement, runtime: FabricRuntime) -> bool:
    return not requirement.marker or requirement.marker.evaluate(
        _fabric_marker_env(runtime)
    )


def _fabric_marker_env(runtime: FabricRuntime) -> dict[str, str]:
    from packaging.markers import default_environment

    environment = default_environment()
    environment.update(
        {
            "implementation_name": "cpython",
            "platform_machine": "x86_64",
            "platform_system": "Linux",
            "python_full_version": f"{runtime.python_version}.0",
            "python_version": runtime.python_version,
            "sys_platform": "linux",
            "extra": "",
        }
    )
    return environment


def _combined_constraint(specifiers: Iterable[str]) -> str:
    parts = sorted({part for value in specifiers for part in value.split(",") if part})
    return ",".join(parts)


def inspect_libraries(libraries: dict) -> tuple[RemotePackage, ...]:
    """Read GA External and Custom library entries into versioned packages."""

    from packaging.utils import InvalidWheelFilename, parse_wheel_filename

    found: list[RemotePackage] = []
    for entry in libraries.get("libraries", ()) or ():
        kind = str(entry.get("libraryType") or "").casefold()
        name = str(entry.get("name") or "")
        if kind == "external" and name:
            version = entry.get("version")
            found.append(
                RemotePackage(
                    normalise_distribution(name),
                    "" if version is None else str(version),
                    "external",
                )
            )
        elif kind == "custom" and name.casefold().endswith(".whl"):
            try:
                distribution, version, _build, _tags = parse_wheel_filename(name)
            except InvalidWheelFilename:
                continue
            found.append(
                RemotePackage(
                    normalise_distribution(str(distribution)),
                    str(version),
                    "custom-wheel",
                    filename=name,
                )
            )
    return tuple(found)


def custom_library_names(libraries: dict) -> tuple[str, ...]:
    """Return every GA Custom library filename without interpreting ownership."""

    return tuple(
        str(entry.get("name"))
        for entry in libraries.get("libraries", ()) or ()
        if str(entry.get("libraryType") or "").casefold() == "custom"
        and entry.get("name")
    )


def plan_requirements(
    closure: Iterable[ResolvedWheel],
    *,
    published: dict,
    staging: dict,
) -> RequirementPlan:
    """Reuse compatible packages and select absent wheels for additive upload."""

    published_by_name = _packages_by_name(inspect_libraries(published))
    staging_by_name = _packages_by_name(inspect_libraries(staging))
    conflicts: list[RequirementConflict] = []
    results: list[ResolvedRequirement] = []
    upload: list[ResolvedWheel] = []
    reused: list[str] = []
    needs_publish = False

    closure_by_name = {wheel.name: wheel for wheel in closure}
    visited: set[str] = set()

    def custom_closure_is_known(
        packages: Iterable[RemotePackage], wheel: ResolvedWheel, location: str
    ) -> bool:
        unknown = tuple(
            sorted(
                {
                    package.version
                    for package in packages
                    if package.source == "custom-wheel"
                    and package.version != wheel.version
                }
            )
        )
        if not unknown:
            return True
        conflicts.append(
            RequirementConflict(
                wheel.name,
                unknown,
                f"=={wheel.version} (resolved wheel closure)",
                location,
            )
        )
        return False

    def include(name: str) -> None:
        nonlocal needs_publish
        if name in visited:
            return
        visited.add(name)
        wheel = closure_by_name[name]
        supplied = published_by_name.get(wheel.name, ())
        bad = _incompatible_versions(supplied, wheel.required)
        if bad:
            conflicts.append(
                RequirementConflict(
                    wheel.name,
                    tuple(sorted({package.version for package in supplied})),
                    wheel.required,
                    "published",
                )
            )
            return
        if not custom_closure_is_known(supplied, wheel, "published"):
            return
        pending = staging_by_name.get(wheel.name, ())
        bad = _incompatible_versions(pending, wheel.required)
        if bad:
            conflicts.append(
                RequirementConflict(
                    wheel.name,
                    tuple(sorted({package.version for package in pending})),
                    wheel.required,
                    "staging",
                )
            )
            return
        if not custom_closure_is_known(pending, wheel, "staging"):
            return
        if supplied:
            package = supplied[0]
            version = package.version or "unspecified"
            results.append(
                ResolvedRequirement(wheel.name, wheel.required, version, package.source)
            )
            reused.append(
                f"{wheel.name}=={package.version}"
                if package.version
                else f"{wheel.name} (unversioned)"
            )
            if package.source != "external":
                for dependency in wheel.dependencies:
                    include(dependency)
            return

        if pending:
            needs_publish = True
            package = pending[0]
            version = package.version or "unspecified"
            results.append(
                ResolvedRequirement(wheel.name, wheel.required, version, package.source)
            )
            reused.append(
                f"{wheel.name}=={package.version}"
                if package.version
                else f"{wheel.name} (unversioned)"
            )
            if package.source != "external":
                for dependency in wheel.dependencies:
                    include(dependency)
            return

        upload.append(wheel)
        needs_publish = True
        results.append(
            ResolvedRequirement(
                wheel.name, wheel.required, wheel.version, "weaver-injected"
            )
        )
        for dependency in wheel.dependencies:
            include(dependency)

    roots = sorted(wheel.name for wheel in closure_by_name.values() if wheel.top_level)
    for name in roots:
        include(name)

    if conflicts:
        raise EnvironmentPackageConflict(conflicts)
    return RequirementPlan(tuple(results), tuple(upload), tuple(reused), needs_publish)


def _packages_by_name(packages: Iterable[RemotePackage]):
    by_name: dict[str, list[RemotePackage]] = defaultdict(list)
    for package in packages:
        by_name[package.name].append(package)
    return by_name


def _incompatible_versions(
    packages: Iterable[RemotePackage], required: str
) -> tuple[str, ...]:
    from packaging.specifiers import SpecifierSet
    from packaging.version import InvalidVersion, Version

    supplied = tuple(packages)
    versions = {package.version for package in supplied}
    if len(versions) > 1:
        return tuple(sorted(versions))
    specifier = SpecifierSet(required)
    if not required:
        return ()
    incompatible = []
    for package in supplied:
        try:
            version = Version(package.version)
        except InvalidVersion:
            incompatible.append(package.version)
            continue
        if not specifier.contains(version, prereleases=True):
            incompatible.append(package.version)
    return tuple(incompatible)
