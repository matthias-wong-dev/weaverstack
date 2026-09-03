"""The Fabric Environment definition, and where Weaver sits inside one.

An Environment item's definition is four kinds of part: ``.platform``,
``Setting/Sparkcompute.yml``, ``Libraries/PublicLibraries/environment.yml`` and
any number of ``Libraries/CustomLibraries/*`` files. A local ``*.Environment``
directory holds the same paths, and the REST API carries them as InlineBase64.

Two things are Weaver's and nothing else is: the ``weaverstack`` requirement in
the external library list, and ``weaverstack-*.whl`` among the custom libraries.
Released mode owns the first and removes the second. Development mode owns the
second, removes the first, and names Weaver's Fabric requirements so the wheel's
imports resolve, because a Fabric custom wheel does not pull its own transitive
dependencies.

The external library list is edited as text. A YAML round trip would drop the
comments and the layout the file was written with, and pip options such as
``--index-url`` sit in the same list as the requirements.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import yaml

from ..errors import CommandError

#: The definition part paths Fabric accepts, other than a custom library.
PLATFORM = ".platform"
EXTERNAL_LIBRARIES = "Libraries/PublicLibraries/environment.yml"
SPARK_COMPUTE = "Setting/Sparkcompute.yml"

#: Every custom library sits directly under this prefix.
CUSTOM_LIBRARIES = "Libraries/CustomLibraries/"

#: What a directory holding one Environment definition is called.
DIRECTORY_SUFFIX = ".Environment"

DISTRIBUTION = "weaverstack"

_SINGLE_PARTS = frozenset({PLATFORM, EXTERNAL_LIBRARIES, SPARK_COMPUTE})

#: An external library list with nothing but Weaver in it.
_MINIMAL_EXTERNAL = "dependencies:\n  - pip:\n"


def normalise_distribution(name: str) -> str:
    """One distribution name, folded the way PEP 503 compares two."""

    import re

    return re.sub(r"[-_.]+", "-", name).lower()


@dataclass(frozen=True)
class EnvironmentDefinition:
    """One Environment definition, as part path to decoded bytes."""

    parts: Mapping[str, bytes]

    def custom_libraries(self) -> tuple[str, ...]:
        """Every custom library filename, in path order."""

        return tuple(
            sorted(
                path[len(CUSTOM_LIBRARIES) :]
                for path in self.parts
                if path.startswith(CUSTOM_LIBRARIES)
            )
        )

    def external_libraries(self) -> str:
        """The external library list, as the text of an ``environment.yml``."""

        return self.parts.get(EXTERNAL_LIBRARIES, b"").decode("utf-8")


def environment_name_from_path(path: Path | str) -> str:
    """The Environment an ``*.Environment`` directory names.

    ``Something.Environment`` publishes ``Something``. The directory is the only
    place the name comes from, so a definition can be published into a workspace
    whose configuration names a different Environment.
    """

    directory = Path(path)
    name = directory.name
    if not name.endswith(DIRECTORY_SUFFIX) or len(name) == len(DIRECTORY_SUFFIX):
        raise CommandError(
            f"{directory}: an Environment definition directory is named "
            f"<Name>{DIRECTORY_SUFFIX}."
        )
    return name[: -len(DIRECTORY_SUFFIX)]


def read_environment_definition(path: Path | str) -> EnvironmentDefinition:
    """Read one local ``*.Environment`` directory as a definition."""

    directory = Path(path)
    if not directory.exists():
        raise CommandError(f"{directory}: no such Environment definition.")
    if not directory.is_dir():
        raise CommandError(
            f"{directory}: an Environment definition is a directory, not a file."
        )
    environment_name_from_path(directory)

    parts: dict[str, bytes] = {}
    for entry in sorted(directory.rglob("*")):
        if not entry.is_file():
            continue
        relative = entry.relative_to(directory).as_posix()
        if relative in _SINGLE_PARTS or relative.startswith(CUSTOM_LIBRARIES):
            parts[relative] = entry.read_bytes()
            continue
        raise CommandError(
            f"{directory}: {relative} is not an Environment definition part. "
            f"Supported parts are {PLATFORM}, {EXTERNAL_LIBRARIES}, "
            f"{SPARK_COMPUTE} and {CUSTOM_LIBRARIES}<file>."
        )

    definition = EnvironmentDefinition(parts=parts)
    if EXTERNAL_LIBRARIES in parts:
        # Read now, so a malformed list is reported against the file the user
        # wrote rather than as a publish failure minutes later.
        pip_entries(definition.external_libraries(), source=f"{directory}")
    return definition


def definition_payload(definition: EnvironmentDefinition) -> dict:
    """The definition as Fabric's create and update APIs take it."""

    return {
        "parts": [
            {
                "path": path,
                "payload": base64.b64encode(definition.parts[path]).decode("ascii"),
                "payloadType": "InlineBase64",
            }
            for path in sorted(definition.parts)
        ]
    }


def definition_from_payload(payload: Mapping) -> EnvironmentDefinition:
    """One definition read back from Fabric, decoded."""

    parts = {}
    for part in (payload.get("definition") or payload).get("parts") or ():
        parts[str(part["path"])] = base64.b64decode(part["payload"])
    return EnvironmentDefinition(parts=parts)


# --- the external library list -------------------------------------------------


def pip_entries(text: str, *, source: str) -> tuple[str, ...]:
    """The pip list an ``environment.yml`` declares, validated.

    Raises when ``dependencies`` is shaped in a way a requirement cannot be
    added to or removed from safely.
    """

    if not text.strip():
        return ()
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise CommandError(f"{source}: {EXTERNAL_LIBRARIES} is not valid YAML: {exc}")
    if document is None:
        return ()
    if not isinstance(document, dict):
        raise CommandError(f"{source}: {EXTERNAL_LIBRARIES} must be a YAML mapping.")
    declared = document.get("dependencies")
    if declared is None:
        return ()
    if not isinstance(declared, list):
        raise CommandError(
            f"{source}: {EXTERNAL_LIBRARIES} declares dependencies that are not a list."
        )
    for entry in declared:
        if isinstance(entry, dict):
            pip = entry.get("pip")
            if pip is None:
                continue
            if not isinstance(pip, list):
                raise CommandError(
                    f"{source}: {EXTERNAL_LIBRARIES} declares a pip section that is "
                    "not a list."
                )
            return tuple(str(item) for item in pip)
    return ()


def requirement_name(entry: str) -> str | None:
    """The distribution one pip entry names, or nothing for an option."""

    text = entry.strip()
    if not text or text.startswith("-"):
        return None
    from packaging.requirements import InvalidRequirement, Requirement

    try:
        return normalise_distribution(Requirement(text).name)
    except InvalidRequirement:
        return None


def pip_scalar(item: str) -> str | None:
    """The value one pip list item holds, read as YAML rather than as text.

    A list item is a YAML scalar, so ``"weaverstack>=0.4"`` carries quotes and
    ``weaverstack>=0.4  # keep this pin`` carries a comment. Both name a
    requirement, and reading the raw text as one finds neither.
    """

    try:
        value = yaml.safe_load(item)
    except yaml.YAMLError:
        return None
    return value if isinstance(value, str) else None


def weaver_requirement(entries: Iterable[str]) -> str | None:
    """The Weaver requirement a pip list already carries, as it is written."""

    for entry in entries:
        if requirement_name(entry) == DISTRIBUTION:
            return entry.strip()
    return None


def released_external_libraries(text: str, *, source: str) -> str:
    """The list with one effective PyPI ``weaverstack`` requirement.

    An existing Weaver requirement keeps the specifier it was written with, so
    ``weaverstack==0.4.0`` stays pinned and a bare name stays unpinned.
    """

    written = weaver_requirement(pip_entries(text, source=source))
    return _edit_pip(text, drop=(), ensure=(written or DISTRIBUTION,), source=source)


def development_external_libraries(
    text: str, *, requirements: Iterable[str], source: str
) -> str:
    """The list with Weaver's Fabric requirements and no ``weaverstack``.

    The checkout's wheel is the Weaver a development publication installs, so a
    PyPI requirement alongside it would resolve to a published version. Its
    imports still have to resolve, and a custom wheel brings no dependencies of
    its own, so Weaver's own requirements are named here.

    An entry the Environment already carries keeps its authored specifier only
    where that specifier can satisfy Weaver's. One that cannot is reported here,
    before anything is staged.
    """

    wanted = tuple(requirements)
    _check_authored_constraints(pip_entries(text, source=source), wanted, source=source)
    return _edit_pip(text, drop=(DISTRIBUTION,), ensure=wanted, source=source)


def _check_authored_constraints(
    entries: Iterable[str], requirements: Iterable[str], *, source: str
) -> None:
    """Refuse an authored requirement that excludes what Weaver needs.

    ``sqlparse==0.5.3`` beside a Weaver requirement of ``sqlparse>=0.6.0`` gives
    a published Environment Weaver cannot import from, and the failure would
    surface as an ImportError in the first notebook cell. It is a comparison of
    two specifiers, so it needs no resolver.
    """

    from packaging.requirements import InvalidRequirement, Requirement

    authored: dict[str, str] = {}
    for entry in entries:
        name = requirement_name(entry)
        if name is not None:
            authored.setdefault(name, entry.strip())

    conflicts: list[str] = []
    for text in requirements:
        try:
            required = Requirement(text)
        except InvalidRequirement:  # pragma: no cover - read from pyproject
            continue
        written = authored.get(normalise_distribution(required.name))
        if written is None:
            continue
        if not _satisfiable(Requirement(written), required):
            conflicts.append(f"  {written}    Weaver needs {text}")
    if conflicts:
        raise CommandError(
            f"{source}: {EXTERNAL_LIBRARIES} pins package versions Weaver cannot "
            "run against:\n"
            + "\n".join(conflicts)
            + "\nWiden or remove those requirements and publish again. No "
            "Environment changes were staged."
        )


def _satisfiable(authored, required) -> bool:
    """Whether some version can satisfy both specifiers.

    Conservative: it answers no only where the two exclude each other provably,
    from an exact pin or from bounds that do not overlap. Anything it cannot
    prove is left alone, because refusing a valid Environment is the worse
    failure. ``!=`` and prefix matches constrain nothing here.
    """

    if not authored.specifier:
        return True

    for pin in _pinned(authored.specifier):
        if not required.specifier.contains(pin, prereleases=True):
            return False

    low, low_closed = _lower(authored.specifier)
    other_low, other_low_closed = _lower(required.specifier)
    if other_low is not None and (low is None or other_low > low):
        low, low_closed = other_low, other_low_closed
    elif other_low is not None and other_low == low:
        low_closed = low_closed and other_low_closed

    high, high_closed = _upper(authored.specifier)
    other_high, other_high_closed = _upper(required.specifier)
    if other_high is not None and (high is None or other_high < high):
        high, high_closed = other_high, other_high_closed
    elif other_high is not None and other_high == high:
        high_closed = high_closed and other_high_closed

    if low is None or high is None:
        return True
    if low > high:
        return False
    return low != high or (low_closed and high_closed)


def _versions(specifier, operators):
    """Every parsable version the specifier names with one of these operators."""

    from packaging.version import InvalidVersion, Version

    found = []
    for clause in specifier:
        if clause.operator not in operators or "*" in clause.version:
            continue
        try:
            found.append(Version(clause.version))
        except InvalidVersion:
            continue
    return found


def _pinned(specifier):
    """The exact versions a specifier pins to."""

    return _versions(specifier, {"=="})


def _lower(specifier):
    """The highest lower bound a specifier states, and whether it includes it."""

    closed = _versions(specifier, {">=", "==", "~="})
    open_ = _versions(specifier, {">"})
    highest = max(closed + open_, default=None)
    if highest is None:
        return None, True
    # Where both a closed and an open bound name it, the open one is stricter.
    return highest, highest not in open_


def _upper(specifier):
    """The lowest upper bound a specifier states, and whether it includes it."""

    closed = _versions(specifier, {"<=", "=="})
    open_ = _versions(specifier, {"<"})
    lowest = min(closed + open_, default=None)
    if lowest is None:
        return None, True
    return lowest, lowest not in open_


def _edit_pip(
    text: str, *, drop: Iterable[str], ensure: Iterable[str], source: str
) -> str:
    """Rewrite one ``environment.yml`` pip list, keeping everything else.

    Line work rather than a YAML round trip: the file carries comments, ordering
    and pip options such as ``--index-url``, and none of them survive a load and
    a dump.
    """

    pip_entries(text, source=source)
    dropped = {normalise_distribution(name) for name in drop}
    wanted = {}
    for entry in ensure:
        name = requirement_name(entry)
        if name is not None:
            wanted[name] = entry.strip()

    lines = (text or _MINIMAL_EXTERNAL).splitlines()
    header = _pip_header(lines)
    if header is None:
        lines = _with_pip_section(lines)
        header = _pip_header(lines)
    assert header is not None
    index, indent = header

    end = index + 1
    kept: list[str] = []
    item_indent = f"{indent}    "
    settled: set[str] = set()
    while end < len(lines):
        line = lines[end]
        if line.strip() and not line.startswith(item_indent):
            break
        if line.strip():
            item_indent = line[: len(line) - len(line.lstrip())]
            # The item's YAML value, so a quoted or commented entry is read as
            # the requirement it names. The line itself is kept as authored.
            scalar = pip_scalar(line.strip().lstrip("-").strip())
            name = requirement_name(scalar) if scalar is not None else None
            if name in dropped:
                end += 1
                continue
            if name is not None and name in settled:
                # A second entry for a name Weaver manages. One requirement can
                # be effective, so the first spelling stands and this one goes.
                end += 1
                continue
            if name in wanted:
                # Already declared. The spelling in the file wins, so a pinned
                # requirement keeps its pin.
                wanted.pop(name)
                settled.add(name)
        kept.append(line)
        end += 1

    added = [f"{item_indent}- {wanted[name]}" for name in sorted(wanted)]
    rewritten = lines[: index + 1] + kept + added + lines[end:]
    return "\n".join(rewritten).rstrip("\n") + "\n"


def _pip_header(lines: list[str]) -> tuple[int, str] | None:
    """Where the ``- pip:`` list starts, and the indent its dash sits at."""

    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped in ("- pip:", "-pip:") or stripped == "- pip:":
            return index, line[: len(line) - len(line.lstrip())]
    return None


def _with_pip_section(lines: list[str]) -> list[str]:
    """The same list, with an empty ``- pip:`` under ``dependencies``."""

    for index, line in enumerate(lines):
        if line.strip() in ("dependencies:", "dependencies: "):
            return lines[: index + 1] + ["  - pip:"] + lines[index + 1 :]
    return [*lines, "dependencies:", "  - pip:"]


#: Requirements Weaver needs on a desktop and never inside Fabric. A published
#: Environment installs what runs there, and these are the transports and the
#: build tooling a console uses to reach it.
DESKTOP_ONLY = frozenset(
    {"azure-identity", "requests", "build", "prompt-toolkit", "packaging"}
)


def runtime_requirements(root: Path) -> tuple[str, ...]:
    """Weaver's Fabric requirements, read from ``pyproject.toml``.

    What a development publication has to name alongside the checkout's wheel:
    a Fabric custom wheel installs no dependencies of its own, so an unnamed
    requirement is an ``ImportError`` in the first notebook cell.
    """

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
