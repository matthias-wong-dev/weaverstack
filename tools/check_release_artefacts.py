#!/usr/bin/env python3
"""Check that the built wheel and sdist carry the expected version.

Defaults to the version declared in `VERSION`. See design/releasing.md.
"""

from __future__ import annotations

import argparse
import email
import sys
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DISTRIBUTION = "weaverstack"


class ArtefactError(Exception):
    """An artefact that does not carry the version it claims to."""


def wheel_version(path: Path) -> str:
    """The version in a wheel's own METADATA."""

    with zipfile.ZipFile(path) as archive:
        names = [
            name
            for name in archive.namelist()
            if name.endswith(".dist-info/METADATA") and name.count("/") == 1
        ]
        if len(names) != 1:
            raise ArtefactError(f"{path.name}: expected one METADATA, found {names}")
        metadata = email.message_from_bytes(archive.read(names[0]))
    return _version_of(metadata, path)


def sdist_version(path: Path) -> str:
    """The version in an sdist's own PKG-INFO."""

    with tarfile.open(path) as archive:
        names = [
            name
            for name in archive.getnames()
            if name.endswith("/PKG-INFO") and name.count("/") == 1
        ]
        if len(names) != 1:
            raise ArtefactError(f"{path.name}: expected one PKG-INFO, found {names}")
        extracted = archive.extractfile(names[0])
        if extracted is None:  # pragma: no cover - a directory named PKG-INFO
            raise ArtefactError(f"{path.name}: {names[0]} is not a file")
        metadata = email.message_from_bytes(extracted.read())
    return _version_of(metadata, path)


def _version_of(metadata: email.message.Message, path: Path) -> str:
    name = metadata.get("Name")
    version = metadata.get("Version")
    if name != DISTRIBUTION:
        raise ArtefactError(f"{path.name}: Name is {name!r}, not {DISTRIBUTION!r}")
    if not version:
        raise ArtefactError(f"{path.name}: no Version in its metadata")
    return version


def check(directory: Path, expected: str) -> list[str]:
    """Every artefact in ``directory``, reported as it was found."""

    wheels = sorted(directory.glob("*.whl"))
    sdists = sorted(directory.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise ArtefactError(
            f"expected one wheel and one sdist in {directory}, found "
            f"{len(wheels)} and {len(sdists)}"
        )

    found = []
    for path, version in (
        (wheels[0], wheel_version(wheels[0])),
        (sdists[0], sdist_version(sdists[0])),
    ):
        found.append(f"  {path.name}  metadata {version}")
        if version != expected:
            raise ArtefactError(
                f"{path.name} carries version {version}, not {expected}"
            )
        # An index and a Fabric Environment read the filename, not the metadata.
        if f"-{expected}" not in path.name.replace(f"{DISTRIBUTION}-", "-", 1):
            raise ArtefactError(f"{path.name} is not named for version {expected}")
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version",
        help="the version to require. Defaults to the repository's VERSION.",
    )
    parser.add_argument(
        "--dist",
        type=Path,
        default=ROOT / "dist",
        help="where the built artefacts are. Defaults to ./dist.",
    )
    args = parser.parse_args()

    expected = args.version
    if expected is None:
        expected = (ROOT / "VERSION").read_text(encoding="utf-8").strip()

    try:
        found = check(args.dist, expected)
    except ArtefactError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Every artefact carries version {expected}:")
    print("\n".join(found))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
