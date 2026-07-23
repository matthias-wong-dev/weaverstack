"""Shipping Weaver into a workspace so a Fabric session can import it.

A development bridge. The end state is ``pip install weaverstack`` into a Fabric
Environment, at which point a session's ``import weaver`` simply works and none
of this runs. Until then the package is copied into the workspace and put on
``sys.path``.

It is worth being clear about what this is *for*. A Fabric test that runs Weaver
on a laptop and reaches into a workspace over HTTP proves the modules work. It
does not prove the product claim — that someone can open a notebook, install
Weaver, and use it. Only code executing inside Fabric proves that, and this is
how the code gets there before PyPI.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..errors import CommandError
from ..hosts import FabricHost
from ..locations import Location
from ..ses.source import content_hash
from ..targets import FILES_AREA, FolderTarget
from .onelake import FabricStore
from .resolution import RUNTIME_AREA, FabricResolver

#: Never shipped: caches, compiled files, and the optional CLI, which a session
#: has no use for.
IGNORED_DIRECTORIES = frozenset({"__pycache__", ".pytest_cache", ".mypy_cache"})
IGNORED_SUFFIXES = (".pyc", ".pyo")


@dataclass(frozen=True)
class SyncReport:
    """What a sync uploaded, and what it found already current."""

    location: Location
    uploaded: tuple[str, ...]
    unchanged: tuple[str, ...]

    @property
    def total(self) -> int:
        return len(self.uploaded) + len(self.unchanged)

    def __str__(self) -> str:
        return (
            f"{len(self.uploaded)} uploaded, {len(self.unchanged)} unchanged "
            f"-> {self.location}"
        )


def package_root() -> Path:
    """The installed ``weaver`` package directory on this machine."""

    import weaver

    return Path(weaver.__file__).parent


def install_location(host: FabricHost, resolver: FabricResolver) -> Location:
    """Where the package is shipped.

    ``weaver_install`` when the host names one, otherwise the convention
    ``<weaver-lakehouse>/Files/weaver``.
    """

    if host.weaver_install:
        return resolver.folder_root(FolderTarget.parse(host.weaver_install))
    return resolver.runtime_root


def _package_files(root: Path) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    for path in sorted(root.rglob("*.py")):
        parts = path.relative_to(root).parts
        if any(part in IGNORED_DIRECTORIES for part in parts):
            continue
        if path.name.endswith(IGNORED_SUFFIXES):
            continue
        files.append(("/".join(parts), path))
    return files


def sync_runtime(
    host: FabricHost,
    *,
    resolver: FabricResolver | None = None,
    store: FabricStore | None = None,
    force: bool = False,
) -> SyncReport:
    """Copy this machine's ``weaver`` package into the workspace.

    Compares content hashes rather than timestamps, and uploads only what
    differs, so a stale remote copy cannot quietly diverge from the code under
    test. Hashing normalises line endings, so a checkout is not a change.

    Rudimentary on purpose: it uploads the package and nothing else. Its
    dependencies — ``pyyaml`` and ``sqlparse`` — are already present in a Fabric
    Spark runtime.
    """

    resolver = resolver or FabricResolver(host)
    store = store or FabricStore()
    destination = install_location(host, resolver)

    root = package_root()
    files = _package_files(root)
    if not files:
        raise CommandError(f"no Python files found under {root}")

    remote_hashes: dict[str, str] = {}
    if not force and store.exists(destination):
        prefix = destination.value.rstrip("/") + "/"
        for entry in store.list(destination, recursive=True):
            if entry.is_directory:
                continue
            relative = entry.location.value[len(prefix):]
            remote_hashes[relative] = ""  # presence only; content compared below

    uploaded: list[str] = []
    unchanged: list[str] = []
    for relative, path in files:
        data = path.read_bytes()
        target = destination.join(*relative.split("/"))
        if not force and relative in remote_hashes:
            try:
                if content_hash(store.read(target)) == content_hash(data):
                    unchanged.append(relative)
                    continue
            except Exception:  # unreadable remote copy: replace it
                pass
        store.write(target, data)
        uploaded.append(relative)

    return SyncReport(
        location=destination,
        uploaded=tuple(uploaded),
        unchanged=tuple(unchanged),
    )


def bootstrap_source(package_parent: str) -> str:
    """The lines a Fabric session runs before it can ``import weaver``.

    ``package_parent`` is the directory *containing* the ``weaver`` package, not
    the package itself — ``import weaver`` searches ``sys.path`` for a directory
    of that name. Shipping to ``Files/weaver`` therefore means inserting
    ``Files``.

    The installed package is tried first, so the day Weaver comes from a Fabric
    Environment this becomes a no-op and the shipped copy goes unused.
    """

    return (
        "import sys\n"
        "try:\n"
        "    import weaver  # from a Fabric Environment, if it is there\n"
        "except ImportError:\n"
        f"    sys.path.insert(0, {package_parent!r})\n"
        "    import weaver\n"
    )


def mounted_package_parent(host: FabricHost) -> str:
    """The session-local path holding the ``weaver`` package directory.

    A Fabric session mounts its default Lakehouse at ``/lakehouse/default``, so
    a package shipped to ``<Lakehouse>/Files/weaver`` is importable once
    ``/lakehouse/default/Files`` is on ``sys.path`` — provided the session's
    default Lakehouse is the one it was shipped to.
    """

    install = host.weaver_install or f"{host.weaver_lakehouse}/{FILES_AREA}/{RUNTIME_AREA}"
    parts = install.strip("/").split("/")
    if len(parts) < 3:
        raise CommandError(
            f"weaver_install must name a Lakehouse and a path beneath it, got {install!r}"
        )
    # Drop the Lakehouse (the mount stands in for it) and the package directory.
    return "/lakehouse/default/" + "/".join(parts[1:-1])
