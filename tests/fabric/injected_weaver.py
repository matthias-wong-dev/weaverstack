"""Put the checkout's own wheel on a Livy session's ``sys.path``.

Publishing Weaver into a Fabric Environment takes about six minutes, and a
hosted test needs it again whenever Weaver Python changes. This builds one wheel
from the checkout, stages it under ``PYTEST_STAGING``, and hands
:class:`~weaver.fabric.LivySession` a bootstrap that extracts it on the driver
and puts it first on ``sys.path``.

The Environment still carries the dependencies (`pyyaml`, `sqlparse`,
`mssql-python`). What it no longer has to carry is Weaver.

A built wheel is staged, and unpacked where it lands. Weaver reaches its bundled
templates and ``warehouse_type_mapping.yml`` through ``Path(__file__)``, so a
packaging change that dropped them from the wheel shows up here.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

#: Where the staged wheel lives, beneath the staging Lakehouse's Files.
WHEEL_AREA = "injected_weaver"

#: Where the driver keeps the extracted package. Named for the wheel's version,
#: so a rebuilt checkout extracts beside the previous one and a session that
#: already holds the current one skips the copy.
DRIVER_ROOT = "/tmp/weaver-injected"


def stage_wheel(store, resolver, staging_lakehouse) -> tuple[str, str]:
    """Build the checkout's wheel, upload it, and return its abfss URL and version.

    `build_wheel` derives the version from the source, so an unchanged checkout
    produces the same filename and the upload overwrites identical bytes. Wheels
    from earlier checkouts go, because the staging Lakehouse is permanent and a
    changed checkout leaves one behind on every run.
    """

    from weaver.fabric.environment import build_wheel
    from weaver.fabric.onelake import abfss_root
    from weaver.targets import ItemRef

    wheel = build_wheel()
    item = ItemRef(staging_lakehouse.name)
    area = resolver.files_root(item) / WHEEL_AREA
    store.write(area / wheel.name, wheel.read_bytes())
    _forget_earlier_wheels(store, area, keep=wheel.name)

    url = (
        abfss_root(staging_lakehouse.workspace_id, staging_lakehouse.id)
        + f"/Files/{WHEEL_AREA}/{wheel.name}"
    )
    return url, _version_of(wheel)


def _forget_earlier_wheels(store, area, *, keep: str) -> None:
    """Remove every staged wheel but the one this run uploaded."""

    for entry in store.list(area):
        if entry.name.endswith(".whl") and entry.name != keep:
            store.delete(entry.location)


def _version_of(wheel: Path) -> str:
    """The version segment of a wheel filename."""

    return wheel.name.split("-")[1]


def bootstrap_source(url: str, version: str) -> str:
    """Python that extracts the staged wheel and imports Weaver from it.

    Idempotent. A session already holding the wanted version does nothing, so
    this runs at session start, where every later statement sees it, and again on
    the product's own `ensure_weaver` without disturbing what is loaded.

    The `sys.modules` purge is what makes the checkout win over an Environment
    that still carries a Weaver wheel. It runs only when the loaded version is
    the wrong one, so objects already held by a test keep the class they came
    from.
    """

    target = f"{DRIVER_ROOT}/{_slug(version)}"
    return f"""
import importlib
import os
import sys
import zipfile

if getattr(sys.modules.get("weaver"), "__version__", None) != {version!r}:
    _target = {target!r}
    if not os.path.isdir(_target):
        from notebookutils import fs as _fs

        _local = _target + ".whl"
        os.makedirs(os.path.dirname(_local), exist_ok=True)
        _fs.cp({url!r}, "file://" + _local)
        with zipfile.ZipFile(_local) as _archive:
            _archive.extractall(_target)
    for _name in [n for n in sys.modules if n == "weaver" or n.startswith("weaver.")]:
        del sys.modules[_name]
    while _target in sys.path:
        sys.path.remove(_target)
    sys.path.insert(0, _target)
    importlib.invalidate_caches()

import weaver

if weaver.__version__ != {version!r}:
    raise ImportError(
        "the injected checkout did not win: this session imported weaverstack "
        + weaver.__version__
        + " from "
        + str(weaver.__file__)
    )
for _dependency in ("yaml", "sqlparse", "mssql_python"):
    importlib.import_module(_dependency)
"""


def _slug(version: str) -> str:
    """A directory-safe stand-in for a version string."""

    return hashlib.sha256(version.encode()).hexdigest()[:16]
