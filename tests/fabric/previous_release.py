"""Install an estate with a previous Weaver release, from that release's own venv.

Run by the upgrade journey as ``python -I previous_release.py <request.json>``
with the interpreter of a venv holding the released ``weaverstack``. It imports
only that release's public API, so nothing from this checkout reaches it.

Spark runs the same release: the released wheel is staged in the staging
Lakehouse and put first on the Livy session's ``sys.path``, which is how the
Fabric suite runs a checkout without publishing it. The Environment supplies
the dependencies.

The request names the workspace, the catalogue, the staging Lakehouse, the
Environment, the wheel, the repository and the items. The result, written to
the request's ``result`` path as JSON, holds each operation's status.
"""

from __future__ import annotations

import hashlib
import json
import sys
import traceback
from pathlib import Path

#: Where the staged wheel lives, beneath the staging Lakehouse's Files.
WHEEL_AREA = "previous_weaver"
DRIVER_ROOT = "/tmp/weaver-previous"


def bootstrap_source(url: str, version: str) -> str:
    """Python that extracts the staged wheel and imports Weaver from it."""

    target = f"{DRIVER_ROOT}/{hashlib.sha256(version.encode()).hexdigest()[:16]}"
    return f"""
import importlib
import os
import sys
import zipfile

_target = {target!r}


def _came_from_the_release(module):
    origin = getattr(module, "__file__", None)
    return bool(origin) and os.path.realpath(origin).startswith(
        os.path.realpath(_target) + os.sep
    )


if not _came_from_the_release(sys.modules.get("weaver")):
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

if not _came_from_the_release(weaver) or weaver.__version__ != {version!r}:
    raise ImportError(
        "the session imported weaverstack " + weaver.__version__ + " from "
        + str(weaver.__file__) + ", not the staged release " + {version!r}
    )
"""


def _stage(wheel: Path, workspace, staging: str) -> str:
    from weaver.fabric import FabricResolver, OneLakeDfsClient
    from weaver.fabric.onelake import abfss_root
    from weaver.fabric.resources import LAKEHOUSE
    from weaver.targets import ItemRef

    resolver = FabricResolver(workspace)
    area = resolver.files_root(ItemRef(staging)) / WHEEL_AREA
    OneLakeDfsClient().write(area / wheel.name, wheel.read_bytes())
    item = resolver.resolve(ItemRef(staging), item_type=LAKEHOUSE)
    return abfss_root(item.workspace_id, item.id) + f"/Files/{WHEEL_AREA}/{wheel.name}"


def _status(result) -> str:
    succeeded = getattr(result, "succeeded", None)
    if succeeded is not None:
        return "succeeded" if succeeded else str(getattr(result, "status", "failed"))
    return repr(result)


def main(path: str) -> int:
    request = json.loads(Path(path).read_text(encoding="utf-8"))
    outcome: dict = {"steps": []}
    try:
        import weaver
        from weaver.fabric import LivySession, emit_source
        from weaver.fabric.auth import desktop_credential, use_credential
        from weaver.sessions import ConsoleSession
        from weaver.targets import ItemRef, WarehouseTarget
        from weaver.workspaces import Workspace

        outcome["version"] = weaver.__version__
        use_credential(desktop_credential())
        workspace = Workspace(
            workspace=request["workspace"],
            catalogue=f"Warehouse/{request['catalogue']}",
            environment=request["environment"],
        )
        wheel = Path(request["wheel"])
        bootstrap = bootstrap_source(
            _stage(wheel, workspace, request["staging"]), weaver.__version__
        )
        livy = LivySession.for_workspace(
            workspace,
            lakehouse=ItemRef(request["staging"]),
            bootstrap=emit_source() + bootstrap,
            weaver_bootstrap=bootstrap,
        )
        with livy, ConsoleSession(workspace=workspace, livy=livy) as session:
            # The catalogue Warehouse is also a target, and both items create
            # `_`; the schema is put there first.
            session.sql_executor(
                WarehouseTarget(ItemRef(request["catalogue"])), workspace=workspace
            ).execute_script("if schema_id(N'_') is null exec('create schema [_]');")
            for operation, items in request["steps"]:
                if operation == "build":
                    result = weaver.build(
                        request["repository"], items=items, session=session
                    )
                elif operation == "load":
                    result = weaver.load(items, session=session)
                elif operation == "test":
                    result = weaver.test(items, session=session)
                else:
                    raise ValueError(f"unknown operation {operation!r}")
                step = {"operation": operation, "items": items}
                step["status"] = _status(result)
                if step["status"] != "succeeded":
                    step["detail"] = repr(
                        getattr(result, "errors", None)
                        or getattr(result, "nodes", None)
                    )[:4000]
                outcome["steps"].append(step)
    except BaseException as exc:  # noqa: BLE001 - reported to the journey
        outcome["error"] = f"{type(exc).__name__}: {exc}"
        outcome["traceback"] = traceback.format_exc()
    Path(request["result"]).write_text(json.dumps(outcome, indent=2), encoding="utf-8")
    return 1 if "error" in outcome else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
