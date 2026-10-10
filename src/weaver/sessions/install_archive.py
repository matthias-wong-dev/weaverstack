"""Verified runtime and bundle carriers for Session-owned installation."""

from __future__ import annotations

import hashlib
import io
import json
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

MAX_EXPANDED_BYTES = 128 * 1024 * 1024


def extract_verified(archive_path: Path, destination: Path, manifest: dict[str, str]):
    if destination.exists():
        raise ValueError("private extraction destination already exists")
    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()
        names = [member.filename for member in members]
        if len(names) != len(set(names)) or set(names) != set(manifest):
            raise ValueError("archive inventory differs from manifest")
        if sum(member.file_size for member in members) > MAX_EXPANDED_BYTES:
            raise ValueError("archive expanded size exceeds the installation limit")
        inventory = set(names)
        for name in names:
            if any(str(parent) in inventory for parent in PurePosixPath(name).parents):
                raise ValueError("unsafe archive file parent collision")
        for member in members:
            name = member.filename
            path = PurePosixPath(name)
            mode = member.external_attr >> 16
            if (
                path.is_absolute()
                or "\\" in name
                or ":" in name
                or any(part in ("", ".", "..") for part in name.split("/"))
                or stat.S_ISLNK(mode)
                or (stat.S_IFMT(mode) and not stat.S_ISREG(mode))
            ):
                raise ValueError("unsafe archive member")
            if hashlib.sha256(archive.read(name)).hexdigest() != manifest[name]:
                raise ValueError("archive content differs from manifest")
        archive.extractall(destination)


def read_receipt(receipt, data: bytes):
    if not isinstance(receipt, dict):
        raise ValueError("invalid installation receipt")
    if (
        type(receipt.get("bytes")) is not int
        or receipt["bytes"] != len(data)
        or receipt.get("sha256") != hashlib.sha256(data).hexdigest()
    ):
        raise ValueError("installation result differs from receipt")
    from .mutation_report import loads

    result = loads(data)
    if not isinstance(result, dict):
        raise ValueError("invalid installation result")
    return result


@dataclass(frozen=True)
class Carrier:
    data: bytes
    manifest: dict[str, str]

    @property
    def sha256(self):
        return hashlib.sha256(self.data).hexdigest()


def pack_mutation(plan, payloads=None, *, request=None) -> Carrier | None:
    """Carry a plan, exact payload bytes and the caller's matching runtime."""
    from ..mutation.bundle import plan_to_yaml
    from ..mutation.executor import validate_inputs

    payloads = validate_inputs(plan, payloads)
    files = {
        "bundle/plan.yml": plan_to_yaml(plan).encode("utf-8"),
        "request.json": json.dumps(
            request or {"plan_id": plan.bundle_id}, sort_keys=True, allow_nan=False
        ).encode("utf-8"),
    }
    files.update({"bundle/" + path: data for path, data in payloads.items()})
    runtime = Path(__file__).resolve().parents[1]
    size = sum(len(data) for data in files.values())
    if size > MAX_EXPANDED_BYTES:
        return None
    for source in sorted(runtime.rglob("*")):
        if (
            source.suffix not in (".py", ".sql", ".yaml", ".yml", ".json")
            or not source.is_file()
        ):
            continue
        if source.is_symlink() or not source.resolve().is_relative_to(runtime):
            raise ValueError("runtime contains an external source file")
        data = source.read_bytes()
        size += len(data)
        if size > MAX_EXPANDED_BYTES:
            return None
        files["runtime/weaver/" + source.relative_to(runtime).as_posix()] = data
    stream = io.BytesIO()
    manifest = {}
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(files.items()):
            archive.writestr(name, data)
            manifest[name] = hashlib.sha256(data).hexdigest()
    return Carrier(stream.getvalue(), manifest)


def bootstrap_source(carrier: Carrier, incoming: str, output: str, *, workers: int):
    import inspect

    from .delta_profile import WRITER_VERSION

    extraction = inspect.getsource(extract_verified)
    body = f"""
import hashlib
import importlib
import json
import shutil
import stat
import sys
import tempfile
import zipfile
from importlib import metadata
from pathlib import Path, PurePosixPath
from notebookutils import fs

_archive_incoming = {incoming!r}
_archive_output = {output!r}
_archive_sha256 = {carrier.sha256!r}
_archive_manifest = json.loads({json.dumps(carrier.manifest)!r})
_archive_previous_path = list(sys.path)
_archive_previous_modules = {{name: module for name, module in sys.modules.items() if name == "weaver" or name.startswith("weaver.")}}
_archive_private = Path(tempfile.mkdtemp(prefix="weaver-install-"))
_archive_switched = False
try:
    _archive_zip = _archive_private / "carrier.zip"
    fs.cp(_archive_incoming, "file:" + str(_archive_zip), False)
    if hashlib.sha256(_archive_zip.read_bytes()).hexdigest() != _archive_sha256:
        raise ValueError("installation carrier differs from receipt")
    MAX_EXPANDED_BYTES = {MAX_EXPANDED_BYTES!r}
    exec({extraction!r})
    _archive_root = _archive_private / "expanded"
    extract_verified(_archive_zip, _archive_root, _archive_manifest)
    _archive_reason = None
    try:
        for _dependency in ("pyarrow", "yaml", "requests", "azure.identity", "mssql_python"):
            importlib.import_module(_dependency)
        if metadata.version("deltalake") != {WRITER_VERSION!r}:
            _archive_reason = "its deltalake is not {WRITER_VERSION}"
    except ImportError as _missing:
        _archive_reason = "it is missing " + str(_missing.name or _missing)
    except metadata.PackageNotFoundError:
        _archive_reason = "it is missing deltalake"
    if _archive_reason is not None:
        _archive_result = {{"status": "declined", "mutated": False, "reason": _archive_reason}}
    else:
        for _name in _archive_previous_modules:
            del sys.modules[_name]
        sys.path.insert(0, str(_archive_root / "runtime"))
        importlib.invalidate_caches()
        _archive_switched = True
        _archive_module = importlib.import_module("weaver.sessions.archive_runtime")
        if Path(_archive_module.__file__).resolve() != (_archive_root / "runtime/weaver/sessions/archive_runtime.py").resolve():
            raise ValueError("archive runtime did not win import resolution")
        _archive_result = _archive_module.run_mutation(_archive_root, spark, _archive_output, _archive_sha256, workers={workers!r})
finally:
    if _archive_switched:
        for _name in list(sys.modules):
            if _name == "weaver" or _name.startswith("weaver."):
                del sys.modules[_name]
        sys.modules.update(_archive_previous_modules)
        sys.path[:] = _archive_previous_path
        importlib.invalidate_caches()
    try:
        shutil.rmtree(_archive_private)
    except Exception as _archive_cleanup_error:
        if "_archive_result" in locals():
            _archive_result["runtime_cleanup_failure"] = {{"path": str(_archive_private), "error": str(_archive_cleanup_error)}}
_archive_result["archive_sha256"] = _archive_sha256
_archive_bytes = json.dumps(_archive_result, separators=(",", ":"), allow_nan=False).encode("utf-8")
fs.put(_archive_output, _archive_bytes.decode("utf-8"), True)
emit({{"sha256": hashlib.sha256(_archive_bytes).hexdigest(), "bytes": len(_archive_bytes)}})
"""
    isolated = (
        "import builtins, threading\nwith builtins.__dict__.setdefault('_weaver_archive_namespace_lock', threading.RLock()):\n"
        + "\n".join("    " + line for line in body.splitlines())
    )
    return "exec(" + repr(isolated) + ', {"spark": spark, "emit": emit})\n'


def execute_mutation_in_fabric(
    session,
    plan,
    payloads=None,
    *,
    workspace=None,
    timeout=600,
    build_datetime=None,
    observer=None,
    concurrency=None,
):
    """Submit one complete plan once; an uncertain invocation is never replayed.

    The carrier is staged under the plan's Spark-home Lakehouse and removed after
    every outcome. It is transport, not a mutation target, so it is outside the
    plan's physical scopes. ``observer`` follows the progress Fabric writes beside
    the carrier while the plan runs.
    """
    from datetime import datetime, timezone
    from uuid import uuid4

    from ..build_bundle.execution import execution_workspace, spark_home_of
    from ..concurrency import validate_concurrency
    from ..errors import BuildError
    from ..fabric.onelake import abfss_path
    from ..mutation.executor import MutationReport, MutationResult, validate_inputs
    from ..targets import ItemRef
    from ..workspaces import CARRIER_AREA
    from .mutation_report import decode_report

    validate_concurrency(concurrency)
    payloads = validate_inputs(plan, payloads)
    actions = [a for _, _, a in plan.actions()]
    invocation_id = uuid4().hex
    if not actions:
        return MutationReport(plan.bundle_id, (), invocation_id=invocation_id)
    frozen_workspace = execution_workspace(plan.execution, plan)
    if workspace is not None and workspace != frozen_workspace:
        raise BuildError("mutation workspace differs from sealed plan")
    home_id = plan.execution.spark_home_target_id
    home = (
        spark_home_of(plan.targets)
        if home_id is None
        else next(t for t in plan.targets if t.id == home_id)
    )
    if home is None:
        raise BuildError("a mutation run in Fabric requires a Lakehouse target")
    build_datetime = build_datetime or datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    if home_id is not None:
        session.require_spark_home(home.name, workspace=frozen_workspace)
    scope = session.scope(frozen_workspace)
    store = scope.transport_store
    area = scope.resolver.files_root(ItemRef(home.name)) / CARRIER_AREA
    stage = area / invocation_id
    incoming, output = stage / "carrier.zip", stage / "result.json"
    progress = stage / "progress.json"

    def remove_stage():
        store.delete(stage, recursive=True)
        try:
            # Non-recursive, so another invocation's carrier keeps the area.
            store.delete(area)
        except Exception:
            pass

    from .archive_runtime import execution_capacity

    # Fabric runs the plan with this deployment's capacity, which the plan omits.
    workers, limits = execution_capacity(
        plan, session.workspace, concurrency=concurrency
    )
    request = {
        "plan_id": plan.bundle_id,
        "invocation_id": invocation_id,
        "build_datetime": build_datetime,
        "timeout": timeout,
        "workers": workers,
        "limits": limits,
    }
    if observer is not None:
        request["progress"] = abfss_path(progress)
    carrier = pack_mutation(plan, payloads, request=request)
    if carrier is None:
        raise BuildError("mutation carrier exceeds expanded size bound")

    source = bootstrap_source(
        carrier,
        abfss_path(incoming),
        abfss_path(output),
        workers=session.direct_delta_workers,
    )
    store.make_directory(stage)
    try:
        store.write(incoming, carrier.data)
    except BaseException:
        remove_stage()
        raise
    record = {
        "plan_id": plan.bundle_id,
        "invocation_id": invocation_id,
        "request": request,
        "archive_sha256": carrier.sha256,
        "carrier": incoming.value,
        "status": "uncertain",
    }
    if not hasattr(session, "archive_mutations"):
        session.archive_mutations = []
    session.archive_mutations.append(record)
    following = (
        None if observer is None else _follow(session, store, progress, observer)
    )
    try:
        try:
            receipt = scope.livy_run(
                source,
                name="mutation_archive",
                timeout=timeout * len(actions),
                retry_submission=False,
                livy=session.foreground_livy(scope),
            )
        finally:
            if following is not None:
                following()
        result = read_receipt(receipt, store.read(output))
        if result.get("archive_sha256") != carrier.sha256:
            raise BuildError("mutation carrier result differs")
        if result.get("status") == "declined" and result.get("mutated") is False:
            # Nothing ran, so the refusal is known rather than uncertain.
            reason = (
                f"Fabric's Spark session cannot run this build: {result['reason']}. "
                "Publish the project's Environment with weaver fabric environment "
                "publish, then build again."
            )
            record["status"] = "declined"
            record["error"] = reason
            return MutationReport(
                plan.bundle_id,
                tuple(MutationResult(a.id, "failed", error=reason) for a in actions),
                invocation_id=invocation_id,
            )
        if result.get("status") != "completed" or result.get("request") != request:
            raise BuildError("mutation result differs from request")
        report = decode_report(plan, result["report"], invocation_id=invocation_id)
        record["status"] = "completed"
        record["action_ids"] = [result.action_id for result in report.results]
        if "runtime_cleanup_failure" in result:
            record["runtime_cleanup_failure"] = result["runtime_cleanup_failure"]
        return report
    except Exception as error:
        record["error"] = str(error)
        return MutationReport(
            plan.bundle_id,
            tuple(MutationResult(a.id, "uncertain", error=str(error)) for a in actions),
            invocation_id=invocation_id,
        )
    finally:
        try:
            remove_stage()
        except Exception as error:
            record["cleanup_error"] = str(error)


def _follow(session, store, location, observer):
    """Read Fabric's progress beside the Livy wait; return what stops reading.

    A read that fails is retried at the next interval, and the final report
    supplies anything the progress never showed.
    """

    import threading

    from .archive_runtime import PROGRESS_INTERVAL

    done = threading.Event()
    context = session.telemetry.capture_context()

    def read():
        seen = 0
        with session.telemetry.use_context(context):
            while not done.wait(PROGRESS_INTERVAL):
                try:
                    records = json.loads(store.read(location))
                except Exception:  # noqa: BLE001 - progress never changes an outcome
                    continue
                for record in records[seen:]:
                    try:
                        observer(record)
                    except Exception:  # noqa: BLE001 - presentation only
                        pass
                seen = len(records)

    reader = threading.Thread(target=read, name="weaver-progress", daemon=True)
    reader.start()

    def stop():
        done.set()
        reader.join(PROGRESS_INTERVAL * 5)

    return stop
