"""Verified runtime and bundle carriers for Session-owned installation."""

from __future__ import annotations

import hashlib
import io
import json
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..mutation.targets import BoundTarget

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


@dataclass(frozen=True)
class ArchiveStaging:
    target: BoundTarget
    path: str

    @property
    def target_id(self):
        return self.target.id

    def to_mapping(self):
        return {"target": self.target.to_mapping(), "path": self.path}

    @classmethod
    def from_mapping(cls, mapping):
        from ..errors import BuildError
        from ..mutation.targets import BoundTarget

        if not isinstance(mapping, dict) or set(mapping) != {"target", "path"}:
            raise BuildError("invalid authorised archive staging")
        return cls(BoundTarget.from_mapping(mapping["target"]), mapping["path"])


def select_staging(plan, candidates):
    """Candidates authorise writable backend locations outside mutation and source scopes."""
    from ..mutation.scopes import ScopeRules
    from ..mutation.validation import validate_mutation_plan

    validate_mutation_plan(plan)
    rules = ScopeRules(plan)
    forbidden = list(plan.protected_scopes)
    from ..mutation.models import PhysicalScope

    for _, _, action in plan.actions():
        forbidden.extend((*action.writes, *action.destructive_scopes))
        if not action.writes and action.executor != "completion_gate":
            forbidden.append(PhysicalScope(action.target_id, ""))
    for candidate in candidates:
        if isinstance(candidate, ArchiveStaging):
            rules = ScopeRules(plan, extra_targets=(candidate.target,))
            checked = PhysicalScope(candidate.target_id, candidate.path)
        else:
            rules = ScopeRules(plan)
            checked = candidate
        rules.check(checked)
        if rules.targets[candidate.target_id].kind != "lakehouse" or not candidate.path:
            continue
        if not any(rules.overlaps(checked, scope) for scope in forbidden):
            return candidate
    return None


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
            _archive_reason = "archive installation requires deltalake {WRITER_VERSION}"
    except (ImportError, metadata.PackageNotFoundError) as _missing:
        _archive_reason = "archive dependency unavailable: " + str(_missing)
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


def execute_mutation_remote(
    session,
    plan,
    payloads=None,
    *,
    staging=None,
    workspace=None,
    timeout=600,
    build_datetime=None,
):
    """Submit one complete plan and retain uncertain invocations for diagnosis."""
    from datetime import datetime, timezone
    from urllib.parse import urlsplit
    from uuid import uuid4

    from ..build_bundle.execution import execution_spark_home, execution_workspace
    from ..errors import BuildError
    from ..mutation.executor import MutationReport, MutationResult, validate_inputs
    from ..mutation.targets import BoundTarget
    from ..targets import ItemRef
    from .mutation_report import decode_report

    payloads = validate_inputs(plan, payloads)
    actions = [a for _, _, a in plan.actions()]
    invocation_id = uuid4().hex
    if not actions:
        return MutationReport(plan.bundle_id, (), invocation_id=invocation_id)
    frozen_workspace = execution_workspace(plan.execution, plan)
    if workspace is not None and workspace != frozen_workspace:
        raise BuildError("mutation workspace differs from sealed plan")
    scope = session.scope(frozen_workspace)
    if staging is None:
        bound = {
            token.casefold()
            for target in plan.targets
            if target.kind == "lakehouse"
            for token in (target.item_id, target.item_name)
            if token
        }
        staging = tuple(
            ArchiveStaging(
                BoundTarget(
                    "archive-staging:" + item.id,
                    "lakehouse",
                    item.id,
                    workspace_name=plan.execution.workspace_name,
                    workspace_id=plan.execution.workspace_id,
                    item_name=item.name,
                ),
                "Files/_weaver_carriers",
            )
            for item in scope.resolver.discover()
            if item.type == "Lakehouse"
            and item.id.casefold() not in bound
            and item.name.casefold() not in bound
        )
    stage_scope = select_staging(plan, staging)
    if stage_scope is None:
        raise BuildError("no safe staging outside mutation and protected source scopes")
    targets = {t.id: t for t in plan.targets}
    staging_target = (
        stage_scope.target
        if isinstance(stage_scope, ArchiveStaging)
        else targets[stage_scope.target_id]
    )
    stage_scope = ArchiveStaging(staging_target, stage_scope.path)
    build_datetime = build_datetime or datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    request = {
        "plan_id": plan.bundle_id,
        "invocation_id": invocation_id,
        "staging": stage_scope.to_mapping(),
        "build_datetime": build_datetime,
        "timeout": timeout,
    }
    carrier = pack_mutation(plan, payloads, request=request)
    if carrier is None:
        raise BuildError("mutation carrier exceeds expanded size bound")
    home = execution_spark_home(plan.execution, plan)
    if home is not None:
        session.require_spark_home(home, workspace=frozen_workspace)
    store = scope.transport_store
    root = scope.resolver.lakehouse(
        ItemRef(staging_target.item_name or staging_target.item_id)
    )
    stage = root.join(*stage_scope.path.split("/"), invocation_id)
    incoming, output = stage / "carrier.zip", stage / "result.json"

    def native(location):
        value = location.value
        if value.startswith("https://"):
            address = urlsplit(value)
            parts = address.path.strip("/").split("/", 1)
            if (
                address.hostname != "onelake.dfs.fabric.microsoft.com"
                or len(parts) != 2
                or address.query
                or address.fragment
            ):
                raise ValueError(
                    "mutation carrier requires a bound OneLake destination"
                )
            return "abfss://" + parts[0] + "@" + address.hostname + "/" + parts[1]
        return value

    source = bootstrap_source(
        carrier, native(incoming), native(output), workers=session.direct_delta_workers
    )
    store.make_directory(stage)
    try:
        store.write(incoming, carrier.data)
    except BaseException:
        store.delete(stage, recursive=True)
        raise
    record = {
        "plan_id": plan.bundle_id,
        "invocation_id": invocation_id,
        "request": request,
        "archive_sha256": carrier.sha256,
        "carrier": incoming.value,
        "remote_result": output.value,
        "status": "uncertain",
    }
    if not hasattr(session, "archive_mutations"):
        session.archive_mutations = []
    session.archive_mutations.append(record)
    try:
        receipt = scope.livy_run(
            source,
            name="mutation_archive",
            timeout=timeout * len(actions),
            retry_submission=False,
        )
        result = read_receipt(receipt, store.read(output))
        if result.get("archive_sha256") != carrier.sha256:
            raise BuildError("mutation carrier result differs")
        if result.get("status") == "declined" and result.get("mutated") is False:
            raise BuildError(result["reason"])
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
        if record["status"] == "completed":
            try:
                store.delete(stage, recursive=True)
            except Exception as error:
                record["cleanup_error"] = str(error)
