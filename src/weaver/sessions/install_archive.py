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
    result = json.loads(data)
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


def pack_bundle(bundle, *, request=None) -> Carrier | None:
    """Carry frozen payloads and the installed caller's matching runtime."""
    import yaml

    from ..errors import InstallError

    runtime = Path(__file__).resolve().parents[1]
    manifest = bundle.store.read(bundle.location / "plan.yml")
    try:
        stored = json.dumps(yaml.safe_load(manifest), sort_keys=True, allow_nan=False)
        frozen = json.dumps(bundle.plan.to_mapping(), sort_keys=True, allow_nan=False)
    except (TypeError, ValueError, yaml.YAMLError) as error:
        raise InstallError(
            "Stored bundle differs from frozen installation plan"
        ) from error
    if stored != frozen:
        raise InstallError("Stored bundle differs from frozen installation plan")
    files = {"bundle/plan.yml": manifest}
    if request is not None:
        files["request.json"] = json.dumps(
            request, sort_keys=True, allow_nan=False
        ).encode("utf-8")
    size = len(files["bundle/plan.yml"])
    for sequence in bundle.plan.sequences:
        for batch in sequence.batches:
            for action in batch.actions:
                if action.payload is None:
                    continue
                name = "bundle/" + action.payload
                if name in files:
                    continue
                data = bundle.store.read(
                    bundle.location.join(*action.payload.split("/"))
                )
                size += len(data)
                if size > MAX_EXPANDED_BYTES:
                    return None
                files[name] = data
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


def decode_report(mapping, plan, *, partial=False):
    import math
    from datetime import datetime

    from ..build_bundle.report import InstallationReport

    planned = plan if isinstance(plan, dict) else plan.to_mapping()
    if mapping["bundle_id"] != planned["bundle_id"]:
        raise ValueError("installation report bundle differs")
    sequences = mapping["sequences"]
    expected = planned["sequences"]
    if partial:
        if mapping["status"] != "running" or len(sequences) > len(expected):
            raise ValueError("invalid partial installation report")
        expected = expected[: len(sequences)]
    elif mapping["status"] not in ("succeeded", "failed"):
        raise ValueError("invalid installation status")
    if [row["number"] for row in sequences] != [row["number"] for row in expected]:
        raise ValueError("installation sequence order differs")
    stopped = False
    for returned, original in zip(sequences, expected, strict=True):
        wanted = [
            (
                action["id"],
                batch["target_id"],
                action["executor"],
                action.get("resource_node_id"),
            )
            for batch in original["batches"]
            for action in batch["actions"]
        ]
        rows = returned["actions"]
        if [
            (
                row["action_id"],
                row["target_id"],
                row["executor"],
                row.get("resource_node_id"),
            )
            for row in rows
        ] != wanted:
            raise ValueError("installation action identities differ")
        if returned["description"] != original["description"]:
            raise ValueError("installation sequence description differs")
        for row in rows:
            status = row["status"]
            if status not in ("succeeded", "failed", "skipped"):
                raise ValueError("invalid installation action status")
            if status == "skipped":
                continue
            duration = row.get("duration_seconds")
            if (
                type(duration) not in (int, float)
                or not math.isfinite(duration)
                or not 0 <= duration <= 86400
            ):
                raise ValueError("invalid installation action duration")
            begin, end = (
                datetime.fromisoformat(row[key])
                for key in ("started_at", "finished_at")
            )
            if begin.utcoffset() is None or end.utcoffset() is None or end < begin:
                raise ValueError("invalid installation action clocks")
            if status == "failed" and not all(
                isinstance(row.get(key), str) and row[key]
                for key in ("error_type", "error_message")
            ):
                raise ValueError("failed action has no error")
        sequence_status = (
            "skipped"
            if stopped
            else "failed"
            if any(row["status"] == "failed" for row in rows)
            else "succeeded"
        )
        if returned["status"] != sequence_status or (
            stopped and any(row["status"] != "skipped" for row in rows)
        ):
            raise ValueError("installation sequence outcome differs")
        stopped = stopped or sequence_status == "failed"
    if not partial and mapping["status"] != ("failed" if stopped else "succeeded"):
        raise ValueError("installation outcome differs from action results")
    report = InstallationReport.from_mapping(mapping)
    if report.started_at.utcoffset() is None:
        raise ValueError("invalid installation report clock")
    if not partial and (
        report.finished_at is None
        or report.finished_at.utcoffset() is None
        or report.finished_at < report.started_at
    ):
        raise ValueError("invalid installation report completion")
    return report


def uncertain_report(plan, error: str, remote_result: str, *, settled=None):
    from datetime import datetime, timezone

    from ..build_bundle.report import ActionResult, InstallationReport, SequenceResult

    mapping = plan if isinstance(plan, dict) else plan.to_mapping()
    now = datetime.now(timezone.utc)
    sequences = list(settled.sequences) if settled is not None else []
    for sequence in mapping["sequences"][len(sequences) :]:
        rows = tuple(
            ActionResult(
                action_id=action["id"],
                executor=action["executor"],
                target_id=batch["target_id"],
                resource_node_id=action.get("resource_node_id"),
                status="failed",
                error_type="UncertainInstallation",
                error_message=error,
                source_path=action.get("source_path"),
                details={"uncertain": True, "remote_result": remote_result},
            )
            for batch in sequence["batches"]
            for action in batch["actions"]
        )
        sequences.append(
            SequenceResult(
                number=sequence["number"],
                description=sequence["description"],
                status="failed",
                actions=rows,
            )
        )
    return InstallationReport(
        bundle_id=mapping["bundle_id"],
        status="failed",
        started_at=settled.started_at if settled is not None else now,
        finished_at=now,
        sequences=tuple(sequences),
    )


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
        _archive_result = _archive_module.run_bundle(_archive_root, spark, _archive_output, _archive_sha256, workers={workers!r})
    _archive_result["archive_sha256"] = _archive_sha256
    _archive_bytes = json.dumps(_archive_result, separators=(",", ":"), allow_nan=False).encode("utf-8")
    fs.put(_archive_output, _archive_bytes.decode("utf-8"), True)
    emit({{"sha256": hashlib.sha256(_archive_bytes).hexdigest(), "bytes": len(_archive_bytes)}})
finally:
    if _archive_switched:
        for _name in list(sys.modules):
            if _name == "weaver" or _name.startswith("weaver."):
                del sys.modules[_name]
        sys.modules.update(_archive_previous_modules)
        sys.path[:] = _archive_previous_path
        importlib.invalidate_caches()
    shutil.rmtree(_archive_private, ignore_errors=True)
"""
    return "exec(" + repr(body) + ', {"spark": spark, "emit": emit})\n'


def install_in_scope(session, bundle, *, workspace=None, timeout=None, request=None):
    import math
    import time
    from urllib.parse import urlsplit
    from uuid import uuid4

    from ..build_bundle.executors import default_executors
    from ..build_bundle.installer import select_install_batches
    from ..fabric.livy import DEFAULT_STATEMENT_TIMEOUT

    plan = bundle.plan
    if request is not None:
        plan = select_install_batches(
            plan,
            sequence_number=request["sequence_number"],
            batch_ids=request["batch_ids"],
        )
    if (
        plan.execution is None
        or plan.execution.spark_home_target_id is None
        or bundle.store is None
    ):
        return None
    actions = [
        action
        for sequence in plan.sequences
        for batch in sequence.batches
        for action in batch.actions
    ]
    if not actions or any(
        action.executor not in default_executors() for action in actions
    ):
        return None
    targets = {target.id: target for target in plan.targets}
    if any(
        targets[batch.target_id].kind == "warehouse"
        and batch.target_id != plan.execution.catalogue_target_id
        for sequence in plan.sequences
        for batch in sequence.batches
    ):
        return None
    allowance = DEFAULT_STATEMENT_TIMEOUT if timeout is None else timeout
    if (
        type(allowance) not in (int, float)
        or not math.isfinite(allowance)
        or allowance <= 0
    ):
        raise ValueError("archive timeout must be a positive per-action allowance")
    started = time.monotonic()
    carrier = pack_bundle(bundle, request=request)
    if carrier is None:
        return None
    target = next(
        target
        for target in plan.targets
        if target.id == plan.execution.spark_home_target_id
    )
    scope = session.scope(workspace)
    store = scope.transport_store
    from ..targets import ItemRef

    root = scope.resolver.lakehouse(ItemRef(target.item_id))
    stage = root.join("Files", "_weaver_install_" + uuid4().hex)
    incoming = stage / "carrier.zip"
    output = stage / "result.json"

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
                raise ValueError("archive carrier requires a bound OneLake destination")
            return "abfss://" + parts[0] + "@" + address.hostname + "/" + parts[1]
        return value

    source = bootstrap_source(
        carrier, native(incoming), native(output), workers=session.direct_delta_workers
    )
    try:
        store.make_directory(stage)
        store.write(incoming, carrier.data)
    except BaseException:
        store.delete(stage, recursive=True)
        raise
    settled = None
    verified_status = None
    received = False

    def record(result):
        if not hasattr(session, "archive_installations"):
            session.archive_installations = []
        session.archive_installations.append(
            result
            | {
                "carrier_bytes": len(carrier.data),
                "archive_seconds": time.monotonic() - started,
            }
        )

    try:
        receipt = scope.livy_run(
            source,
            name="install_bundle_archive",
            timeout=allowance * len(actions),
            retry_submission=False,
        )
        received = True
        result = read_receipt(receipt, store.read(output))
        if result.get("archive_sha256") != carrier.sha256:
            raise ValueError("archive result belongs to a different carrier")
        if (
            result.get("status") == "declined"
            and result.get("mutated") is False
            and isinstance(result.get("reason"), str)
        ):
            verified_status = "declined"
            return None
        if result.get("status") != "completed":
            raise ValueError("archive installation has not completed")
        if request is not None and result.get("request") != request:
            raise ValueError("archive result belongs to a different batch selection")
        report = decode_report(result["report"], plan)
        verified_status = "completed"
        record(result)
        return report
    except Exception as error:
        try:
            result = json.loads(store.read(output))
            if result.get("archive_sha256") == carrier.sha256:
                if request is not None and result.get("request") != request:
                    raise ValueError(
                        "archive recovery belongs to a different batch selection"
                    )
                if result.get("status") == "completed" and not received:
                    report = decode_report(result["report"], plan)
                    verified_status = "completed"
                    record(result)
                    return report
                if result.get("status") == "running":
                    settled = decode_report(result["report"], plan, partial=True)
        except Exception:
            pass
        return uncertain_report(plan, str(error), output.value, settled=settled)
    finally:
        if verified_status is not None:
            try:
                store.delete(stage, recursive=True)
            except Exception as cleanup_error:
                failure = {
                    "status": verified_status,
                    "stage": stage.value,
                    "remote_result": output.value,
                    "error_type": type(cleanup_error).__name__,
                    "error_message": str(cleanup_error),
                }
                session.archive_cleanup_failures.append(failure)
                try:
                    session.warn(
                        f"Installation carrier cleanup failed: {stage.value}. Result retained at {output.value}: {cleanup_error}"
                    )
                except Exception:
                    pass
