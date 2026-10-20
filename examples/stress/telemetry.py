"""Retain raw evidence for the generated stress estate."""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import yaml

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_API_SEGMENT = re.compile(r"[A-Za-z0-9._-]+\Z")
_KUSTO_HOST_SUFFIXES = (".kusto.windows.net", ".kusto.fabric.microsoft.com")


def _safe_api_segment(value: str) -> bool:
    return value not in {"", ".", ".."} and _API_SEGMENT.fullmatch(value) is not None


def _kusto_origin(value: object) -> str:
    origin = str(value).rstrip("/")
    parsed = urlparse(origin)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(
            "query_service_uri must be an approved HTTPS Kusto origin."
        ) from exc
    hostname = (parsed.hostname or "").casefold()
    if (
        parsed.scheme != "https"
        or not hostname
        or not any(hostname.endswith(suffix) for suffix in _KUSTO_HOST_SUFFIXES)
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.path
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("query_service_uri must be an approved HTTPS Kusto origin.")
    return origin


@dataclass(frozen=True)
class TelemetryConfig:
    capacity_id: str
    query_service_uri: str
    database: str
    overview_table: str
    operation_table: str

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "TelemetryConfig":
        required = (
            "capacity_id",
            "query_service_uri",
            "database",
            "overview_table",
            "operation_table",
        )
        missing = [name for name in required if not value.get(name)]
        if missing:
            raise ValueError(
                f"Telemetry configuration is missing {', '.join(missing)}."
            )
        capacity_id = str(value["capacity_id"])
        try:
            capacity_id = str(uuid.UUID(capacity_id))
        except ValueError as exc:
            raise ValueError("capacity_id must be a UUID.") from exc
        query_service_uri = _kusto_origin(value["query_service_uri"])
        identifiers = {
            name: str(value[name])
            for name in ("database", "overview_table", "operation_table")
        }
        for name, identifier in identifiers.items():
            if not _IDENTIFIER.fullmatch(identifier):
                raise ValueError(f"{name} must be a KQL identifier.")
        return cls(
            capacity_id=capacity_id,
            query_service_uri=query_service_uri,
            **identifiers,
        )

    @classmethod
    def load(cls, path: Path) -> "TelemetryConfig":
        value = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("Telemetry configuration must be a mapping.")
        return cls.from_mapping(value)


class TelemetryError(RuntimeError):
    pass


class KustoClient:
    def __init__(
        self,
        *,
        query_service_uri: str,
        database: str,
        credential,
        request=None,
    ) -> None:
        if not _IDENTIFIER.fullmatch(database):
            raise ValueError("database must be a KQL identifier.")
        self.query_service_uri = _kusto_origin(query_service_uri)
        self.database = database
        self.credential = credential
        if request is None:
            import requests

            request = requests.request
        self.request = request

    def window(
        self, table: str, started_at: datetime, finished_at: datetime
    ) -> list[dict[str, Any]]:
        if not _IDENTIFIER.fullmatch(table):
            raise ValueError("table must be a KQL identifier.")
        query = (
            f"{table}\n"
            "| where EventProcessedUtcTime between "
            f"(datetime({_kusto_time(started_at)}) .. "
            f"datetime({_kusto_time(finished_at)}))\n"
            "| order by EventProcessedUtcTime asc"
        )
        return self.query(query)

    def query(self, query: str) -> list[dict[str, Any]]:
        token = self.credential.get_token(
            "https://kusto.kusto.windows.net/.default"
        ).token
        response = _read_request(
            self.request,
            "POST",
            f"{self.query_service_uri}/v2/rest/query",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"db": self.database, "csl": query},
            timeout=120,
        )
        if response.status_code != 200:
            detail = getattr(response, "text", "").strip()
            suffix = f": {detail[:1000]}" if detail else ""
            raise TelemetryError(
                f"KQL query returned HTTP {response.status_code}{suffix}."
            )
        payload = response.json()
        if isinstance(payload, dict):
            detail = _error_detail(payload)
            if detail:
                raise TelemetryError(f"KQL query failed: {detail}")
            raise TelemetryError("KQL query returned an invalid response.")
        primary = None
        for frame in payload:
            if frame.get("TableKind") == "PrimaryResult":
                names = [column["ColumnName"] for column in frame.get("Columns", ())]
                primary = [dict(zip(names, row)) for row in frame.get("Rows", ())]
            if frame.get("FrameType") == "DataSetCompletion":
                failed = frame.get("HasErrors") or frame.get("Cancelled")
                detail = _error_detail(frame)
                if failed or detail:
                    raise TelemetryError(
                        "KQL query did not complete successfully"
                        + (f": {detail}" if detail else ".")
                    )
        if primary is None:
            raise TelemetryError("KQL query returned no primary result.")
        return primary


def _kusto_time(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Telemetry timestamps must include a timezone.")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _error_detail(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("OneApiErrors", "Errors", "Error", "error"):
            if value.get(key):
                return _error_detail(value[key])
        for key in ("message", "Message", "code", "Code"):
            if value.get(key):
                return str(value[key])
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(filter(None, (_error_detail(item) for item in value)))
    return str(value) if value else ""


class TelemetryCapture:
    def __init__(self, config: TelemetryConfig, *, credential=None, request=None):
        self.config = config
        if credential is None:
            from azure.identity import AzureCliCredential

            credential = AzureCliCredential()
        self.credential = credential
        if request is None:
            import requests

            request = requests.request
        self.request = request

    def collect(
        self,
        evidence: "EvidenceRun",
        *,
        projects: dict[str, Path],
        started_at: datetime,
        finished_at: datetime,
    ) -> dict[str, Any]:
        discovery_errors: list[dict[str, str]] = []
        try:
            targets = self.discover(projects)
        except Exception as exc:  # retain other independent telemetry layers
            targets = {"spark": [], "warehouses": []}
            discovery_errors.append({"error": _exception_text(exc)})
        result = {
            "discovery": {
                "spark_targets": len(targets["spark"]),
                "warehouse_targets": len(targets["warehouses"]),
                "errors": discovery_errors,
            }
        }
        try:
            result["capacity"] = self.capacity(
                evidence, started_at=started_at, finished_at=finished_at
            )
        except Exception as exc:  # continue with Spark and Warehouse evidence
            result["capacity"] = {
                "overview": 0,
                "operations": 0,
                "errors": [{"error": _exception_text(exc)}],
            }
        try:
            result["spark"] = self.spark(
                evidence,
                targets=targets["spark"],
                started_at=started_at,
                finished_at=finished_at,
            )
        except Exception as exc:  # continue with Warehouse evidence
            error = {"error": _exception_text(exc)}
            result["spark"] = {
                "applications": 0,
                "targets": [
                    {**target, "applications": 0, "apps": [], "errors": [error]}
                    for target in targets["spark"]
                ],
                "errors": [error],
            }
        try:
            result["warehouses"] = self.warehouses(
                evidence,
                targets=targets["warehouses"],
                started_at=started_at,
                finished_at=finished_at,
            )
        except Exception as exc:
            error = {"error": _exception_text(exc)}
            result["warehouses"] = {
                "warehouses": len(targets["warehouses"]),
                "requests": 0,
                "targets": [
                    {**target, "requests": 0, "errors": [error]}
                    for target in targets["warehouses"]
                ],
                "errors": [error],
            }
        evidence.capture("collection", result)
        return result

    def discover(self, projects: dict[str, Path]) -> dict[str, list[dict[str, str]]]:
        spark: list[dict[str, str]] = []
        warehouses: list[dict[str, str]] = []
        for project, config_path in projects.items():
            authored = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
            if not isinstance(authored, dict):
                raise ValueError(f"{config_path} must contain a workspace mapping.")
            workspace = self._resolve_workspace(str(authored["workspace"]))
            workspace_capacity = str(workspace.get("capacityId") or "").casefold()
            if workspace_capacity != self.config.capacity_id:
                raise TelemetryError(
                    f"Workspace {workspace.get('displayName')} is not assigned to "
                    f"capacity {self.config.capacity_id}."
                )
            items = self._fabric_list(f"workspaces/{workspace['id']}/items")
            targets = authored.get("targets") or {}
            for logical, physical in targets.items():
                if str(logical).startswith("Lakehouse/"):
                    item = _one_item(items, str(physical), "Lakehouse")
                    spark.append(
                        {
                            "project": project,
                            "workspace_id": workspace["id"],
                            "workspace_name": workspace["displayName"],
                            "lakehouse_id": item["id"],
                            "lakehouse_name": item["displayName"],
                        }
                    )
            warehouse_names = [str(authored["catalogue"]).split("/", 1)[-1]]
            warehouse_names.extend(
                str(physical)
                for logical, physical in targets.items()
                if str(logical).startswith("Warehouse/")
            )
            for name in dict.fromkeys(warehouse_names):
                item = _one_item(items, name, "Warehouse")
                warehouses.append(
                    {
                        "project": project,
                        "workspace_config": str(Path(config_path)),
                        "workspace_id": workspace["id"],
                        "workspace_name": workspace["displayName"],
                        "warehouse_id": item["id"],
                        "warehouse_name": item["displayName"],
                    }
                )
        return {"spark": spark, "warehouses": warehouses}

    def _resolve_workspace(self, reference: str) -> dict[str, Any]:
        try:
            uuid.UUID(reference)
        except ValueError:
            workspaces = self._fabric_list("workspaces")
            matches = [
                workspace
                for workspace in workspaces
                if str(workspace.get("displayName", "")).casefold()
                == reference.casefold()
            ]
            if len(matches) != 1:
                raise TelemetryError(
                    f"Workspace {reference!r} resolved to {len(matches)} items."
                )
            workspace_id = matches[0]["id"]
        else:
            workspace_id = reference
        return self._fabric_get(f"workspaces/{workspace_id}")

    def capacity(
        self,
        evidence: "EvidenceRun",
        *,
        started_at: datetime,
        finished_at: datetime,
    ) -> dict[str, Any]:
        client = KustoClient(
            query_service_uri=self.config.query_service_uri,
            database=self.config.database,
            credential=self.credential,
            request=self.request,
        )
        rows = {}
        errors: list[dict[str, str]] = []
        for name, table in (
            ("overview", self.config.overview_table),
            ("operations", self.config.operation_table),
        ):
            query = (
                f"{table}\n"
                "| extend EventTime = todatetime(['time'])\n"
                "| where EventTime between "
                f"(datetime({_kusto_time(started_at)}) .. "
                f"datetime({_kusto_time(finished_at)}))\n"
                "| where tolower(tostring(data.capacityId)) == "
                f"'{self.config.capacity_id}'\n"
                "| order by EventTime asc"
            )
            try:
                rows[name] = client.query(query)
            except Exception as exc:  # query the other required capacity table
                rows[name] = []
                errors.append({"layer": name, "error": _exception_text(exc)})
                continue
            _write_jsonl(evidence.path / "capacity" / f"{name}.jsonl", rows[name])
        return {**{name: len(found) for name, found in rows.items()}, "errors": errors}

    def spark(
        self,
        evidence: "EvidenceRun",
        *,
        targets: list[dict[str, str]],
        started_at: datetime,
        finished_at: datetime,
    ) -> dict[str, Any]:
        applications = 0
        errors: list[dict[str, str]] = []
        target_results: list[dict[str, Any]] = []
        for target in targets:
            target_result: dict[str, Any] = {
                **target,
                "applications": 0,
                "apps": [],
                "errors": [],
            }
            target_results.append(target_result)
            workspace_id = target["workspace_id"]
            lakehouse_id = target["lakehouse_id"]
            sessions_path = (
                f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/livySessions"
            )
            try:
                sessions = self._fabric_list(sessions_path)
            except Exception as exc:
                error = {
                    "lakehouse_id": lakehouse_id,
                    "error": _exception_text(exc),
                }
                target_result["errors"].append(error)
                errors.append(error)
                continue
            for session in sessions:
                if not _session_overlaps(session, started_at, finished_at):
                    continue
                livy_id = str(session.get("livyId") or "")
                application_id = str(session.get("sparkApplicationId") or "")
                if not livy_id or not application_id:
                    continue
                if not _safe_api_segment(livy_id) or not _safe_api_segment(
                    application_id
                ):
                    error = {
                        "application_id": application_id,
                        "error": "Fabric returned an invalid Spark identifier.",
                    }
                    target_result["errors"].append(error)
                    errors.append(error)
                    continue
                applications += 1
                target_result["applications"] += 1
                root = evidence.path / "spark" / application_id
                _write_json(root / "session.json", {**target, **session})
                base = f"{sessions_path}/{livy_id}/applications/{application_id}"
                app_result: dict[str, Any] = {
                    "application_id": application_id,
                    "attempt": None,
                    "layers": {},
                    "errors": [],
                }
                target_result["apps"].append(app_result)
                try:
                    attempt = _latest_attempt(self._fabric_get(base))
                    app_result["attempt"] = attempt
                except Exception as exc:
                    error = {
                        "application_id": application_id,
                        "endpoint": "application.json",
                        "error": _exception_text(exc),
                    }
                    app_result["errors"].append(error)
                    errors.append(error)
                    continue
                endpoints = {
                    "resourceUsage": (
                        "resource-usage.json",
                        f"{base}/{attempt}/resourceUsage",
                    ),
                    "jobs": ("jobs.json", f"{base}/{attempt}/jobs"),
                    "stages": ("stages.json", f"{base}/{attempt}/stages"),
                    "executors": ("executors.json", f"{base}/{attempt}/allexecutors"),
                    "sql": ("sql.json", f"{base}/{attempt}/sql"),
                }
                for layer, (name, path) in endpoints.items():
                    try:
                        payload = self._fabric_get(path)
                    except Exception as exc:
                        error = {
                            "application_id": application_id,
                            "endpoint": name,
                            "error": _exception_text(exc),
                        }
                        app_result["errors"].append(error)
                        errors.append(error)
                        app_result["layers"][layer] = 0
                        continue
                    _write_json(root / name, payload)
                    app_result["layers"][layer] = _payload_count(payload)
        return {
            "applications": applications,
            "targets": target_results,
            "errors": errors,
        }

    def warehouses(
        self,
        evidence: "EvidenceRun",
        *,
        targets: list[dict[str, str]],
        started_at: datetime,
        finished_at: datetime,
        session_factory=None,
    ) -> dict[str, Any]:
        if session_factory is None:
            import weaver

            session_factory = weaver.session
        statement = """
select *
from queryinsights.exec_requests_history
where start_time <= ? and end_time >= ?
order by end_time
""".strip()
        parameters = (
            finished_at.astimezone(timezone.utc).replace(tzinfo=None),
            started_at.astimezone(timezone.utc).replace(tzinfo=None),
        )
        request_count = 0
        errors: list[dict[str, str]] = []
        target_results: list[dict[str, Any]] = []
        for target in targets:
            name = target["warehouse_name"]
            target_result: dict[str, Any] = {
                **target,
                "requests": 0,
                "errors": [],
            }
            target_results.append(target_result)
            try:
                endpoint = self._warehouse_endpoint(target)
                with session_factory(
                    workspace_config=target["workspace_config"]
                ) as session:
                    rows = getattr(session, "sql_executor")(endpoint).query(
                        statement, parameters
                    )
            except Exception as exc:  # preserve other targets after one read fails
                error = {
                    "project": target["project"],
                    "warehouse_id": target["warehouse_id"],
                    "warehouse": name,
                    "error": _exception_text(exc),
                }
                target_result["errors"].append(error)
                errors.append(error)
                continue
            target_result["requests"] = len(rows)
            request_count += len(rows)
            path = (
                evidence.path
                / "warehouse"
                / (
                    f"{_slug(target['project'])}-{_slug(name)}-"
                    f"{_slug(target['warehouse_id'])}.jsonl"
                )
            )
            _write_jsonl(path, rows)
        return {
            "warehouses": len(targets),
            "requests": request_count,
            "targets": target_results,
            "errors": errors,
        }

    def _warehouse_endpoint(self, target: dict[str, str]):
        from weaver.sql import SqlEndpoint

        workspace_id = target["workspace_id"]
        warehouse_id = target["warehouse_id"]
        current = self._fabric_get(
            f"workspaces/{workspace_id}/warehouses/{warehouse_id}"
        )
        returned_id = str(current.get("id") or "")
        if returned_id.casefold() != warehouse_id.casefold():
            raise TelemetryError(
                f"Fabric returned Warehouse {returned_id!r} for ID {warehouse_id!r}."
            )
        if current.get("type") not in (None, "Warehouse"):
            raise TelemetryError(f"Fabric item {warehouse_id!r} is not a Warehouse.")
        current_name = str(current.get("displayName") or "").strip()
        if not current_name:
            raise TelemetryError(
                f"Fabric returned no name for Warehouse {warehouse_id!r}."
            )
        connection = self._fabric_get(
            f"workspaces/{workspace_id}/warehouses/{warehouse_id}/connectionString"
        ).get("connectionString")
        if not isinstance(connection, str) or not connection.strip():
            raise TelemetryError(
                f"Fabric returned no connection string for Warehouse {warehouse_id!r}."
            )
        return SqlEndpoint(
            server=_sql_server(connection),
            database=current_name,
            workspace_id=workspace_id,
            warehouse_id=warehouse_id,
            warehouse_name=current_name,
        )

    def _fabric_list(self, path: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        next_path: str | None = path
        seen: set[str] = set()
        while next_path:
            url = self._fabric_url(next_path)
            if url in seen:
                raise TelemetryError("Fabric returned a repeated continuation URL.")
            seen.add(url)
            payload = self._fabric_get(url)
            values = payload.get("value", ())
            if not isinstance(values, list):
                raise TelemetryError("Fabric list response value must be a list.")
            items.extend(values)
            continuation = payload.get("continuationUri")
            if continuation:
                next_path = str(continuation)
                self._fabric_url(next_path, continuation=True)
                continue
            token = payload.get("continuationToken")
            next_path = _with_continuation_token(path, str(token)) if token else None
        return items

    @staticmethod
    def _fabric_url(path: str, *, continuation: bool = False) -> str:
        parsed = urlparse(path)
        if parsed.scheme or parsed.netloc:
            try:
                port = parsed.port
            except ValueError as exc:
                raise TelemetryError("Fabric continuation URI is invalid.") from exc
            if (
                parsed.scheme != "https"
                or (parsed.hostname or "").casefold() != "api.fabric.microsoft.com"
                or parsed.username is not None
                or parsed.password is not None
                or port is not None
                or parsed.fragment
                or not parsed.path.startswith("/v1/")
            ):
                label = "continuation URI" if continuation else "request URI"
                raise TelemetryError(
                    f"Fabric {label} must stay on api.fabric.microsoft.com."
                )
            return path
        if path.startswith("//") or parsed.fragment:
            raise TelemetryError("Fabric request path is invalid.")
        return f"https://api.fabric.microsoft.com/v1/{path.lstrip('/')}"

    def _fabric_get(self, path: str):
        token = self.credential.get_token(
            "https://api.fabric.microsoft.com/.default"
        ).token
        url = self._fabric_url(path)
        response = _read_request(
            self.request,
            "GET",
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
        )
        if response.status_code != 200:
            raise TelemetryError(
                f"Fabric GET {path} returned HTTP {response.status_code}."
            )
        return response.json()


def _exception_text(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _payload_count(value: Any) -> int:
    if isinstance(value, (list, tuple)):
        return len(value)
    if isinstance(value, dict):
        for key in ("timestamps", "value"):
            if key in value:
                return _payload_count(value[key])
        if "data" in value:
            return _payload_count(value["data"])
        counts = [_payload_count(item) for item in value.values()]
        return max(counts, default=0)
    return int(value not in (None, "", False))


def _sql_server(value: str) -> str:
    text = value.strip().strip(";")
    if ";" in text or "=" in text:
        fields = {}
        for part in text.split(";"):
            if "=" in part:
                key, item = part.split("=", 1)
                fields[key.strip().casefold()] = item.strip()
        text = (
            fields.get("server")
            or fields.get("data source")
            or fields.get("address")
            or fields.get("addr")
            or fields.get("network address")
            or ""
        )
    text = text.removeprefix("tcp:").strip()
    if "," in text:
        text = text.rsplit(",", 1)[0].strip()
    if not text:
        raise TelemetryError("Fabric returned an invalid Warehouse SQL endpoint.")
    return text


def _with_continuation_token(path: str, token: str) -> str:
    parsed = urlparse(path)
    query = [
        (name, value)
        for name, value in parse_qsl(parsed.query)
        if name != "continuationToken"
    ]
    query.append(("continuationToken", token))
    return urlunparse(parsed._replace(query=urlencode(query)))


def _latest_attempt(application: dict[str, Any]) -> int:
    attempts = application.get("attempts") if isinstance(application, dict) else None
    if not isinstance(attempts, list):
        raise TelemetryError("Spark application response has no attempts.")
    found: list[int] = []
    for attempt in attempts:
        try:
            number = int(attempt["attemptId"])
        except (KeyError, TypeError, ValueError):
            continue
        if number >= 0:
            found.append(number)
    if not found:
        raise TelemetryError("Spark application response has no valid attempt number.")
    return max(found)


def _session_overlaps(
    session: dict[str, Any], started_at: datetime, finished_at: datetime
) -> bool:
    submitted = _parse_utc(
        session.get("startDateTime") or session.get("submittedDateTime")
    )
    ended = _parse_utc(session.get("endDateTime")) or datetime.max.replace(
        tzinfo=timezone.utc
    )
    return submitted is not None and submitted <= finished_at and ended >= started_at


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Fabric timestamp has no timezone: {value!r}.")
    return parsed.astimezone(timezone.utc)


def _read_request(request, method: str, url: str, **kwargs):
    import requests

    for attempt in range(1, 5):
        try:
            response = request(method, url, **kwargs)
        except requests.exceptions.RequestException:
            if attempt == 4:
                raise
            time.sleep(attempt)
            continue
        if response.status_code not in {429, 502, 503, 504} or attempt == 4:
            return response
        asked = getattr(response, "headers", {}).get("Retry-After")
        try:
            delay = float(asked) if asked is not None else float(attempt)
        except ValueError:
            delay = float(attempt)
        time.sleep(max(0.0, delay))
    raise AssertionError("unreachable")


def _one_item(items, name: str, item_type: str) -> dict[str, Any]:
    matches = [
        item
        for item in items
        if str(item.get("displayName", "")).casefold() == name.casefold()
        and item.get("type") == item_type
    ]
    if len(matches) != 1:
        raise TelemetryError(f"{item_type} {name!r} resolved to {len(matches)} items.")
    return matches[0]


class EvidenceRun:
    """One immutable evidence directory for a stress run."""

    def __init__(
        self,
        root: Path,
        *,
        run_id: str | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.run_id = run_id or self.now().strftime("%Y%m%dT%H%M%SZ")
        if (
            not self.run_id
            or self.run_id in {".", ".."}
            or Path(self.run_id).name != self.run_id
            or "/" in self.run_id
            or "\\" in self.run_id
        ):
            raise ValueError("run_id must be one safe path component.")
        self.path = (self.root / self.run_id).resolve()
        try:
            self.path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("run_id must resolve inside the evidence root.") from exc
        self.manifest_path = self.path / "manifest.json"
        self.manifest: dict[str, Any] = {}
        self._names: dict[str, int] = {}
        self.started_at: datetime | None = None

    def start(self, *, command: str, options: dict, identity: dict) -> None:
        if self.path.parent != self.root:
            raise ValueError("run_id must resolve inside the evidence root.")
        self.path.mkdir(parents=True, exist_ok=False)
        (self.path / "weaver").mkdir()
        self.started_at = self.now()
        self.manifest = {
            "run_id": self.run_id,
            "status": "running",
            "command": command,
            "options": options,
            "identity": identity,
            "started_at": self.started_at.isoformat(),
            "finished_at": None,
            "reports": [],
            "sessions": [],
        }
        self._write_manifest()

    def report(self, step: str, project: str, report) -> Path:
        relative = self._relative(step, project, "report")
        mapping = report.to_mapping()
        installation = getattr(report, "installation_report", None)
        if installation is not None:
            mapping["installation_report"] = installation.to_mapping()
        _write_json(self.path / relative, mapping)
        self.manifest["reports"].append(relative.as_posix())
        self._write_manifest()
        return self.path / relative

    def session(self, step: str, project: str, session) -> Path:
        relative = self._relative(step, project, "session")
        _write_json(self.path / relative, session.telemetry.to_mapping())
        self.manifest["sessions"].append(relative.as_posix())
        self._write_manifest()
        return self.path / relative

    def finish(self, *, status: str) -> None:
        self.manifest["status"] = status
        self.manifest["finished_at"] = self.now().isoformat()
        self._write_manifest()

    def capture(self, name: str, value: Any) -> None:
        self.manifest.setdefault("telemetry", {})[name] = value
        self._write_manifest()

    def _relative(self, step: str, project: str, kind: str) -> Path:
        stem = f"{_slug(project)}-{_slug(step)}-{kind}"
        occurrence = self._names.get(stem, 0) + 1
        self._names[stem] = occurrence
        suffix = "" if occurrence == 1 else f"-{occurrence}"
        return Path("weaver") / f"{stem}{suffix}.json"

    def _write_manifest(self) -> None:
        _write_json(self.manifest_path, self.manifest)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, default=str, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _write_jsonl(path: Path, values) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        for value in values:
            stream.write(json.dumps(value, default=str) + "\n")
    os.replace(temporary, path)
