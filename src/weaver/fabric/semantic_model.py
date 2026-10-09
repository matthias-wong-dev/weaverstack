"""Definition and Power BI execution for one resolved SemanticModel."""

from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass

from ..errors import ConfigError, reported_message
from .client import FabricError

POWER_BI_API = "https://api.powerbi.com/v1.0/myorg"


def validate_bound_model(item) -> None:
    from .resources import SEMANTIC_MODEL, Item

    if not isinstance(item, Item) or item.type != SEMANTIC_MODEL:
        raise ConfigError("A bound semantic model must name a typed SemanticModel item")
    for label, value in (("workspace ID", item.workspace_id), ("item ID", item.id)):
        if not isinstance(value, str) or not re.fullmatch(
            r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value
        ):
            raise ConfigError(
                f"SemanticModel/{item.name} has invalid {label} {value!r}"
            )


class SemanticRefreshError(FabricError):
    def __init__(self, message, *, request_id, body, status_code=None):
        from ..runtime.semantic_refresh_result import SemanticRefreshResult

        super().__init__(message, status_code=status_code)
        self.result = SemanticRefreshResult.from_response(
            {**body, "request_id": request_id}, error_message=message
        )


class ConnectionBindingError(FabricError):
    """A semantic model data source has no single connection to bind to."""


@dataclass(frozen=True)
class DataSourceBinding:
    """The ``server;database`` paths bound, and those no connection reaches."""

    bound: tuple[str, ...] = ()
    unreached: tuple[str, ...] = ()


#: Connections that a data source reference can be bound to.
_BINDABLE = frozenset({"ShareableCloud", "OnPremisesGateway", "VirtualNetworkGateway"})


def list_connections(client) -> list[dict]:
    connections, path = [], "connections"
    while path:
        body = client.get_json(path)
        connections.extend(body.get("value", []))
        token = body.get("continuationToken")
        path = f"connections?continuationToken={token}" if token else None
    return connections


class SemanticModelClient:
    def __init__(self, workspace_id: str, model_id: str, *, fabric, power_bi):
        self.workspace_id = workspace_id
        self.model_id = model_id
        self.fabric = fabric
        self.power_bi = power_bi
        self.item_path = f"workspaces/{workspace_id}/semanticModels/{model_id}"
        self.dataset_path = f"groups/{workspace_id}/datasets/{model_id}"

    def get_definition(self, *, format: str = "TMSL", timeout: float = 900) -> dict:
        if format not in {"TMSL", "TMDL"}:
            raise ConfigError("Semantic definition format must be TMSL or TMDL")
        response = self.fabric.request(
            "POST",
            f"{self.item_path}/getDefinition?format={format}",
            expected=(200, 202),
        )
        if response.status_code == 202:
            self.fabric.wait_for_operation(response, timeout=timeout)
            operation = response.headers.get("x-ms-operation-id")
            if not operation:
                raise FabricError("Semantic model definition has no operation ID")
            body = self.fabric.get_json(f"operations/{operation}/result")
        else:
            body = response.json()
        return body["definition"]

    def get_connections(self) -> list[dict]:
        return self.fabric.paged(
            f"workspaces/{self.workspace_id}/items/{self.model_id}/connections"
        )

    def update_definition(
        self, definition: dict, *, allow_purge_data: bool = False, timeout: float = 900
    ) -> dict:
        response = self.fabric.request(
            "POST",
            f"{self.item_path}/updateDefinition",
            payload={
                "definition": definition,
                "options": {"allowPurgeData": allow_purge_data},
            },
            expected=(200, 202),
            retry_transient=False,
        )
        return self.fabric.wait_for_operation(response, timeout=timeout)

    def refresh(
        self, *, timeout: float = 900, poll_interval: float = 2, unreached=()
    ) -> dict:
        """Refresh the model and wait for the outcome.

        ``unreached`` are the ``server;database`` paths no connection reaches,
        which a connection failure names as the fix.
        """

        if not math.isfinite(timeout) or not 1 <= timeout < 86400:
            raise ConfigError(
                "Semantic model refresh timeout must be at least one second and less than 24 hours"
            )
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ConfigError(
                "Semantic model refresh poll interval must be positive and finite"
            )
        deadline = time.monotonic() + timeout
        minutes, seconds = divmod(int(timeout), 60)
        hours, minutes = divmod(minutes, 60)
        response = self.power_bi.request(
            "POST",
            f"{self.dataset_path}/refreshes",
            payload={
                "type": "full",
                "commitMode": "transactional",
                "retryCount": 0,
                "timeout": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
            },
            expected=(202,),
            retry_transient=False,
            timeout=min(timeout, self.power_bi.timeout),
            deadline=deadline,
        )
        request_id = response.headers.get("x-ms-request-id")
        if not isinstance(request_id, str) or not re.fullmatch(
            r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", request_id
        ):
            raise FabricError(
                "Semantic model refresh was accepted without a valid request ID; completion is unknown"
            )
        path = f"{self.dataset_path}/refreshes/{request_id}"
        body = {}
        while time.monotonic() < deadline:
            try:
                response = self.power_bi.request(
                    "GET",
                    path,
                    expected=(200, 202),
                    timeout=min(deadline - time.monotonic(), self.power_bi.timeout),
                    deadline=deadline,
                )
                body = response.json()
            except FabricError as exc:
                raise SemanticRefreshError(
                    str(exc),
                    request_id=request_id,
                    body=body,
                    status_code=exc.status_code,
                ) from exc
            status = body.get("status")
            if status == "Completed":
                if time.monotonic() >= deadline:
                    break
                return {**body, "request_id": request_id}
            if status not in {"Unknown", "NotStarted", "InProgress"}:
                messages = "; ".join(_refresh_errors(body))
                # Fabric's message stays whole; a hint is only appended. Known
                # unreached paths need no reading of the message.
                hint = ""
                if unreached:
                    hint = (
                        " No connection has the path "
                        + ", ".join(unreached)
                        + ". Create a connection for it in Fabric, then load again."
                    )
                elif any(
                    word in messages.casefold()
                    for word in ("connection", "gateway", "credential", "not bound")
                ):
                    hint = " Check the model's data connection in Fabric settings; the connection owner can grant access or update credentials."
                raise SemanticRefreshError(
                    f"Semantic model refresh {request_id} ended with status {status!r}"
                    + (f": {messages}" if messages else "")
                    + hint,
                    request_id=request_id,
                    body=body,
                )
            time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))
        raise SemanticRefreshError(
            f"Semantic model refresh {request_id} did not complete within {timeout}s",
            request_id=request_id,
            body=body,
        )

    def data_sources(self) -> list[dict]:
        return self.power_bi.get_json(f"{self.dataset_path}/datasources").get(
            "value", []
        )

    def bind_data_sources(self) -> DataSourceBinding:
        """Bind each unbound SQL data source to the one connection that reaches it.

        A connection reaches a data source when its path is the source's
        ``server;database``. A source no connection reaches keeps the
        connection Fabric gave it, such as single sign-on.
        """

        unbound = [
            source
            for source in self.data_sources()
            if source.get("datasourceType") == "Sql" and not source.get("datasourceId")
        ]
        if not unbound:
            return DataSourceBinding()
        connections = [
            c
            for c in list_connections(self.fabric)
            if c.get("connectivityType") in _BINDABLE
            and (c.get("connectionDetails") or {}).get("type") == "SQL"
        ]
        bound, unreached = [], []
        for source in unbound:
            details = source.get("connectionDetails") or {}
            server, database = details.get("server"), details.get("database")
            path = f"{server};{database}"
            matches = [
                c
                for c in connections
                if str(c["connectionDetails"].get("path", "")).casefold()
                == path.casefold()
            ]
            if not matches:
                unreached.append(path)
                continue
            if len(matches) > 1:
                names = ", ".join(sorted(str(c.get("displayName")) for c in matches))
                raise ConnectionBindingError(
                    f"The model reads database {database} on {server}, and several "
                    f"connections reach it: {names}. Keep one."
                )
            connection = matches[0]
            self.fabric.request(
                "POST",
                f"{self.item_path}/bindConnection",
                payload={
                    "connectionBinding": {
                        "id": connection["id"],
                        "connectivityType": connection["connectivityType"],
                        "connectionDetails": {"type": "SQL", "path": path},
                    }
                },
                expected=(200,),
                retry_transient=False,
            )
            bound.append(path)
        return DataSourceBinding(bound=tuple(bound), unreached=tuple(unreached))

    def query_dax(self, query: str) -> list[dict]:
        response = self.power_bi.request(
            "POST",
            f"{self.dataset_path}/executeQueries",
            payload={
                "queries": [{"query": query}],
                "serializerSettings": {"includeNulls": True},
            },
            expected=(200,),
        )
        try:
            payload = response.json()
            for key in ("results", "tables"):
                _check_dax_error(payload)
                entries = payload.get(key)
                if not isinstance(entries, list) or len(entries) != 1:
                    raise ValueError(f"expected one {key} entry")
                payload = entries[0]
            _check_dax_error(payload)
            rows = payload.get("rows", [])
            if not isinstance(rows, list) or any(
                not isinstance(row, dict) for row in rows
            ):
                raise ValueError("expected result rows")
            return rows
        except (AttributeError, TypeError, ValueError) as exc:
            raise FabricError(
                f"DAX returned an invalid response for {self.model_id}: {exc}"
            ) from exc

    def invalid_measures(self) -> list[tuple[str, str, str]]:
        """Measures the deployed model cannot evaluate, as (table, measure, reason).

        Fabric accepts a definition whose measures reference missing objects and
        marks them in `INFO.VIEW.MEASURES()`. Evaluating one confirms it and
        returns the reason.
        """

        rows = self.query_dax(
            "EVALUATE SELECTCOLUMNS("
            'FILTER(INFO.VIEW.MEASURES(), [State] <> "Valid"), '
            '"Table", [Table], "Measure", [Name], "State", [State])'
        )
        invalid = []
        for row in rows:
            table, measure = str(row.get("[Table]")), str(row.get("[Measure]"))
            # The state only nominates a measure; failing to evaluate confirms it.
            try:
                self.query_dax(
                    f'EVALUATE ROW("Value", {_dax_table(table)}[{_dax_member(measure)}])'
                )
            except FabricError as exc:
                invalid.append(
                    (table, measure, _dax_reason(str(exc)) or str(row.get("[State]")))
                )
        return invalid


def _dax_table(name: str) -> str:
    return "'" + name.replace("'", "''") + "'"


def _dax_member(name: str) -> str:
    return name.replace("]", "]]")


def _dax_reason(message: str) -> str:
    message = re.sub(r"</?oii>", "", message)
    message = message.removeprefix("DAX failed: ")
    return re.sub(r"^MdxScript\([^)]*\) \(\d+, \d+\) ", "", message).strip()


def _refresh_errors(body) -> list[str]:
    """Distinct readable failure messages, without the service's JSON wrapping.

    Power BI repeats one cause per table and nests it as JSON inside JSON.
    """

    import json

    found, codes = [], []

    def collect(value):
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("{"):
                try:
                    collect(json.loads(text))
                    return
                except ValueError:
                    pass
            if text and text not in found:
                found.append(text)
        elif isinstance(value, dict):
            code = value.get("errorCode") or value.get("code")
            if isinstance(code, str) and code not in codes:
                codes.append(code)
            if "errorDescription" in value:
                collect(value["errorDescription"])
            detail = value.get("detail")
            if isinstance(detail, dict) and "value" in detail:
                collect(detail["value"])
            for key in ("error", "pbi.error", "details"):
                if key in value:
                    collect(value[key])
            if "message" in value and not any(
                k in value for k in ("error", "pbi.error", "details", "detail")
            ):
                collect(value["message"])
        elif isinstance(value, list):
            for member in value:
                collect(member)

    for message in body.get("messages", []):
        if isinstance(message, dict) and message.get("type") == "Error":
            collect(message.get("message", ""))
    collect(body.get("serviceExceptionJson") or "")
    # A transaction's other tables report only that they were cancelled.
    cancelled = "The current operation was cancelled because another operation in the transaction failed."
    messages = [m for m in found if m != cancelled] or found
    labels = [c for c in codes if not c.endswith("_Details_Label")]
    return messages + ([f"({', '.join(labels)})"] if labels else [])


def _check_dax_error(payload):
    if not isinstance(payload, dict):
        raise ValueError("expected a result object")
    if payload.get("error"):
        raise FabricError(
            f"DAX failed: {reported_message(payload) or payload['error']}"
        )
