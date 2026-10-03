"""Definition and Power BI execution for one resolved SemanticModel."""

from __future__ import annotations

import math
import re
import time

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


class SemanticModelClient:
    def __init__(self, workspace_id: str, model_id: str, *, fabric, power_bi):
        self.workspace_id = workspace_id
        self.model_id = model_id
        self.fabric = fabric
        self.power_bi = power_bi
        self.item_path = f"workspaces/{workspace_id}/semanticModels/{model_id}"
        self.dataset_path = f"groups/{workspace_id}/datasets/{model_id}"

    def get_definition(self, *, timeout: float = 900) -> dict:
        response = self.fabric.request(
            "POST", f"{self.item_path}/getDefinition?format=TMSL", expected=(200, 202)
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

    def refresh(self, *, timeout: float = 900, poll_interval: float = 2) -> dict:
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
                messages = "; ".join(
                    str(message.get("message", ""))
                    for message in body.get("messages", [])
                    if isinstance(message, dict) and message.get("type") == "Error"
                )
                service_error = body.get("serviceExceptionJson")
                if service_error:
                    messages = "; ".join(filter(None, (messages, str(service_error))))
                hint = ""
                if any(
                    word in messages.casefold()
                    for word in (
                        "premium_aswl_error",
                        "gateway",
                        "credential",
                        "not bound",
                    )
                ):
                    hint = " Check the model's connection and gateway in Fabric settings; ask the connection owner to configure credentials or grant access."
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


def _check_dax_error(payload):
    if not isinstance(payload, dict):
        raise ValueError("expected a result object")
    if payload.get("error"):
        raise FabricError(
            f"DAX failed: {reported_message(payload) or payload['error']}"
        )
