"""Deploy and execute Fabric notebooks from the optional desktop CLI."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
import time

from ..errors import CommandError
from .client import FabricClient, FabricError
from .resources import (
    ENVIRONMENT,
    LAKEHOUSE,
    NOTEBOOK,
    ItemNotFoundError,
    find_item,
    find_workspace,
)

_SUPPORTED = {
    ".ipynb": ("ipynb", "notebook-content.ipynb"),
    ".py": ("fabricGitSource", "notebook-content.py"),
}
_TERMINAL = {"completed", "failed", "cancelled", "canceled", "deduped"}
_SUCCEEDED = {"completed", "deduped"}


@dataclass(frozen=True)
class NotebookPushResult:
    workspace: str
    notebook: str
    notebook_id: str
    source: str
    action: str

    def to_mapping(self) -> dict:
        return self.__dict__.copy()


@dataclass(frozen=True)
class NotebookRunResult:
    workspace: str
    notebook: str
    notebook_id: str
    job_url: str
    status: str
    exit_value: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status.casefold() in _SUCCEEDED

    def to_mapping(self) -> dict:
        return self.__dict__.copy()


def push_notebook(
    source: str | Path,
    *,
    workspace: str,
    name: str | None = None,
    description: str | None = None,
    client: FabricClient | None = None,
) -> NotebookPushResult:
    """Create or update one Notebook definition; Resources are not transported."""

    source_path = Path(source).expanduser()
    if not source_path.is_file():
        raise CommandError(f"Notebook source was not found: {source_path}")
    try:
        definition_format, part_path = _SUPPORTED[source_path.suffix.casefold()]
    except KeyError as exc:
        raise CommandError("Notebook source must use the .py or .ipynb extension.") from exc
    notebook_name = name or source_path.stem
    client = client or FabricClient()
    physical_workspace = find_workspace(workspace, client=client)
    definition = {
        "definition": {
            "format": definition_format,
            "parts": [
                {
                    "path": part_path,
                    "payload": base64.b64encode(source_path.read_bytes()).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
            ],
        }
    }
    try:
        notebook = find_item(
            physical_workspace, notebook_name, item_type=NOTEBOOK, client=client
        )
    except ItemNotFoundError:
        payload = {"displayName": notebook_name, **definition}
        if description:
            payload["description"] = description
        response = client.request(
            "POST",
            f"workspaces/{physical_workspace.id}/notebooks",
            payload=payload,
            expected=(200, 201, 202),
        )
        client.wait_for_operation(response)
        notebook = find_item(
            physical_workspace, notebook_name, item_type=NOTEBOOK, client=client
        )
        action = "created"
    else:
        response = client.request(
            "POST",
            f"workspaces/{physical_workspace.id}/notebooks/{notebook.id}/updateDefinition",
            payload=definition,
            expected=(200, 202),
        )
        client.wait_for_operation(response)
        action = "updated"
    return NotebookPushResult(
        workspace=physical_workspace.name,
        notebook=notebook.name,
        notebook_id=notebook.id,
        source=str(source_path),
        action=action,
    )


def run_notebook(
    name: str,
    *,
    workspace: str,
    lakehouse: str,
    environment: str,
    wait: bool = True,
    timeout: float = 7200.0,
    poll_interval: float = 10.0,
    client: FabricClient | None = None,
) -> NotebookRunResult:
    """Run a notebook with an explicit default Lakehouse and Environment."""

    client = client or FabricClient()
    physical_workspace = find_workspace(workspace, client=client)
    notebook = find_item(physical_workspace, name, item_type=NOTEBOOK, client=client)
    default_lakehouse = find_item(
        physical_workspace, lakehouse, item_type=LAKEHOUSE, client=client
    )
    attached_environment = find_item(
        physical_workspace, environment, item_type=ENVIRONMENT, client=client
    )
    reference = lambda item: {
        "referenceType": "ById",
        "itemId": item.id,
        "workspaceId": physical_workspace.id,
    }
    payload = {
        "executionData": {
            "compute": "Spark",
            "computeConfiguration": {
                "defaultLakehouse": reference(default_lakehouse),
                "attachedEnvironment": reference(attached_environment),
            },
        }
    }
    response = client.request(
        "POST",
        f"workspaces/{physical_workspace.id}/notebooks/{notebook.id}/jobs/execute/instances?beta=false",
        payload=payload,
        expected=(202,),
    )
    job_url = response.headers.get("Location") or response.headers.get("location")
    if not job_url:
        raise FabricError("Fabric accepted the notebook job without returning its location.")
    result = NotebookRunResult(
        workspace=physical_workspace.name,
        notebook=notebook.name,
        notebook_id=notebook.id,
        job_url=job_url,
        status="Accepted",
    )
    if not wait:
        return result

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(max(0.0, poll_interval))
        body = client.get_json(job_url)
        status = str(body.get("status") or body.get("state") or "Unknown")
        if status.casefold() not in _TERMINAL:
            continue
        result = NotebookRunResult(
            workspace=physical_workspace.name,
            notebook=notebook.name,
            notebook_id=notebook.id,
            job_url=job_url,
            status=status,
            exit_value=body.get("exitValue"),
        )
        if not result.succeeded:
            reason = body.get("failureReason") or body.get("error") or "no reason returned"
            raise FabricError(f"Notebook {name!r} {status}: {reason}")
        return result
    raise FabricError(f"Notebook {name!r} did not finish within {int(timeout)} seconds.")
