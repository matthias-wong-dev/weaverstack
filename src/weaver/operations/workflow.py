"""Named workflows executed through the public Python operations."""

from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

from ..errors import CommandError, WeaverError

FINDINGS = frozenset({"test", "health"})


@dataclass(frozen=True)
class WorkflowStep:
    command: str
    status: str = "not_run"
    result: Any = None
    error: str | None = None

    def to_mapping(self) -> dict:
        result = self.result
        if hasattr(result, "to_mapping"):
            result = result.to_mapping()
        elif hasattr(result, "as_dict"):
            result = result.as_dict()
        elif is_dataclass(result) and not isinstance(result, type):
            result = asdict(result)
        return {
            "command": self.command,
            "status": self.status,
            "result": result,
            "error": self.error,
        }


@dataclass(frozen=True)
class WorkflowReport:
    name: str
    file: str
    workspace: str
    steps: tuple[WorkflowStep, ...]
    telemetry: dict | None = None

    @property
    def succeeded(self) -> bool:
        return bool(self.steps) and all(
            step.status == "succeeded" for step in self.steps
        )

    @property
    def status(self) -> str:
        return "succeeded" if self.succeeded else "failed"

    def to_mapping(self) -> dict:
        return {
            "name": self.name,
            "file": self.file,
            "workspace": self.workspace,
            "status": self.status,
            "steps": [step.to_mapping() for step in self.steps],
            "telemetry": self.telemetry,
        }


def load_workflow(name, *, file=None):
    import yaml

    path = Path(file or "workflow.yml").expanduser()
    if not path.is_file():
        raise CommandError(f"Workflow file not found: {path}. Check file.")
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise CommandError(f"{path}: {exc}") from exc
    workflows = document.get("workflows") if isinstance(document, dict) else None
    if not isinstance(workflows, dict):
        raise CommandError(f"{path}: workflows must be a mapping of names")
    if name not in workflows:
        raise CommandError(f"{path}: workflow {name!r} not found")
    entries = workflows[name]
    if not isinstance(entries, list) or not entries:
        raise CommandError(f"{path}: workflow {name!r} must be a non-empty list")
    if any(not isinstance(entry, str) or not entry.strip() for entry in entries):
        raise CommandError(f"{path}: each workflow entry must be a command line")
    return [entry.strip() for entry in entries], path


def operation_call(parsed):
    """Bind parsed request values to the native operation, not a CLI handler."""
    import weaver

    name = parsed.command
    function = None
    if name == "initialize":
        name = "initialise"
    if name == "fabric":
        area = parsed.fabric_command
        if area == "environment":
            name = "publish_environment"
        elif area == "notebook":
            name = {"push": "push_notebook", "run": "run_notebook"}[
                parsed.notebook_command
            ]
        elif area == "capacity":
            from ..fabric.capacity import run_capacity_action

            function = run_capacity_action
            name = "capacity"
        else:
            raise CommandError("Name a Fabric management operation")
    if name != "capacity":
        function = getattr(weaver, name, None)
    if not callable(function):
        raise CommandError(f"No public Python operation for {name!r}")
    values = vars(parsed).copy()
    if values.get("extra_items"):
        values["items"] = [*(values.get("items") or []), *values["extra_items"]]
    if name == "health":
        values["inventories"] = not values.get("no_inventory", False)
    if name == "mirror":
        values["mirror"] = values.get("mirror_source")
    if name == "publish_environment":
        values["environment"] = values.get("environment_ref")
    if name == "run_notebook":
        values["wait"] = not values.get("no_wait", False)
    signature = inspect.signature(function)
    for key in (
        "command",
        "fabric_command",
        "environment_command",
        "notebook_command",
        "handler",
        "requires",
        "lakehouses",
        "extra_items",
        "no_inventory",
        "mirror_source",
        "environment_ref",
        "no_wait",
        "json",
        "yes",
        "non_interactive",
        "interactive",
    ):
        values.pop(key, None)
    if "timings" not in signature.parameters:
        values.pop("timings", None)
    accepts_keywords = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    unsupported = {
        key
        for key, value in values.items()
        if value is not None and key not in signature.parameters
    }
    if unsupported and not accepts_keywords:
        raise CommandError(
            f"{name} has no matching Python parameter for "
            + ", ".join(sorted(unsupported))
        )
    kwargs = {
        key: value
        for key, value in values.items()
        if value is not None and (key in signature.parameters or accepts_keywords)
    }
    # Bind now: a malformed later request must not follow earlier mutations.
    try:
        signature.bind(**kwargs)
    except TypeError as exc:
        raise CommandError(f"{name}: {exc}") from exc
    return name, function, kwargs


def workflow(
    name,
    *,
    file=None,
    workspace=None,
    workspace_config=None,
    catalogue=None,
    environment=None,
    timings=False,
    session=None,
) -> WorkflowReport:
    """Execute a named workflow in one Session and return each operation's result.

    Calling this Python operation authorises the authored sequence. Validation
    and Health findings remain in the report and do not stop later commands;
    an execution failure leaves the remaining steps unrun.
    """
    from ..command_parser import build_parser
    from ..commandline import command_words
    from ..sessions.host import use_or_create_session
    from .workspace import operation_workspace

    entries, path = load_workflow(name, file=file)
    parser = build_parser()
    requests = []
    excluded = {
        "session": "a workflow cannot open an interactive prompt",
        "workflow": "a workflow cannot run another workflow",
    }
    for entry in entries:
        words = command_words(entry, excluded=excluded)
        try:
            parsed = parser.parse_args(words)
        except SystemExit as exc:
            raise CommandError(f"{entry!r} is not a valid Weaver command") from exc
        requests.append((parsed, *operation_call(parsed)))
    configured = operation_workspace(
        "workflow",
        workspace=workspace,
        workspace_config=workspace_config,
        catalogue=catalogue,
        environment=environment,
        session=session,
        needs_catalogue=False,
    )
    for parsed, operation, function, kwargs in requests:
        if getattr(parsed, "workspace", None) or getattr(
            parsed, "workspace_config", None
        ):
            child = operation_workspace(
                "workflow",
                workspace=getattr(parsed, "workspace", None),
                workspace_config=getattr(parsed, "workspace_config", None),
                needs_catalogue=False,
            )
            if child != configured:
                raise CommandError(
                    "Workflow commands name different workspace configurations; a workflow runs in one Workspace."
                )
    steps = [WorkflowStep(entry) for entry in entries]
    with use_or_create_session(session, workspace=configured) as opened:
        from ..run import new_workflow_id

        with opened.workflow(new_workflow_id()):
            for index, (parsed, operation, function, kwargs) in enumerate(requests):
                parameters = inspect.signature(function).parameters
                if "session" in parameters:
                    # The shared Session fixes the workspace/configuration. A
                    # command can override catalogue/Environment but not retarget it.
                    kwargs.pop("workspace", None)
                    kwargs.pop("workspace_config", None)
                    kwargs["session"] = opened
                    if operation in {"initialise", "doctor"}:
                        kwargs["workspace"] = configured.workspace
                    if operation != "initialise":
                        for key, value in (
                            ("catalogue", catalogue),
                            ("environment", environment),
                        ):
                            if value is not None and key in parameters:
                                kwargs.setdefault(key, value)
                try:
                    result = function(**kwargs)
                except WeaverError as exc:
                    steps[index] = WorkflowStep(
                        entries[index], "failed", error=str(exc)
                    )
                    break
                succeeded = getattr(
                    result, "succeeded", getattr(result, "is_healthy", True)
                )
                succeeded = succeeded and getattr(result, "returncode", 0) == 0
                steps[index] = WorkflowStep(
                    entries[index], "succeeded" if succeeded else "failed", result
                )
                if not succeeded and operation not in FINDINGS:
                    break
        telemetry = opened.telemetry.to_mapping() if timings else None
    return WorkflowReport(
        name, str(path), configured.workspace, tuple(steps), telemetry
    )
