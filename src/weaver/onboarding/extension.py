"""Preserve an initialised project's sources while adding selected items."""

from __future__ import annotations

import hashlib
import json

import yaml

from ..errors import CommandError
from .project import WORKFLOW_FILE, WORKSPACE_CONFIG_FILE

RECORD = ".weaver-initialise.json"


def read_project(destination):
    from ..config import load_workspace

    path = destination / WORKSPACE_CONFIG_FILE
    configured = load_workspace(path) if path.is_file() else None
    record = destination / RECORD
    state = json.loads(record.read_text()) if record.is_file() else {}
    if state and (
        state.get("version") != 1 or not isinstance(state.get("files"), dict)
    ):
        raise CommandError(f"Invalid Initialise record: {record}")
    return configured, state


def selections(configured, state, **selected):
    previous = state.get("items", {})
    result = {}
    for field, kind in (
        ("lakehouse", "Lakehouse"),
        ("warehouse", "Warehouse"),
        ("semantic_model", "SemanticModel"),
    ):
        old = previous.get(field)
        if old is None and configured is not None:
            names = [i.item_name for i in configured.targets if i.item_type == kind]
            if len(names) == 1:
                old = names[0]
        value = selected[field]
        if old and value and old != value:
            raise CommandError(
                f"{kind}/{old} is already selected in workspace-config.yml; cannot replace it with {kind}/{value}."
            )
        result[field] = value or old
    return result


def _digest(content):
    return hashlib.sha256(
        content.encode("utf-8") if isinstance(content, str) else content
    ).hexdigest()


def extend_files(destination, files, request, configured, state):
    from ..initialise import InitialiseError

    old_example = state.get("example")
    if old_example and request.example and old_example != request.example:
        raise InitialiseError(
            f"This project uses {old_example}; cannot add {request.example} to the same example."
        )
    original = dict(files)
    known = state.get("files", {})
    for path in files:
        existing = destination / path
        if path in known and existing.is_file():
            if (
                path in {WORKFLOW_FILE, "README.md"}
                and _digest(existing.read_bytes()) == known[path]
            ):
                continue
            files[path] = existing.read_bytes()
    if configured is not None:
        path = destination / WORKSPACE_CONFIG_FILE
        current = yaml.safe_load(path.read_bytes())
        generated = yaml.safe_load(original[WORKSPACE_CONFIG_FILE])
        targets = current.setdefault("targets", {})
        changed = False
        for field in ("catalogue", "environment"):
            if not current.get(field) and generated.get(field):
                current[field] = generated[field]
                changed = True
        for item, binding in generated.get("targets", {}).items():
            if item not in targets:
                targets[item] = binding
                changed = True
        files[WORKSPACE_CONFIG_FILE] = (
            yaml.safe_dump(current, sort_keys=False) if changed else path.read_bytes()
        )
    # Ownership is recorded only after the candidate passes the ordinary project
    # preflight. A newly introduced colliding path is never adopted as an edit.
    hashes = dict(known)
    for path, content in files.items():
        if path not in known or (
            path in {WORKFLOW_FILE, "README.md"} and files[path] == original[path]
        ):
            hashes[path] = _digest(content)
    record = {
        "version": 1,
        "example": request.example or old_example,
        "items": {
            k: getattr(request, k) for k in ("lakehouse", "warehouse", "semantic_model")
        },
        "files": hashes,
    }
    files[RECORD] = json.dumps(record, indent=2, sort_keys=True) + "\n"
    return files
