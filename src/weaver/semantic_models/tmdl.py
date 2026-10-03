"""Import PBIP's TMDL definition into TMSL-shaped dictionaries."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ..errors import ConfigError

_NAME = r"(?:'(?:[^']|'')*'|[^\s=:.]+)"
_OBJECT = re.compile(
    rf"(database|model|table|column|measure|partition|relationship)"
    rf"(?:\s+({_NAME}))?(?:\s*=\s*(.*))?\Z",
    re.IGNORECASE,
)
_REFERENCE = re.compile(rf"({_NAME})\.({_NAME})\Z")
_COLLECTIONS = {
    ("model", "table"): "tables",
    ("model", "relationship"): "relationships",
    ("table", "column"): "columns",
    ("table", "measure"): "measures",
    ("table", "partition"): "partitions",
}
_COMMON = {"description": str}
_PROPERTIES = {
    "database": {"compatibilityLevel": int},
    "model": {
        "culture": str,
        "sourceQueryCulture": str,
        "defaultPowerBIDataSourceVersion": str,
        "discourageImplicitMeasures": bool,
    },
    "table": {"isHidden": bool, "lineageTag": str, "dataCategory": str},
    "column": {
        "dataType": str,
        "sourceColumn": str,
        "formatString": str,
        "summarizeBy": str,
        "isKey": bool,
        "isHidden": bool,
        "lineageTag": str,
    },
    "measure": {"formatString": str, "displayFolder": str, "lineageTag": str},
    "partition": {"mode": str},
    "relationship": {
        "fromColumn": str,
        "toColumn": str,
        "fromCardinality": str,
        "toCardinality": str,
        "crossFilteringBehavior": str,
        "isActive": bool,
    },
    "dataAccessOptions": {"legacyRedirects": bool, "returnErrorValuesAsNull": bool},
}


def _name(value: str) -> str:
    return value[1:-1].replace("''", "'") if value.startswith("'") else value


def _text(value: str) -> str:
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1].replace('""', '"')
    return value


def _put(node: dict, key: str, value: Any, location: str) -> None:
    if key in node:
        raise ConfigError(f"{location}: duplicate TMDL property {key!r}")
    node[key] = value


def _indent(line: str) -> int:
    prefix = line[: len(line) - len(line.lstrip(" \t"))]
    return len(prefix.expandtabs(4))


def _dedent(line: str, width: int) -> str:
    column = 0
    for index, character in enumerate(line):
        if column == width:
            return line[index:]
        if character not in " \t":
            break
        column += 4 - column % 4 if character == "\t" else 1
        if column > width:
            raise ConfigError("TMDL tab crosses an expression indentation boundary")
    if not line.strip():
        return ""
    raise ConfigError("TMDL expression is indented less than its closing fence")


def _expression(
    lines: list[str], index: int, indent: int, *, fenced: bool = False
) -> tuple[str, int]:
    if fenced:
        start = index
        while index < len(lines) and lines[index].strip() != "```":
            index += 1
        if index == len(lines):
            raise ConfigError("TMDL expression has no closing fence")
        prefix = _indent(lines[index])
        expression = "\n".join(_dedent(line, prefix) for line in lines[start:index])
        return expression, index + 1
    collected = []
    while index < len(lines):
        line = lines[index]
        if line.strip() and _indent(line) <= indent:
            break
        collected.append(line)
        index += 1
    nonempty = [line for line in collected if line.strip()]
    if not nonempty:
        raise ConfigError("TMDL expression is empty")
    prefix = min(_indent(line) for line in nonempty)
    return "\n".join(
        _dedent(line, prefix).rstrip() for line in collected
    ).rstrip(), index


def _parse_file(path: Path, database: dict, references: list[str]) -> None:
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    model = database.setdefault("model", {})
    stack: list[tuple[int, str, dict]] = []
    descriptions: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        index += 1
        text = line.strip()
        if not text:
            continue
        location = f"{path}:{index}"
        indent = _indent(line)
        if text.startswith("///"):
            descriptions.append(text[3:].lstrip())
            continue
        if text.startswith("//"):
            continue
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent_kind, parent = (
            (stack[-1][1], stack[-1][2]) if stack else ("model", model)
        )
        if parent_kind == "table reference":
            raise ConfigError(
                f"{location}: unsupported child-bearing TMDL table reference"
            )
        if text.lower().startswith("ref table "):
            references.append(_name(text[len("ref table ") :]))
            stack.append((indent, "table reference", {}))
            continue
        match = _OBJECT.fullmatch(text)
        if match:
            kind, name, value = match.groups()
            kind = kind.lower()
            if kind == "database":
                node = database
                if name:
                    _put(node, "name", _name(name), location)
            elif kind == "model":
                node = model
            else:
                collection = _COLLECTIONS.get((parent_kind, kind))
                if collection is None or name is None:
                    raise ConfigError(f"{location}: unsupported TMDL object {text!r}")
                name = _name(name)
                members = parent.setdefault(collection, [])
                node = next(
                    (n for n in members if n["name"].casefold() == name.casefold()),
                    None,
                )
                if node is None:
                    node = {"name": name}
                    members.append(node)
            if descriptions:
                _put(node, "description", "\n".join(descriptions), location)
                descriptions.clear()
            if value is not None:
                if kind == "partition":
                    if value not in {"m", "calculated"}:
                        raise ConfigError(
                            f"{location}: unsupported TMDL partition source {value!r}"
                        )
                    _put(node, "source", {"type": value}, location)
                elif kind == "measure":
                    if not value or value == "```":
                        value, index = _expression(
                            lines, index, indent + 4, fenced=value == "```"
                        )
                    _put(node, "expression", value, location)
                else:
                    raise ConfigError(
                        f"{location}: unsupported TMDL expression on {kind}"
                    )
            stack.append((indent, kind, node))
            continue
        if text == "dataAccessOptions" and parent_kind == "model":
            _put(parent, "dataAccessOptions", {}, location)
            stack.append((indent, "dataAccessOptions", parent["dataAccessOptions"]))
            continue
        if text.startswith("source =") and parent_kind == "partition":
            value = text.split("=", 1)[1].strip()
            if not value or value == "```":
                value, index = _expression(lines, index, indent, fenced=value == "```")
            _put(parent["source"], "expression", value, location)
            continue
        key, separator, value = text.partition(":")
        supported = {**_COMMON, **_PROPERTIES[parent_kind]}
        native_key = next(
            (k for k in supported if k.casefold() == key.casefold()), None
        )
        if native_key is None:
            raise ConfigError(
                f"{location}: unsupported TMDL property {key!r} on {parent_kind}"
            )
        value = value.strip()
        expected = supported[native_key]
        if expected is bool:
            if not separator:
                value = True
            elif value.lower() in {"true", "false"}:
                value = value.lower() == "true"
            else:
                raise ConfigError(f"{location}: {native_key} requires a boolean")
        elif not separator:
            raise ConfigError(f"{location}: {native_key} requires a value")
        elif expected is int:
            try:
                value = int(value)
            except ValueError as exc:
                raise ConfigError(
                    f"{location}: {native_key} requires an integer"
                ) from exc
        else:
            value = _text(value)
        if parent_kind == "relationship" and native_key in {"fromColumn", "toColumn"}:
            assert isinstance(value, str)
            reference = _REFERENCE.fullmatch(value)
            if reference is None:
                raise ConfigError(f"{location}: expected a TMDL Table.Column reference")
            table, column = map(_name, reference.groups())
            _put(parent, native_key.replace("Column", "Table"), table, location)
            value = column
        _put(parent, native_key, value, location)


def import_model_folder(folder: Path) -> dict:
    definition = Path(folder) / "definition"
    if not definition.is_dir():
        raise ConfigError(f"{folder}: semantic model has no TMDL definition folder")
    database: dict = {"model": {}}
    references: list[str] = []
    paths = sorted(definition.rglob("*.tmdl"))
    for path in paths:
        _parse_file(path, database, references)
    tables = database["model"].get("tables", [])
    if references:
        named = {t["name"]: t for t in tables}
        if len(set(references)) != len(references) or any(
            n not in named for n in references
        ):
            raise ConfigError(f"{folder}: duplicate or unresolved TMDL table reference")
        database["model"]["tables"] = [named[n] for n in references] + [
            table for table in tables if table["name"] not in references
        ]
    return database


def import_pbip(path: Path) -> dict:
    path = Path(path)
    shortcut = json.loads(path.read_text(encoding="utf-8-sig"))
    models = set()
    for artifact in shortcut["artifacts"]:
        report = path.parent / artifact["report"]["path"]
        definition = json.loads(
            (report / "definition.pbir").read_text(encoding="utf-8-sig")
        )
        reference = definition["datasetReference"]
        if "byPath" not in reference:
            raise ConfigError(f"{path}: PBIP must reference a local semantic model")
        models.add((report / reference["byPath"]["path"]).resolve())
    if len(models) != 1:
        raise ConfigError(f"{path}: PBIP must reference exactly one semantic model")
    return import_model_folder(models.pop())
