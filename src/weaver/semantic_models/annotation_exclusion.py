"""Remove explicitly excluded source spans and their requested-value entries."""

import copy
from dataclasses import replace

from .patching import _pointer
from .tmdl import Document


def _fold(path):
    return tuple((kind.casefold(), name.casefold()) for kind, name in path)


def _without(requested, path):
    result = copy.deepcopy(requested)
    kind, name = path[0]
    collection = {"table": "tables", "column": "columns"}[kind]
    members = []
    for value in result.get(collection, []):
        if value["name"].casefold() != name.casefold():
            members.append(value)
        elif len(path) > 1:
            members.append(_without(value, path[1:]))
    if members:
        result[collection] = members
    else:
        result.pop(collection, None)
    return result


def exclude_paths(contribution, paths):
    paths = tuple(sorted(set(tuple(tuple(pair) for pair in path) for path in paths)))
    if not paths:
        return contribution
    wanted = {_fold(path) for path in paths}
    parts = dict(contribution.parts)
    for filename, data in contribution.parts.items():
        if not filename.startswith("definition/") or not filename.endswith(".tmdl"):
            continue
        document = Document(filename, data)
        ranges = sorted(
            (n.description_start, n.end)
            for n in document.spans
            if _fold(n.path) in wanted
        )
        merged = []
        for start, end in ranges:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        if not merged:
            continue
        for start, end in reversed(merged):
            del document.lines[start:end]
        text = "".join(document.lines)
        if text.lstrip("\ufeff").strip():
            parts[filename] = text.encode("utf-8")
        else:
            del parts[filename]
    requested = contribution.requested
    for path in paths:
        requested = _without(requested, path)
    pointers = tuple(_pointer(path) for path in paths)
    removed_tables = {path[0][1].casefold() for path in paths if len(path) == 1}
    return replace(
        contribution,
        parts=parts,
        requested=requested,
        absent=paths,
        owned=tuple(
            owner
            for owner in contribution.owned
            if not any(owner == p or owner.startswith(p + "/") for p in pointers)
        ),
        source_references={
            k: v
            for k, v in contribution.source_references.items()
            if k.casefold() not in removed_tables
        },
        source_bindings={
            k: v
            for k, v in contribution.source_bindings.items()
            if k.casefold() not in removed_tables
        },
    )
