"""Apply supported addon edits to an effective TMDL package."""

from dataclasses import replace

from .compiler import (
    _COMMON,
    _SCHEMAS,
    _addon_patch,
    _expand_dax,
    _merge,
    escape,
    leaf_properties,
)
from .fragments import source_table
from .references import source_identity
from .render import empty_parts
from .tmdl import PackageEditor


def _pointer(path):
    collections = {
        "table": "tables",
        "column": "columns",
        "measure": "measures",
        "partition": "partitions",
        "expression": "expressions",
        "relationship": "relationships",
        "role": "roles",
        "tablePermission": "tablePermissions",
        "hierarchy": "hierarchies",
        "level": "levels",
        "annotation": "annotations",
    }
    return "/model" + "".join("/" + collections[k] + "/" + escape(n) for k, n in path)


def _patch_object(editor, path, kind, patch, owned):
    if not editor.locations(path):
        editor.add(path, kind, patch)
        owned.add(_pointer(path))
        return
    schema = {**_COMMON, **_SCHEMAS[kind]}
    for key, value in patch.items():
        if key == "name":
            continue
        child = schema.get(key)
        if kind == "partition" and key == "source":
            editor.source(path, value)
        elif key == {
            "measure": "expression",
            "column": "expression",
            "expression": "expression",
            "annotation": "value",
            "tablePermission": "filterExpression",
        }.get(kind):
            editor.expression(path, key, value)
        elif isinstance(child, tuple):
            for member in value:
                _patch_object(
                    editor,
                    path + ((child[0], member["name"]),),
                    child[0],
                    member,
                    owned,
                )
        else:
            editor.property(path, key, value)


def apply_addons(contribution, name, addons):
    editor = PackageEditor(contribution.parts or empty_parts(name))
    owned = set(contribution.owned)
    if not contribution.parts:
        owned.add("/model")
    references = dict(contribution.source_references)
    requested = (
        dict(contribution.requested)
        if contribution.parts
        else {"culture": "en-US", "defaultPowerBIDataSourceVersion": "powerBI_V3"}
    )
    provenance = dict(contribution.provenance)
    for addon, origin in addons:
        if addon is None:
            continue
        patch = _addon_patch(addon)
        for table in patch.get("tables", []):
            if ".source" in table:
                reference = str(source_identity(table.pop(".source")))
                references[table["name"]] = reference
                provenance[f"/model/tables/{escape(table['name'])}/.source"] = {
                    "source": origin,
                    "reason": ".source",
                    "reference": reference,
                }
        dax_tables = {t["name"] for t in patch.get("tables", []) if ".dax" in t}
        _expand_dax(
            patch, {"tables": [source_table(editor.parts, name) for name in dax_tables]}
        )
        _patch_object(editor, (), "model", patch, owned)
        requested = _merge(requested, patch)
        for path in leaf_properties({"model": patch}):
            reason = (
                ".dax"
                if any(
                    path.startswith(f"/model/tables/{escape(t)}/partitions/")
                    for t in dax_tables
                )
                else "overlay"
            )
            provenance[path] = {"source": origin, "reason": reason}
    return replace(
        contribution,
        parts=editor.parts,
        requested=requested,
        provenance=provenance,
        owned=tuple(sorted(owned)),
        source_references=references,
    )
