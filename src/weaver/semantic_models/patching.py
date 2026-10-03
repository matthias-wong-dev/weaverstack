"""Patch known Weaver-owned generated objects without rewriting their neighbours."""

from .compiler import _COMMON, _SCHEMAS, escape


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
