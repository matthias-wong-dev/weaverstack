"""Live views over native TMDL objects.

An object is an editor and a path. Properties are read from and written to the
addressed spans by their native TMDL names; nothing here lists which objects or
properties exist. Unchanged bytes stay unchanged.
"""

from __future__ import annotations

import re

from ..errors import ConfigError
from .fragments import expression_text, scalar
from .render import expression_lines
from .tmdl import (
    PackageEditor,
    content_end,
    folded,
    quote_name,
    root_file,
    text_value,
)

# The property a header assignment sets, as in `measure Revenue = SUM(...)`.
HEADER_PROPERTY = {
    "measure": "expression",
    "column": "expression",
    "expression": "expression",
    "partition": "sourceType",
    "annotation": "value",
    "tablepermission": "filterExpression",
    "calculationitem": "expression",
    "extendedproperty": "value",
}

# Plural attributes that name a collection even when it has no members yet.
# Any other plural names a collection once a member of that kind is declared.
_COLLECTIONS = frozenset(
    {
        "annotations",
        "calculationItems",
        "columnPermissions",
        "columns",
        "cultures",
        "dataSources",
        "expressions",
        "extendedProperties",
        "functions",
        "hierarchies",
        "levels",
        "measures",
        "members",
        "partitions",
        "perspectives",
        "relationships",
        "roles",
        "tablePermissions",
        "tables",
    }
)
_BARE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_DOUBLE_QUOTED = re.compile(r'"(?:[^"]|"")*"')
_SINGLE_QUOTED = re.compile(r"'(?:[^']|'')*'")
_PROPERTY = re.compile(r"(?P<name>[^\s:=]+)\s*(?P<op>[:=])?\s*(?P<rest>.*)\Z")


def _singular(plural):
    return plural[:-3] + "y" if plural.endswith("ies") else plural[:-1]


def _name_token(name):
    return name if _BARE_NAME.match(name) else quote_name(name)


def _scalar(text):
    value = text.strip()
    if value.casefold() in {"true", "false"}:
        return value.casefold() == "true"
    if _DOUBLE_QUOTED.fullmatch(value) or _SINGLE_QUOTED.fullmatch(value):
        return scalar(value)
    return value


class TmdlDefinition:
    """An editable semantic model definition: TMDL parts keyed by path."""

    def __init__(self, parts):
        self._editor = parts if isinstance(parts, PackageEditor) else None
        if self._editor is None:
            self._editor = PackageEditor(parts)

    @property
    def model(self):
        return TmdlObject(self._editor, ())

    @property
    def parts(self):
        return dict(self._editor.parts)


class TmdlObject:
    """One native TMDL object, addressed by kind and name from the model down.

    `obj.dataType`, `obj["dataType"]` and `obj.dataType = "int64"` use native
    property names. A missing property reads as None. Plural names such as
    `tables` and `columns` are collections of child objects.
    """

    __slots__ = ("_editor", "_path")

    def __init__(self, editor, path):
        object.__setattr__(self, "_editor", editor)
        object.__setattr__(self, "_path", tuple(path))

    @property
    def name(self):
        return self._declaration()[1].name if self._path else "Model"

    @property
    def parent(self):
        return TmdlObject(self._editor, self._path[:-1]) if self._path else None

    def objects(self, kind):
        """Child objects of any native kind, including ones Weaver does not know."""

        return TmdlCollection(self, kind)

    def set_expression(self, key, expression):
        """Set an expression-valued property, written as `key = ...`."""

        self._editor.journal.append(("set", self._path, key, expression))
        if self[key] != expression:
            self._editor.expression(self._path, key, expression)

    def remove(self):
        """Delete this object and every reference to it."""

        if not self._path:
            raise ConfigError("The model cannot be removed")
        self._editor.journal.append(("remove", self._path))
        self._editor.remove(self._path)

    @property
    def _kind(self):
        return self._path[-1][0].casefold() if self._path else "model"

    def _declaration(self):
        found = self._editor.locations(self._path)
        if not found:
            raise ConfigError(f"TMDL {self!r} is not declared")
        return found[0]

    def _header_property(self, key):
        default = HEADER_PROPERTY.get(self._kind)
        return default is not None and default.casefold() == key.casefold()

    def __getitem__(self, key):
        if key.casefold() == "description":
            document, node = self._declaration()
            if node.description_start == node.header:
                return None
            return "\n".join(
                line.strip()[3:].lstrip(" ")
                for line in document.lines[node.description_start : node.header]
            )
        if self._header_property(key):
            document, node = self._declaration()
            if node.value is not None:
                return expression_text(document, node)
        matches = self._editor._property_spans(self._path, key)
        if not matches:
            return None
        document, node = matches[0]
        match = _PROPERTY.match(node.text)
        if match["op"] == "=":
            return expression_text(document, node)
        if match["op"] == ":":
            return _scalar(match["rest"])
        if node.children:
            return TmdlObject(self._editor, self._path + (("opaque", node.name),))
        return True

    def __setitem__(self, key, value):
        if key.casefold() == "name":
            raise ConfigError(f"TMDL {self!r}: renaming an object is unsupported")
        if value is None:
            del self[key]
            return
        if isinstance(value, float):
            value = repr(value)
        expression = self._header_property(key)
        if expression:
            key = HEADER_PROPERTY[self._kind]
        elif key.casefold() != "description":
            matches = self._editor._property_spans(self._path, key)
            if matches:
                node = matches[0][1]
                op = _PROPERTY.match(node.text)["op"]
                if op is None and node.children:
                    raise ConfigError(
                        f"TMDL {self!r}/{key} is a block property; edit its members"
                    )
                key, expression = node.name, op == "="
        self._editor.journal.append(("set", self._path, key, value))
        if self[key] == value:
            return
        if expression and self._kind == "partition" and key == "sourceType":
            document, node = self._declaration()
            line = document.lines[node.header]
            head = node.text.partition("=")[0].rstrip()
            ending = line[len(line.rstrip("\r\n")) :]
            self._editor.parts[document.path] = document.replace(
                node.header, node.header + 1, f"{node.prefix}{head} = {value}{ending}"
            )
        elif expression:
            self._editor.expression(self._path, key, value)
        else:
            self._editor.property(
                self._path, key, str(value) if key == "description" else value
            )

    def __delitem__(self, key):
        self._editor.journal.append(("unset", self._path, key))
        if key.casefold() == "description":
            document, node = self._declaration()
            self._editor.parts[document.path] = document.replace(
                node.description_start, node.header, ""
            )
            return
        self._editor.remove_property(self._path, key)

    def __getattr__(self, key):
        if key.startswith("_"):
            raise AttributeError(key)
        if key in _COLLECTIONS:
            return TmdlCollection(self, _singular(key))
        value = self[key]
        if value is None and key.endswith("s"):
            collection = TmdlCollection(self, _singular(key))
            if len(collection):
                return collection
        return value

    def __setattr__(self, key, value):
        if key.startswith("_"):
            raise AttributeError(key)
        self[key] = value

    def __delattr__(self, key):
        del self[key]

    def __eq__(self, other):
        return (
            isinstance(other, TmdlObject)
            and other._editor is self._editor
            and folded(other._path) == folded(self._path)
        )

    def __hash__(self):
        return hash(folded(self._path))

    def __repr__(self):
        return (
            "/".join(f"{kind} {_name_token(name)}" for kind, name in self._path)
            or "model"
        )


class TmdlCollection:
    """Declared child objects of one kind; lookup by name ignores case."""

    __slots__ = ("_owner", "_kind")

    def __init__(self, owner, kind):
        self._owner = owner
        self._kind = kind

    def _names(self):
        return self._owner._editor.children(self._owner._path, self._kind)

    def _object(self, name):
        return TmdlObject(
            self._owner._editor, self._owner._path + ((self._kind, name),)
        )

    def __iter__(self):
        return iter([self._object(name) for name in self._names()])

    def __len__(self):
        return len(self._names())

    def __contains__(self, name):
        return any(n.casefold() == name.casefold() for n in self._names())

    def __getitem__(self, name):
        for declared in self._names():
            if declared.casefold() == name.casefold():
                return self._object(declared)
        raise KeyError(f"{self._owner!r} has no {self._kind} {name!r}")

    def __delitem__(self, name):
        self[name].remove()

    def add(self, name, expression=None, *, description=None, **properties):
        """Declare a new object; `expression` is its header assignment."""

        editor = self._owner._editor
        if name in self:
            raise ConfigError(f"{self._owner!r} already declares {self._kind} {name!r}")
        header = f"{self._kind} {_name_token(name)}"
        path = self._owner._path
        filename = root_file(self._kind, name) if not path else None
        if filename is not None:
            newline = "\r\n" if b"\r\n" in editor.parts.get(filename, b"") else "\n"
            prefix, unit = "", "\t"
        else:
            located = editor.locations(path)
            if not located:
                raise ConfigError(f"TMDL {self._owner!r} is not declared")
            document, parent = located[0]
            newline = document.newline
            prefix = document.child_prefix(parent)
            unit = prefix[len(parent.prefix) :] or "\t"
        text = (
            "".join(
                prefix + "/// " + line + newline
                for line in (description or "").split("\n")
            )
            if description is not None
            else ""
        )
        if expression is None:
            text += prefix + header + newline
        elif "\n" in expression or expression != expression.strip() or not expression:
            text += expression_lines(header, expression, prefix, unit, newline)
        else:
            text += prefix + header + " = " + expression + newline
        for key, value in properties.items():
            if isinstance(value, float):
                value = repr(value)
            text += f"{prefix}{unit}{key}: {text_value(value)}{newline}"
        if filename is not None:
            previous = editor.parts.get(filename, b"")
            editor.parts[filename] = (
                previous + (newline.encode() if previous else b"") + text.encode()
            )
        else:
            document, parent = editor.locations(path)[0]
            position = content_end(parent)
            gap = newline if any(c.kind for c in parent.children) else ""
            editor.parts[document.path] = document.replace(
                position, position, gap + text
            )
        editor.journal.append(("add", path + ((self._kind, name),)))
        return self._object(name)

    def __repr__(self):
        return f"{self._owner!r}/{self._kind}s"
