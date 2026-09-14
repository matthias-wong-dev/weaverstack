"""Read Lakehouse ``shortcuts.py`` and Warehouse ``shortcuts.yml`` declarations.

Lakehouse declarations are parsed without execution. Warehouse declarations are
grouped by whether their targets are logical or physical. An item's programs can
import its declared Lakehouse shortcut names.
"""

from __future__ import annotations

import ast
from typing import Iterable, Mapping

import yaml

from ..errors import DiscoveryError
from .model import (
    LOGICAL_TARGET,
    NAME_SEPARATOR,
    PHYSICAL_TARGET,
    TARGET_TYPES,
    VIEW_SHORTCUT,
    ShortcutDeclaration,
    WeaverDocumentId,
    WeaverItemId,
)

LAKEHOUSE_FILE = "shortcuts.py"
WAREHOUSE_FILE = "shortcuts.yml"
SHORTCUT_FILES = (LAKEHOUSE_FILE, WAREHOUSE_FILE)

CONSTRUCTOR = "Shortcut"

# Positional declaration arguments follow this order.
PARAMETERS = ("shortcut_type", "target_type", "target", "workspace")


def _literal(node: ast.AST, *, relative: str, what: str):
    try:
        return ast.literal_eval(node)
    except ValueError:
        raise DiscoveryError(
            f"{relative}: {what} must be a literal value, not an expression. "
            "Write the value directly."
        ) from None


def _call(node: ast.AST, *, relative: str, name: str) -> ast.Call:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        raise DiscoveryError(f"{relative}: {name} must use {CONSTRUCTOR}(...).")
    if node.func.id != CONSTRUCTOR:
        raise DiscoveryError(
            f"{relative}: {name} must use {CONSTRUCTOR}(...), not {node.func.id}(...)."
        )
    return node


def _arguments(call: ast.Call, *, relative: str, name: str) -> dict:
    if len(call.args) > len(PARAMETERS):
        expected = ", ".join(PARAMETERS)
        raise DiscoveryError(
            f"{relative}: {name} passes too many arguments to {CONSTRUCTOR}. "
            f"Use only {expected}."
        )
    arguments = {
        parameter: _literal(value, relative=relative, what=f"{name}'s {parameter}")
        for parameter, value in zip(PARAMETERS, call.args)
    }
    for keyword in call.keywords:
        if keyword.arg is None:
            raise DiscoveryError(
                f"{relative}: {name} cannot unpack arguments with **. "
                "Write each argument directly."
            )
        if keyword.arg not in PARAMETERS:
            expected = ", ".join(PARAMETERS)
            raise DiscoveryError(
                f"{relative}: {CONSTRUCTOR} does not accept {keyword.arg!r}. "
                f"Use only {expected}."
            )
        if keyword.arg in arguments:
            raise DiscoveryError(
                f"{relative}: {name} gives {keyword.arg!r} twice. Remove one value."
            )
        arguments[keyword.arg] = _literal(
            keyword.value, relative=relative, what=f"{name}'s {keyword.arg}"
        )
    for required in ("shortcut_type", "target_type", "target"):
        if required not in arguments:
            raise DiscoveryError(f"{relative}: {name} must provide {required}.")
    return arguments


def _declaration_name(node: ast.Assign, *, relative: str) -> str:
    if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
        raise DiscoveryError(
            f"{relative}: each declaration assigns one name, as "
            f"Name = {CONSTRUCTOR}(...)."
        )
    return node.targets[0].id


def _reject_repeat(seen: dict[str, str], name: str, *, relative: str) -> None:
    prior = seen.get(name.casefold())
    if prior is not None:
        detail = (
            "is declared twice"
            if prior == name
            else f"and {prior} differ only by case and cannot coexist"
        )
        raise DiscoveryError(f"{relative}: {name} {detail}. Rename or remove it.")
    seen[name.casefold()] = name


def read_lakehouse_shortcuts(
    text: str, *, owner: WeaverItemId, relative: str
) -> tuple[ShortcutDeclaration, ...]:
    """Read a Lakehouse's shortcut declarations without running the file."""

    try:
        module = ast.parse(text, filename=relative)
    except SyntaxError as exc:
        raise DiscoveryError(f"{relative}: invalid Python: {exc}") from exc

    declarations: list[ShortcutDeclaration] = []
    seen: dict[str, str] = {}
    for statement in module.body:
        if isinstance(statement, (ast.Import, ast.ImportFrom)):
            continue
        if isinstance(statement, ast.Expr) and isinstance(
            statement.value, ast.Constant
        ):
            # A module docstring, or a string used as a comment.
            continue
        if not isinstance(statement, ast.Assign):
            raise DiscoveryError(
                f"{relative}: line {statement.lineno} is not a shortcut declaration. "
                f"Use Name = {CONSTRUCTOR}(...); only imports and comments may appear "
                "beside declarations."
            )
        name = _declaration_name(statement, relative=relative)
        call = _call(statement.value, relative=relative, name=name)
        arguments = _arguments(call, relative=relative, name=name)
        _reject_repeat(seen, name, relative=relative)
        try:
            declarations.append(
                ShortcutDeclaration(
                    owner=owner, name=name, relative_path=relative, **arguments
                )
            )
        except Exception as exc:
            raise DiscoveryError(f"{relative}: {exc}") from exc
    return tuple(declarations)


def read_warehouse_shortcuts(
    text: str, *, owner: WeaverItemId, relative: str
) -> tuple[ShortcutDeclaration, ...]:
    """Read a Warehouse's logical and physical shortcut declarations."""

    from .metadata import _UniqueKeyLoader

    try:
        loaded = yaml.load(text, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise DiscoveryError(f"{relative}: invalid YAML: {exc}") from exc
    if loaded is None:
        return ()
    if not isinstance(loaded, dict):
        raise DiscoveryError(
            f"{relative}: expected {LOGICAL_TARGET} and {PHYSICAL_TARGET} sections. "
            "Map each destination view to its target."
        )
    unknown = sorted(str(section) for section in set(loaded) - set(TARGET_TYPES))
    if unknown:
        expected = ", ".join(TARGET_TYPES)
        raise DiscoveryError(
            f"{relative}: unknown section(s): {', '.join(unknown)}. "
            f"Use only {expected}."
        )

    declarations: list[ShortcutDeclaration] = []
    seen: dict[str, str] = {}
    for target_type in TARGET_TYPES:
        entries = loaded.get(target_type)
        if entries is None:
            continue
        if not isinstance(entries, dict):
            raise DiscoveryError(
                f"{relative}: the {target_type} section must map each destination "
                "view to its target."
            )
        for raw_destination, raw_target in entries.items():
            if not isinstance(raw_destination, str) or not isinstance(raw_target, str):
                raise DiscoveryError(
                    f"{relative}: destinations and targets must be strings. "
                    "Write both as quoted YAML values."
                )
            try:
                destination = WeaverDocumentId.parse(raw_destination)
            except Exception as exc:
                raise DiscoveryError(f"{relative}: {exc}") from exc
            if destination.item != owner:
                raise DiscoveryError(
                    f"{relative}: destination {raw_destination} belongs to "
                    f"{destination.item}, not {owner}. Move it to that item's "
                    "shortcuts file or change the destination."
                )
            identity = destination.object_id
            name = f"{identity.schema}{NAME_SEPARATOR}{identity.object}"
            _reject_repeat(seen, name, relative=relative)
            try:
                declarations.append(
                    ShortcutDeclaration(
                        owner=owner,
                        name=name,
                        shortcut_type=VIEW_SHORTCUT,
                        target_type=target_type,
                        target=raw_target,
                        relative_path=relative,
                    )
                )
            except Exception as exc:
                raise DiscoveryError(f"{relative}: {exc}") from exc
    return tuple(declarations)


def validate_destinations(
    shortcuts: Iterable[ShortcutDeclaration],
    *,
    documents: Mapping[WeaverDocumentId, object],
    schemas_by_item: Mapping[WeaverItemId, Iterable[str]],
) -> None:
    """Reject shortcut destinations that overlap project-owned names.

    A schema shortcut owns its destination namespace; writes below it land in
    the source item.
    """

    shortcuts = tuple(shortcuts)
    folded_documents = {str(identity).casefold(): identity for identity in documents}

    claimed: dict[str, str] = {}
    for declaration in shortcuts:
        destination = str(declaration.destination)
        native = folded_documents.get(destination.casefold())
        if native is not None:
            raise DiscoveryError(
                f"Shortcut {declaration.name} in {declaration.owner} conflicts with "
                f"project object {native}. Rename or remove the shortcut."
            )
        prior = claimed.get(destination.casefold())
        if prior is not None:
            raise DiscoveryError(
                f"{declaration.owner}: shortcuts {destination} and {prior} have the "
                "same destination. Rename one of them."
            )
        claimed[destination.casefold()] = destination

    # A shortcut's namespace belongs to the item it points at.
    namespaces = {
        (declaration.owner, declaration.schema): declaration
        for declaration in shortcuts
        if declaration.is_schema
    }
    for declaration in shortcuts:
        if declaration.is_schema:
            continue
        owning = namespaces.get((declaration.owner, declaration.schema))
        if owning is not None:
            raise _beneath(str(declaration.destination), owning)
    for identity in documents:
        owning = namespaces.get((identity.item, identity.object_id.schema))
        if owning is not None:
            raise _beneath(str(identity), owning)
    for item, declared in schemas_by_item.items():
        for schema in declared:
            owning = namespaces.get((item, schema))
            if owning is not None:
                raise DiscoveryError(
                    f"{item}: schema {schema!r} conflicts with shortcut {owning.name}, "
                    f"which points to {owning.target}. Remove the schema declaration "
                    "or use a different shortcut destination."
                )


def _beneath(name: str, owning: ShortcutDeclaration) -> DiscoveryError:
    return DiscoveryError(
        f"{name} conflicts with schema shortcut {owning.name} in {owning.owner}, "
        f"which points to {owning.target}. Move or remove {name}, or use a different "
        "shortcut destination."
    )
