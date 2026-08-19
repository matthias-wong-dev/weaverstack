"""Read an item's ``shortcuts.py`` and ``external.yml`` declarations.

``shortcuts.py`` is Python syntax used for static declarations. It is parsed,
never executed: a build reads it to learn what an item points at, and the same
names are importable from the item's own programs at load time. So the accepted
syntax is deliberately narrow, and anything that would only have a meaning when
run is refused here rather than ignored.

``external.yml`` is the Warehouse's equivalent, keyed by the destination view.
"""

from __future__ import annotations

import ast
from typing import Iterable, Mapping

import yaml

from ..errors import DiscoveryError
from .model import (
    ExternalDeclaration,
    ShortcutDeclaration,
    WeaverDocumentId,
    WeaverItemId,
)

#: The file each surface is declared in, at the item root.
SHORTCUTS_FILE = "shortcuts.py"
EXTERNAL_FILE = "external.yml"

#: The declaration call an authored ``shortcuts.py`` is written with.
CONSTRUCTOR = "Shortcut"

#: Its parameters, in the order a positional call gives them.
PARAMETERS = ("shortcut_type", "target", "workspace", "bind")

#: What ``external.yml`` accepts under one destination.
EXTERNAL_FIELDS = ("target", "bind")


def _literal(node: ast.AST, *, relative: str, what: str):
    """One authored constant, or a declaration error naming what was written."""

    try:
        return ast.literal_eval(node)
    except ValueError:
        raise DiscoveryError(
            f"{relative}: {what} must be a constant. {SHORTCUTS_FILE} is read "
            "without being run, so write the value out."
        ) from None


def _call(node: ast.AST, *, relative: str, name: str) -> ast.Call:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        raise DiscoveryError(
            f"{relative}: {name} must be assigned a {CONSTRUCTOR}(...) call"
        )
    if node.func.id != CONSTRUCTOR:
        raise DiscoveryError(
            f"{relative}: {name} is assigned {node.func.id}(...), and "
            f"{SHORTCUTS_FILE} declares {CONSTRUCTOR}s only"
        )
    return node


def _arguments(call: ast.Call, *, relative: str, name: str) -> dict:
    """The call's arguments as a mapping, positional and keyword alike."""

    if len(call.args) > len(PARAMETERS):
        raise DiscoveryError(
            f"{relative}: {name} passes more arguments than {CONSTRUCTOR} takes"
        )
    arguments = {
        parameter: _literal(value, relative=relative, what=f"{name}'s {parameter}")
        for parameter, value in zip(PARAMETERS, call.args)
    }
    for keyword in call.keywords:
        if keyword.arg is None:
            raise DiscoveryError(
                f"{relative}: {name} unpacks its arguments. Write each one out."
            )
        if keyword.arg not in PARAMETERS:
            expected = ", ".join(PARAMETERS)
            raise DiscoveryError(
                f"{relative}: {name} names {keyword.arg!r}, and {CONSTRUCTOR} "
                f"takes {expected}"
            )
        if keyword.arg in arguments:
            raise DiscoveryError(f"{relative}: {name} gives {keyword.arg!r} twice")
        arguments[keyword.arg] = _literal(
            keyword.value, relative=relative, what=f"{name}'s {keyword.arg}"
        )
    for required in ("shortcut_type", "target"):
        if required not in arguments:
            raise DiscoveryError(f"{relative}: {name} declares no {required}")
    return arguments


def _declaration_name(node: ast.Assign, *, relative: str) -> str:
    if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
        raise DiscoveryError(
            f"{relative}: each declaration assigns one name, as "
            f"Name = {CONSTRUCTOR}(...)"
        )
    return node.targets[0].id


def read_shortcuts(
    text: str, *, owner: WeaverItemId, relative: str
) -> tuple[ShortcutDeclaration, ...]:
    """Every shortcut one item declares, read without running the file."""

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
                f"{relative}: line {statement.lineno} is "
                f"{type(statement).__name__.lower()}, and {SHORTCUTS_FILE} holds "
                f"{CONSTRUCTOR} declarations, imports and comments only"
            )
        name = _declaration_name(statement, relative=relative)
        call = _call(statement.value, relative=relative, name=name)
        arguments = _arguments(call, relative=relative, name=name)
        prior = seen.get(name.casefold())
        if prior is not None:
            detail = (
                "is declared twice"
                if prior == name
                else f"and {prior} differ only by case and cannot coexist"
            )
            raise DiscoveryError(f"{relative}: {name} {detail}")
        seen[name.casefold()] = name
        try:
            declarations.append(
                ShortcutDeclaration(
                    owner=owner, name=name, relative_path=relative, **arguments
                )
            )
        except Exception as exc:
            raise DiscoveryError(f"{relative}: {exc}") from exc
    return tuple(declarations)


def read_external(
    text: str, *, owner: WeaverItemId, relative: str
) -> tuple[ExternalDeclaration, ...]:
    """Every external view one Warehouse item declares."""

    from .metadata import _UniqueKeyLoader

    try:
        loaded = yaml.load(text, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        raise DiscoveryError(f"{relative}: invalid YAML: {exc}") from exc
    if loaded is None:
        return ()
    if not isinstance(loaded, dict):
        raise DiscoveryError(
            f"{relative} maps each destination view to what it points at"
        )

    declarations: list[ExternalDeclaration] = []
    seen: dict[str, str] = {}
    for raw_destination, body in loaded.items():
        if not isinstance(raw_destination, str):
            raise DiscoveryError(f"{relative}: destinations must be strings")
        try:
            destination = WeaverDocumentId.parse(raw_destination)
        except Exception as exc:
            raise DiscoveryError(f"{relative}: {exc}") from exc
        if destination.item != owner:
            raise DiscoveryError(
                f"{relative}: destination {raw_destination} belongs to "
                f"{destination.item}, and this file declares {owner}'s own views"
            )
        if not isinstance(body, dict):
            raise DiscoveryError(
                f"{relative}: {raw_destination} must carry a target, written as "
                "'target: <ItemType/ItemName/Schema.Object>'"
            )
        unknown = sorted(set(body) - set(EXTERNAL_FIELDS))
        if unknown:
            expected = ", ".join(EXTERNAL_FIELDS)
            raise DiscoveryError(
                f"{relative}: {raw_destination} names {', '.join(unknown)}, and "
                f"an external reference takes {expected}"
            )
        if "target" not in body:
            raise DiscoveryError(f"{relative}: {raw_destination} declares no target")
        folded = str(destination).casefold()
        prior = seen.get(folded)
        if prior is not None:
            raise DiscoveryError(
                f"{relative}: destinations {raw_destination} and {prior} differ "
                "only by case"
            )
        seen[folded] = raw_destination
        try:
            declarations.append(
                ExternalDeclaration(
                    destination=destination,
                    target=body["target"],
                    bind=bool(body.get("bind", False)),
                )
            )
        except Exception as exc:
            raise DiscoveryError(f"{relative}: {exc}") from exc
    return tuple(declarations)


def validate_destinations(
    shortcuts: Iterable[ShortcutDeclaration],
    externals: Iterable[ExternalDeclaration],
    *,
    documents: Mapping[WeaverDocumentId, object],
    schemas_by_item: Mapping[WeaverItemId, Iterable[str]],
) -> None:
    """Hold every declared destination against what else the estate claims.

    A destination may not collide with something the repository already declares,
    and it may not sit inside a schema or folder shortcut. OneLake makes a
    shortcut a read-write window into the item it points at, so a write beneath
    one lands in that item.
    """

    shortcuts = tuple(shortcuts)
    externals = tuple(externals)
    folded_documents = {str(identity).casefold(): identity for identity in documents}

    claimed: dict[str, str] = {}
    for declaration in shortcuts:
        destination = str(declaration.destination)
        native = folded_documents.get(destination.casefold())
        if native is not None:
            raise DiscoveryError(
                f"shortcut {declaration.name} in {declaration.owner} would be "
                f"called {destination}, which the repository already declares "
                f"as {native}"
            )
        prior = claimed.get(destination.casefold())
        if prior is not None:
            raise DiscoveryError(
                f"{declaration.owner} declares {destination} and {prior}, which "
                "name the same destination"
            )
        claimed[destination.casefold()] = destination

    for external in externals:
        destination = str(external.destination)
        native = folded_documents.get(destination.casefold())
        if native is not None:
            raise DiscoveryError(
                f"external reference {destination} collides with the declared "
                f"document {native}"
            )

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
    for external in externals:
        owning = namespaces.get(
            (external.destination.item, external.destination.object_id.schema)
        )
        if owning is not None:
            raise _beneath(str(external.destination), owning)
    for item, declared in schemas_by_item.items():
        for schema in declared:
            owning = namespaces.get((item, schema))
            if owning is not None:
                raise DiscoveryError(
                    f"{item} declares schema {schema!r} and also shortcuts it "
                    f"to {owning.target}. A schema shortcut presents the source "
                    "item's namespace, so the item cannot own objects there too."
                )


def _beneath(name: str, owning: ShortcutDeclaration) -> DiscoveryError:
    return DiscoveryError(
        f"{name} sits inside the schema shortcut {owning.name} in "
        f"{owning.owner}, which points at {owning.target}. Weaver owns the "
        "shortcut and nothing beneath it, because anything written there is "
        "written into the item the shortcut points at."
    )
