"""Describe the target state a build intends to leave.

Changes are declared beside actions; executor behaviour is not modelled here.
Every physical action is paired with exactly one change by ``action_id``. Pruned
objects keep their identity here because they have no repository node.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ..errors import BuildError

#: What a change does to the target.
ADD = "add"
REMOVE = "remove"
EFFECTS = (ADD, REMOVE)

#: Change kinds map directly to ``TargetInventory`` collections.
SCHEMA = "schema"
TABLE = "table"
VIEW = "view"
FOLDER = "folder"
FOLDER_SCHEMA = "folder_schema"
FILE = "file"
STORED_PROCEDURE = "stored_procedure"
RUNTIME_REFERENCE = "runtime_reference"
OBJECT_KINDS = (
    SCHEMA,
    TABLE,
    VIEW,
    FOLDER,
    FOLDER_SCHEMA,
    FILE,
    STORED_PROCEDURE,
    RUNTIME_REFERENCE,
)

#: Which inventory collection each kind lives in.
_COLLECTION = {
    SCHEMA: "schemas",
    TABLE: "tables",
    VIEW: "views",
    FOLDER: "folders",
    FOLDER_SCHEMA: "folder_schemas",
    FILE: "files",
    STORED_PROCEDURE: "procedures",
    RUNTIME_REFERENCE: "runtime_references",
}


@dataclass(frozen=True, order=True)
class TargetChange:
    """One object a build will add to or remove from a target.

    ``name`` must match inventory spelling, such as ``DWG.Customer`` or
    ``_/Load/lib/dates.py``, because changes are applied to a physical read.
    """

    effect: str
    object_kind: str
    name: str
    #: The action that makes this change.
    action_id: str

    def __post_init__(self) -> None:
        if self.effect not in EFFECTS:
            raise BuildError(
                f"change effect must be one of {', '.join(EFFECTS)}, got {self.effect!r}"
            )
        if self.object_kind not in OBJECT_KINDS:
            raise BuildError(
                f"change object kind must be one of {', '.join(OBJECT_KINDS)}, "
                f"got {self.object_kind!r}"
            )
        if not self.name or not self.action_id:
            raise BuildError("a change needs both a name and the action producing it")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "effect": self.effect,
            "object_kind": self.object_kind,
            "name": self.name,
            "action_id": self.action_id,
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "TargetChange":
        return cls(
            effect=str(mapping["effect"]),
            object_kind=str(mapping["object_kind"]),
            name=str(mapping["name"]),
            action_id=str(mapping["action_id"]),
        )


def added(object_kind: str, name: str, action_id: str) -> TargetChange:
    return TargetChange(ADD, object_kind, name, action_id)


def removed(object_kind: str, name: str, action_id: str) -> TargetChange:
    return TargetChange(REMOVE, object_kind, name, action_id)


def merge(
    *sections: Mapping[str, Iterable[TargetChange]],
) -> dict[str, tuple[TargetChange, ...]]:
    """Fold several target-keyed change sets into one, preserving order."""

    merged: dict[str, list[TargetChange]] = {}
    for section in sections:
        for target_id, changes in section.items():
            merged.setdefault(target_id, []).extend(changes)
    return {target_id: tuple(changes) for target_id, changes in merged.items()}


def apply_to(inventory, changes: Iterable[TargetChange]):
    """Return a new inventory with these changes applied.

    Folder additions imply their schema; whole-schema removals remain explicit.
    """

    from dataclasses import replace

    collections = {
        field: list(getattr(inventory, field)) for field in set(_COLLECTION.values())
    }
    for change in changes:
        collection = collections[_COLLECTION[change.object_kind]]
        folded = {value.casefold() for value in collection}
        if change.effect == ADD:
            if change.name.casefold() not in folded:
                collection.append(change.name)
        else:
            collection[:] = [
                value
                for value in collection
                if value.casefold() != change.name.casefold()
            ]
            if change.object_kind == FOLDER_SCHEMA:
                # Removing an area takes what is inside it with it, exactly as
                # deleting the directory does.
                prefix = f"{change.name.casefold()}."
                collections["folders"] = [
                    value
                    for value in collections["folders"]
                    if not value.casefold().startswith(prefix)
                ]

    implied = {value.split(".", 1)[0] for value in collections["folders"]}
    for area in sorted(implied):
        if area.casefold() not in {v.casefold() for v in collections["folder_schemas"]}:
            collections["folder_schemas"].append(area)

    return replace(
        inventory,
        **{
            field: tuple(sorted(values, key=str.casefold))
            for field, values in collections.items()
        },
    )
