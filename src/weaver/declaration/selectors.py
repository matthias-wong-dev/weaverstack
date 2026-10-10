"""Expand source selectors into actual logical items."""

from ..errors import BuildError, IdentityError
from .model import ITEM_TYPES, WeaverItemId


def expand_item_selectors(values, *, repository, configured_items=()):
    known = {item.identity for item in repository.items} | set(configured_items)
    from ..catalogue.builtin import BUILTIN_ITEM

    known.discard(BUILTIN_ITEM)
    expanded = {}
    explicit = {}
    for value in values:
        if not isinstance(value, str):
            raise BuildError(
                f"a build item must be a string, got {type(value).__name__}"
            )
        if is_powerbi_selector(value):
            for item in powerbi_items(value, repository=repository, error=BuildError):
                expanded.setdefault(item, str(item))
        elif value in ITEM_TYPES:
            matches = sorted(item for item in known if item.item_type == value)
            if not matches:
                raise BuildError(f"{value}: no items match this selector")
            for item in matches:
                expanded.setdefault(item, str(item))
        else:
            logical = value.partition("=")[0].strip()
            try:
                item = WeaverItemId.parse(logical)
            except IdentityError as exc:
                raise BuildError(str(exc)) from exc
            if item in explicit and explicit[item] != value:
                raise BuildError(f"{item}: conflicting exact item selections")
            explicit[item] = value
            expanded.setdefault(item, value)
    expanded.update(explicit)
    return list(expanded.values())


def is_powerbi_selector(value: str) -> bool:
    """Whether ``value`` is ``PowerBI`` or ``PowerBI/<project>``."""

    return value == "PowerBI" or value.startswith("PowerBI/")


def powerbi_items(value: str, *, repository, error) -> tuple[WeaverItemId, ...]:
    """The items of every Power BI project, or of the one ``value`` names."""

    projects = repository.powerbi_projects
    if value != "PowerBI":
        name = value[len("PowerBI/") :]
        if name not in projects:
            raise error(f"{value}: Power BI project not found")
        projects = {name: projects[name]}
    matches = tuple(
        sorted({item for project in projects.values() for item in project.items})
    )
    if not matches:
        raise error(f"{value}: no items match this selector")
    return matches
