"""Strict manifest field and value checks."""

from collections.abc import Mapping
from dataclasses import MISSING, fields

from ..errors import BuildError


def checked_mapping(mapping, cls, *, exclude=(), required=()):
    if not isinstance(mapping, Mapping):
        raise BuildError(f"{cls.__name__} must be a mapping")
    declared = {f.name: f for f in fields(cls) if f.name not in exclude}
    unknown = set(mapping) - declared.keys()
    if unknown:
        raise BuildError(
            f"{cls.__name__} has unknown fields: {sorted(unknown, key=str)!r}"
        )
    needed = set(required) | {
        name
        for name, f in declared.items()
        if f.default is MISSING and f.default_factory is MISSING
    }
    missing = needed - mapping.keys()
    if missing:
        raise BuildError(
            f"{cls.__name__} is missing required fields: {sorted(missing)!r}"
        )
    return mapping


def owned_strings(value, *, what):
    if not isinstance(value, (list, tuple)):
        raise BuildError(f"{what} must be an explicit list")
    result = tuple(value)
    for member in result:
        require_string(member, what=what)
    return result


def require_string(value, *, what, optional=False):
    if value is None and optional:
        return
    if not isinstance(value, str) or not value:
        raise BuildError(f"{what} must be a non-empty string")


def freeze_value(value):
    import math
    from types import MappingProxyType

    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise BuildError("manifest mapping keys must be strings")
        return MappingProxyType(
            {key: freeze_value(member) for key, member in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(freeze_value(member) for member in value)
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    raise BuildError(f"invalid manifest value of type {type(value).__name__}")


def thaw_value(value):
    if isinstance(value, Mapping):
        return {key: thaw_value(member) for key, member in value.items()}
    if isinstance(value, tuple):
        return [thaw_value(member) for member in value]
    return value


def decoded_items(value, decoder):
    if not isinstance(value, (list, tuple)):
        raise BuildError("manifest collection must be an explicit list")
    return tuple(decoder(member) for member in value)


def owned_items(value, cls):
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(member, cls) for member in value
    ):
        raise BuildError(f"collection requires frozen mutation {cls.__name__} values")
    return tuple(value)
