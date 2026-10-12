"""Invocation execution limits."""

from .errors import CommandError


def validate_concurrency(value: int | None) -> int | None:
    if value is not None and (type(value) is not int or value < 1):
        raise CommandError("concurrency must be a positive integer")
    return value
