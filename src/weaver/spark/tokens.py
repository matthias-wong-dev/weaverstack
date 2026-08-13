"""Expand logical object tokens in frozen Spark payloads.

Payloads identify objects logically; installation resolves the selected target's
physical spelling. Unexpanded token syntax is invalid Spark SQL.
"""

from __future__ import annotations

import re

from ..errors import InstallError
from .destination import SparkDestination

#: ``{{object:Schema.Name}}`` — one managed object.
OBJECT = re.compile(r"\{\{object:([^.{}]+)\.([^.{}]+)\}\}")

#: ``{{schema:Name}}`` — one managed schema.
SCHEMA = re.compile(r"\{\{schema:([^.{}]+)\}\}")

#: ``{{epoch}}`` — the instant this installation published its Registry.
#:
#: Scoped to the installation rather than a destination, so :func:`expand` does
#: not resolve it: every statement in a build receives the same value however
#: many destinations they name. :func:`substitute_epoch` runs first, because
#: ``expand`` rejects any token it does not recognise.
#:
#: A token rather than a literal frozen at generation time: a rendered clock
#: would make the same repository produce different payload bytes on every run,
#: and a bundle's identity is its bytes.
EPOCH = re.compile(r"\{\{epoch\}\}")

#: The payload spelling of the publication epoch.
EPOCH_TOKEN = "{{epoch}}"

#: Anything else in token shape. Matched only so an unknown one is reported
#: rather than passed through to the engine as mystery syntax.
ANY = re.compile(r"\{\{[^{}]*\}\}")


def object_token(schema: str, name: str) -> str:
    """The payload spelling of one object."""

    return f"{{{{object:{_part(schema, 'schema')}.{_part(name, 'object name')}}}}}"


def schema_token(schema: str) -> str:
    """The payload spelling of one schema."""

    return f"{{{{schema:{_part(schema, 'schema')}}}}}"


def expand(text: str, destination: SparkDestination) -> str:
    """Every token in ``text``, resolved against one destination.

    An unrecognised token is an error. Leaving it in place would hand Spark
    something it cannot parse — better — or, if the shape ever became valid
    syntax, something that means the wrong thing — far worse.
    """

    text = OBJECT.sub(
        lambda match: destination.qualify(match.group(1), match.group(2)), text
    )
    text = SCHEMA.sub(
        lambda match: destination.qualified_schema(match.group(1)), text
    )
    leftover = ANY.search(text)
    if leftover:
        raise InstallError(
            f"{leftover.group(0)} is not a name this installer knows how to "
            f"resolve against {destination.item!r}"
        )
    return text


def substitute_epoch(text: str, epoch: str | None) -> str:
    """Resolve ``{{epoch}}`` to one installation's publication instant.

    Separate from :func:`expand` because the value is not a destination's
    business: one install writes Registry rows for several items against several
    targets, and they all have to carry the same instant or two rows published
    by one build would order against each other.

    A statement carrying the token when no epoch was supplied is a fault worth
    naming here — the alternative is ``expand`` reporting it as an unresolvable
    name, which says nothing about the missing value.
    """

    if not EPOCH.search(text):
        return text
    if epoch is None:
        raise InstallError(
            "a statement names {{epoch}} but this installation supplied none, so "
            "the row it writes could not be dated"
        )
    return EPOCH.sub(epoch.replace("\\", "\\\\"), text)


def _part(value: str, what: str) -> str:
    """One name part, checked against the delimiters the token is built from."""

    name = (value or "").strip()
    if not name:
        raise ValueError(f"{what} must not be empty")
    for character in (".", "{", "}"):
        if character in name:
            raise ValueError(
                f"{what} must not contain {character!r}, which delimits an "
                f"object token: {value!r}"
            )
    return name
