"""How a frozen payload names an object without naming a destination.

A generated statement has to say *which* object it acts on. It must not say
which Lakehouse, in which workspace, at which path: that is bound by the batch's
target, resolved at install time, and different in every environment. Writing it
into the SQL would make two bundles of the same repository differ in every
payload merely for having been generated somewhere else, which is exactly the
comparison how-does-build-work §15 exists to protect.

So a payload names an object logically, and the executor asks the batch's
destination what that is called there::

    CREATE VIEW {{object:Sales.ActiveCustomer}} AS
    SELECT * FROM {{object:Sales.Customer}} WHERE IsActive

    Fabric   ->  `Weaver`.`Play_Lakehouse_1`.`Sales`.`ActiveCustomer`
    local    ->  `sales_lh__sales`.`ActiveCustomer`

This is substitution of a transport-level value, not a template (§16). Nothing
semantic is left for the installer to decide: the object, its schema, the
statement and the destination are all fixed before the bundle is written — the
only thing supplied late is how that already-chosen destination spells a name.
A reviewer reading the payload sees the object; the manifest's target block says
where it goes. A bare two-part name said neither, and resolved through whatever
the session happened to be attached to.

The tokens are deliberately unmistakable. ``{{`` and ``}}`` are not Spark SQL, so
an unexpanded one is a syntax error at the point of use rather than a name that
quietly resolves somewhere else.
"""

from __future__ import annotations

import re

from ..errors import InstallError
from .destination import SparkDestination

#: ``{{object:Schema.Name}}`` — one managed object.
OBJECT = re.compile(r"\{\{object:([^.{}]+)\.([^.{}]+)\}\}")

#: ``{{schema:Name}}`` — one managed schema.
SCHEMA = re.compile(r"\{\{schema:([^.{}]+)\}\}")

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
