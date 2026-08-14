"""Coarse resource requirements declared by commands.

Commands declare possible requirements before execution so a Session can warm
shared resources. Runtime planning decides which resources an operation uses;
declaring a requirement does not acquire it.
"""

from __future__ import annotations

#: A Fabric credential. Everything that crosses needs one; the emulator needs none.
AUTH = "auth"
#: Turning a logical name into a physical item, over REST or locally.
RESOLVER = "resolver"
#: The file transport — reading and writing a Lakehouse's Files area.
ONELAKE = "onelake"
#: A connection to a Warehouse, per Warehouse.
TDS = "tds"
#: Spark: a Livy session from a desktop, the in-process one everywhere else.
LIVY = "livy"

#: Every requirement a command may declare. Named here so a typo in a command's
#: declaration is a failure rather than a requirement silently nobody honours.
REQUIREMENTS = frozenset({AUTH, RESOLVER, ONELAKE, TDS, LIVY})


def requirements(*names: str) -> frozenset[str]:
    """One command's declaration, checked against the vocabulary."""

    declared = frozenset(names)
    unknown = declared - REQUIREMENTS
    if unknown:
        raise ValueError(
            f"unknown resource requirement(s): {', '.join(sorted(unknown))}; "
            f"expected some of {', '.join(sorted(REQUIREMENTS))}"
        )
    return declared


def union(*declarations) -> frozenset[str]:
    """The maximum set a sequence of commands will want between them.

    What `compose` warms before the first command runs: a sequence that ends in
    a load should not wait for Livy at the end of the build in front of it.
    """

    combined: set[str] = set()
    for declaration in declarations:
        combined |= set(declaration or ())
    return frozenset(combined)


__all__ = [
    "AUTH",
    "LIVY",
    "ONELAKE",
    "REQUIREMENTS",
    "RESOLVER",
    "TDS",
    "requirements",
    "union",
]
