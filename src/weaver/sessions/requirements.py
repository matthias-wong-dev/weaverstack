"""Resource requirements declared by commands.

Commands declare possible requirements before execution so a Session can warm
shared resources. Runtime planning decides which resources an operation uses;
declaring a requirement does not acquire it.
"""

from __future__ import annotations

AUTH = "auth"
RESOLVER = "resolver"
ONELAKE = "onelake"
TDS = "tds"
LIVY = "livy"

#: Every requirement a command may declare. Unknown names fail immediately.
REQUIREMENTS = frozenset({AUTH, RESOLVER, ONELAKE, TDS, LIVY})


def requirements(*names: str) -> frozenset[str]:
    declared = frozenset(names)
    unknown = declared - REQUIREMENTS
    if unknown:
        raise ValueError(
            f"Unknown resource requirement(s): {', '.join(sorted(unknown))}. "
            f"Expected any of {', '.join(sorted(REQUIREMENTS))}."
        )
    return declared


def union(*declarations) -> frozenset[str]:
    """The requirements to warm before a command sequence starts.

    This lets a later command's acquisition overlap earlier work.
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
