"""What a command needs, said before it runs.

A Session cannot guess. It does not know what a build is, and it must not learn
— a Session that understood build/load/test semantics would be a second place
deciding what an operation does. So the direction is the other way round:
**a command declares its coarse requirements, and the Session prepares them.**

.. code-block:: text

    weaver load Warehouse/Reporting     → auth, resolver, tds
    weaver load Lakehouse/Sales         → auth, resolver, onelake, livy
    weaver build ./repository           → auth, resolver, onelake, livy, tds

That is enough to start the expensive things once, in the background, before
the first one is wanted — which is the whole reason `weaver session` exists, and
the reason `compose` can take the union of a whole sequence and warm the maximum
set once rather than discovering each resource mid-run.

**Two levels, deliberately.**

*Command requirements* are what parsed arguments already imply. They are coarse
and they are a **superset**: `weaver load Lakehouse/Sales` says Livy may be
needed, because a Lakehouse usually holds Python primitives — not because this
particular estate does.

*Execution requirements* are what the BuildBundle or RunGraph actually turns out
to contain, and only they can be exact. A RunGraph of nothing but Warehouse
procedures needs no Livy however the request was spelled.

Warm-up uses the first; routing uses the second. Which is why nothing below this
may treat a declared requirement as permission to acquire: **preparing is not
using.** A Warehouse-only run declares Livy, warms nothing it does not want, and
must still never open a Spark session — the acquisition stays where the need is
discovered, and this only gives it a head start when it is coming anyway.
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
