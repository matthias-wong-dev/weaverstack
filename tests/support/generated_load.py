"""Reading a generated Warehouse load back apart, for tests that assert on it.

``create_load()`` produces an *installer*: a script that reads the built table's
columns and assembles the procedure from a template it carries as a SQL literal.
So a claim about generated SQL belongs to one of two texts, and which one matters.

A claim about *which columns* are named — the loadable set, the comparison set,
the update list — is the installer's, and reads its text directly.

A claim about the *reconciliation* is the procedure's, and every quote in it is
doubled by the literal it travels in. :func:`procedure` undoes that, so such a
claim can be written the way the procedure is written.
"""

from __future__ import annotations

#: Where the installer embeds the procedure it assembles.
_OPENS = "set @weaver_proc_sql = N'"


def procedure(installer: str) -> str:
    """The procedure the installer will create, as the Warehouse will see it."""

    start = installer.index(_OPENS) + len(_OPENS)
    at = start
    # The literal ends at the first quote that is not half of a doubled pair.
    while True:
        at = installer.index("'", at)
        if installer[at + 1] != "'":
            break
        at += 2
    return installer[start:at].replace("''", "'")


__all__ = ["procedure"]
