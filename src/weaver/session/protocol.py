"""The version of the contract a console and a Fabric Environment share.

Weaver runs coarse units of work inside a Fabric session by shipping Python that
imports the *published* wheel. The console and that wheel are therefore two
independently versioned halves of one contract, and they drift the moment a
refactor changes what crosses between them: a payload key, a constructor
signature, the name of a module.

Drift without a version produces the worst possible diagnosis — an
``ImportError`` for a module the reader can see in their checkout, or a
``KeyError`` deep inside deserialization. So every program carries a guard, and a
stale Environment answers with the one sentence that helps:

.. code-block:: text

    this workspace's Weaver speaks protocol 3; this console speaks 4 —
    publish the current wheel with `weaver install`

This is a version check and nothing more. It is not a compatibility layer, and
Weaver does not attempt to speak an older protocol: while the product is
pre-alpha, republishing is a command, not a migration.

Bump :data:`PROTOCOL_VERSION` whenever the shape of what crosses changes — the
program bodies, the payloads they emit, or the core objects those payloads are
parsed back into.
"""

from __future__ import annotations

#: The remote-execution contract this checkout speaks.
PROTOCOL_VERSION = 1

#: The key a guarded program emits when the far side speaks a different one.
PROTOCOL_ERROR = "weaver_protocol_error"


def guarded(body: str, *, version: int = PROTOCOL_VERSION) -> str:
    """``body``, refusing to run against a Weaver that speaks another protocol.

    The import is guarded rather than assumed, because an Environment old enough
    to matter predates this module entirely, and "no protocol" is a protocol
    mismatch rather than a crash.
    """

    indented = "\n".join(f"    {line}" if line.strip() else line for line in body.splitlines())
    return (
        "try:\n"
        "    from weaver.session.protocol import PROTOCOL_VERSION as _weaver_protocol\n"
        "except Exception:\n"
        "    _weaver_protocol = 0\n"
        f"if _weaver_protocol != {version}:\n"
        f"    emit({{{PROTOCOL_ERROR!r}: {{'remote': _weaver_protocol, 'local': {version}}}}})\n"
        "else:\n"
        f"{indented}\n"
    )


def check(payload, *, workspace=None):
    """Raise where ``payload`` is a protocol refusal; otherwise return it."""

    from ..errors import CommandError

    if not isinstance(payload, dict) or PROTOCOL_ERROR not in payload:
        return payload
    mismatch = payload[PROTOCOL_ERROR]
    remote = mismatch.get("remote", 0)
    spoken = "speaks no protocol version" if not remote else f"speaks protocol {remote}"
    where = f" in {workspace}" if workspace else ""
    raise CommandError(
        f"the Weaver published{where} {spoken}; this console speaks protocol "
        f"{mismatch.get('local', PROTOCOL_VERSION)} — publish the current wheel "
        "with `weaver install`"
    )


__all__ = ["PROTOCOL_ERROR", "PROTOCOL_VERSION", "check", "guarded"]
