"""Carry a known load refusal across a transport that only moves data.

A refusal is a result with an exception around it. Raising it inside Fabric
would reach the desktop as a Livy traceback, and its counts would have to be
read out of the message. This is the one small envelope both boundaries agree
on, so the row crosses as a row.
"""

from __future__ import annotations

from typing import Any, Mapping

from ..errors import LoadError
from .load_result import LoadResult

#: Marks a row that is a refusal rather than a load result. Chosen so it cannot
#: collide with a result column.
REFUSAL_KEY = "__weaver_refusal__"

#: The envelope's own version, independent of the result it carries.
REFUSAL_VERSION = 1


def refusal_envelope(exc: BaseException) -> dict[str, Any] | None:
    """The envelope for a load error carrying a result, or ``None``.

    Anything else is an execution failure and stays one.
    """

    result = getattr(exc, "result", None)
    if not isinstance(exc, LoadError) or not isinstance(result, LoadResult):
        return None
    return {
        REFUSAL_KEY: REFUSAL_VERSION,
        "error_type": type(exc).__name__,
        "message": str(exc),
        "result": result.as_row(),
    }


def refused(row: Any) -> bool:
    return isinstance(row, Mapping) and REFUSAL_KEY in row


def decoded_refusal(row: Mapping[str, Any]) -> LoadError:
    """Rebuild the error a remote entry point returned instead of raising."""

    version = row.get(REFUSAL_KEY)
    if version != REFUSAL_VERSION:
        raise LoadError(
            f"A load refusal arrived in format {version!r}, which this Weaver "
            f"version does not read; it reads {REFUSAL_VERSION}. Publish this "
            "version to the Fabric Environment."
        )
    return LoadError(
        str(row.get("message") or "the load was refused"),
        result=LoadResult.from_row(row["result"]),
    )


__all__ = [
    "REFUSAL_KEY",
    "REFUSAL_VERSION",
    "decoded_refusal",
    "refusal_envelope",
    "refused",
]
