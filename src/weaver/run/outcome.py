"""Normalise returned and raised dispatch outcomes.

Keep whether dispatch raised: rejected-row counts alone cannot distinguish a
tolerated load that wrote valid rows from a refused load that wrote nothing.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import WeaverError, reported_executor
from .resolution import ENDPOINT_REFRESH
from .result import (
    DISPATCH_EXCEPTION,
    ENDPOINT_REFRESH_FAILURE,
    FAILED,
    PRIMITIVE_FAILURE,
    PRIMITIVE_REJECTS,
    RESULT_CONTRACT_INVALID,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    RunFailure,
    error,
    reports_outcome,
    warning,
)


@dataclass(frozen=True)
class Outcome:
    """A dispatch outcome with its status and evidence."""

    status: str
    result: object
    messages: tuple = ()
    #: Distinguishes a check that could not run from one that found a failure.
    raised: bool = False
    #: Named refusals record as Failed; unexpected dispatch errors record as Error.
    refused: bool = False


def settle(node, *, returned=None, raised: BaseException | None = None) -> Outcome:

    if raised is not None:
        return _raised(node, raised)
    # The contract is "it says whether it succeeded", not "it is a LoadResult".
    # A validation returns a judgement about data rather than a count of work,
    # and both are results a run can settle. What is refused is a primitive that
    # returned something answering neither.
    if not reports_outcome(returned):
        return _malformed(node, returned)
    return Outcome(
        status=status_of(returned),
        result=returned,
        # It ran and reported, so whatever it says is Weaver's own decision.
        refused=True,
        messages=_messages(node, returned),
    )


def _raised(node, exc: BaseException) -> Outcome:
    """Preserve typed evidence from a failure without treating its counts as written."""

    carried = getattr(exc, "result", None)
    result = (
        carried
        if reports_outcome(carried)
        else RunFailure(f"{type(exc).__name__}: {exc}")
    )

    # A failure Weaver named is reported against the primitive that named it;
    # anything else is the dispatch itself coming apart, and saying so is the
    # difference between "the load refused these rows" and "something threw".
    named = isinstance(exc, WeaverError)
    # A raised refusal reads as the returned one does, so an operator sees the
    # same sentence whichever engine reported it.
    refusal = getattr(result, "is_refusal", False)
    return Outcome(
        status=FAILED,
        raised=True,
        refused=named,
        result=result,
        messages=(
            error(
                _failure_code(node) if named else DISPATCH_EXCEPTION,
                (
                    f"{node.node_id} refused the load: {result.error_message}"
                    if named and refusal
                    else f"{node.node_id} failed: {exc}"
                    if named
                    else f"{node.node_id} raised {type(exc).__name__}: {exc}"
                ),
                source=node.primitive_kind if named else "run.dispatch",
                executor=reported_executor(exc),
            ),
        ),
    )


def _malformed(node, returned) -> Outcome:
    return Outcome(
        status=FAILED,
        # Not an exception, but nothing ran to completion either.
        raised=True,
        result=RunFailure(
            f"{type(returned).__name__} does not report whether the primitive succeeded"
        ),
        messages=(
            error(
                RESULT_CONTRACT_INVALID,
                f"Cannot record the result for {node.node_id}: "
                f"{type(returned).__name__} does not report whether it succeeded. "
                "Return a Weaver runtime result.",
                source=node.primitive_kind,
            ),
        ),
    )


def status_of(result) -> str:
    """Map a primitive result to the shared status vocabulary.

    Tolerated rejects preserve the successful writes. A Static skip records that
    no work ran and is not an ordinary success.

    A refusal is read before the reject count, because a gate that refuses a
    load often has rejected rows to report and nothing was written.
    """

    if getattr(result, "is_static_skip", False):
        return SKIPPED
    if result.succeeded:
        return SUCCEEDED
    if getattr(result, "is_refusal", False):
        return FAILED
    return SUCCEEDED_WITH_REJECTS if getattr(result, "rows_rejected", 0) else FAILED


def _messages(node, result) -> tuple:
    if result.succeeded:
        return ()
    if getattr(result, "is_refusal", False):
        return (
            error(
                _failure_code(node),
                f"{node.node_id} refused the load: {result.error_message}",
                source=node.primitive_kind,
            ),
        )
    if getattr(result, "rows_rejected", 0):
        return (
            warning(
                PRIMITIVE_REJECTS,
                f"{node.node_id} rejected {result.rows_rejected} row(s): "
                f"{result.error_message}",
                source=node.primitive_kind,
            ),
        )
    reported = getattr(result, "error_message", None)
    if not reported:
        return ()
    return (
        error(
            _failure_code(node),
            f"{node.node_id} reported failure: {reported}",
            source=node.primitive_kind,
        ),
    )


def _failure_code(node) -> str:
    return (
        ENDPOINT_REFRESH_FAILURE
        if node.primitive_kind == ENDPOINT_REFRESH
        else PRIMITIVE_FAILURE
    )


__all__ = ["Outcome", "settle", "status_of"]
