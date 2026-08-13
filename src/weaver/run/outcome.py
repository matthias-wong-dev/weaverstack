"""What one dispatch produced, and *how* it produced it.

The second part is why this exists. A primitive that refuses rows raises when
it was told not to tolerate them, and the exception carries counts that include
the rejections — so the result alone cannot tell a refusal from a tolerated
load. Both have ``succeeded=False`` and ``rows_rejected > 0``:

.. code-block:: text

    tolerated   the valid rows were written, and the refusal was reported
    raised      nothing was written

Keeping the exception is what decides the status without inferring it from the
counts. An exception becomes data here unconditionally: the run records what
happened, and the operation decides what to do about it once the whole graph is
recorded.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import WeaverError
from .result import RunFailure, reports_outcome
from .result import (
    DISPATCH_EXCEPTION,
    ENDPOINT_REFRESH_FAILURE,
    PRIMITIVE_FAILURE,
    PRIMITIVE_REJECTS,
    RESULT_CONTRACT_INVALID,
    error,
    warning,
)
from .resolution import ENDPOINT_REFRESH
from .result import FAILED, SUCCEEDED, SUCCEEDED_WITH_REJECTS


@dataclass(frozen=True)
class Outcome:
    """One dispatch, normalised: a status, a result and what to say about it."""

    status: str
    #: Whatever the primitive reported. Anything that says whether it succeeded
    #: — a load's counts, a validation's judgement, a future operation's own.
    result: object
    messages: tuple = ()
    #: Whether the dispatch threw rather than returning. A failed node that
    #: *raised* produced no judgement at all — which is a different thing from
    #: one that ran and reported failure, and the difference is what tells "the
    #: check could not be evaluated" from "the check found something wrong".
    raised: bool = False


def settle(node, *, returned=None, raised: BaseException | None = None) -> Outcome:
    """The outcome of dispatching one node, however the dispatch ended."""

    if raised is not None:
        return _raised(node, raised)
    # The contract is "it says whether it succeeded", not "it is a LoadResult".
    # A validation returns a judgement about data rather than a count of work,
    # and both are results a run can settle — what is refused is a primitive
    # that returned something which answers neither.
    if not reports_outcome(returned):
        return _malformed(node, returned)
    return Outcome(
        status=_status(returned),
        result=returned,
        messages=_messages(node, returned),
    )


def _raised(node, exc: BaseException) -> Outcome:
    """A dispatch that raised is a failed node whatever it was carrying.

    The target was not modified, so calling it "succeeded with rejects" would
    report rows that were never written.

    Whatever result the failure carried is kept, from whichever error type
    carried it. A load failure carries a load result and a validation failure
    carries a validation result, and substituting one for the other would hand a
    reader counts that do not exist on the thing they asked about.
    """

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
    return Outcome(
        status=FAILED,
        raised=True,
        result=result,
        messages=(
            error(
                _failure_code(node) if named else DISPATCH_EXCEPTION,
                (
                    f"{node.node_id} failed: {exc}"
                    if named
                    else f"{node.node_id} raised {type(exc).__name__}: {exc}"
                ),
                source=node.primitive_kind if named else "run.dispatch",
            ),
        ),
    )


def _malformed(node, returned) -> Outcome:
    """Not an exception, but not a dispatch either.

    Nothing ran to completion, so it must never read as a tolerated rejection.
    """

    return Outcome(
        status=FAILED,
        # Not an exception, but nothing ran to completion either.
        raised=True,
        result=RunFailure(
            f"the primitive returned {type(returned).__name__}, which does not "
            "report whether it succeeded"
        ),
        messages=(
            error(
                RESULT_CONTRACT_INVALID,
                f"{node.node_id} returned {type(returned).__name__}, which does "
                "not report whether it succeeded",
                source=node.primitive_kind,
            ),
        ),
    )


def _status(result) -> str:
    if result.succeeded:
        return SUCCEEDED
    # A primitive that refused rows and was asked to tolerate them wrote the
    # valid ones and *returned* the refusal. That is not a failed step; a step
    # that failed without refusing anything is. A result with no notion of
    # rejected rows — a validation's judgement — simply failed.
    return SUCCEEDED_WITH_REJECTS if getattr(result, "rows_rejected", 0) else FAILED


def _messages(node, result) -> tuple:
    if result.succeeded:
        return ()
    if getattr(result, "rows_rejected", 0):
        return (
            warning(
                PRIMITIVE_REJECTS,
                f"{node.node_id} rejected {result.rows_rejected} row(s): "
                f"{result.error_message}",
                source=node.primitive_kind,
            ),
        )
    return (
        error(
            _failure_code(node),
            f"{node.node_id} reported failure: {result.error_message}",
            source=node.primitive_kind,
        ),
    )


def _failure_code(node) -> str:
    return (
        ENDPOINT_REFRESH_FAILURE
        if node.primitive_kind == ENDPOINT_REFRESH
        else PRIMITIVE_FAILURE
    )


__all__ = ["Outcome", "settle"]
