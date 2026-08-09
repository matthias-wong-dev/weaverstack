"""What one dispatch produced, and *how* it produced it.

The second part is the whole reason this exists. A primitive that refuses rows
raises when it was told not to tolerate them, and the exception carries a result
whose counts include the rejections — so a reader looking only at the result
cannot tell a refusal from a tolerated load. Both have ``succeeded=False`` and
``rows_rejected > 0``, and they mean opposite things:

.. code-block:: text

    tolerated   the valid rows were written, and the refusal was reported
    raised      nothing was written

Keeping the exception is what lets the status be decided without inferring
anything from the counts — and inferring from the counts is exactly how a run
that wrote nothing comes to report "succeeded with rejects".

Everything an exception becomes is *data* here, unconditionally, whatever
fault tolerance says: the run has to record what happened before it decides what
to do about it. Deciding is the operation's, once the whole graph is recorded.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import LoadError
from ..load_report import (
    DISPATCH_EXCEPTION,
    ENDPOINT_REFRESH_FAILURE,
    PRIMITIVE_FAILURE,
    PRIMITIVE_REJECTS,
    RESULT_CONTRACT_INVALID,
    error,
    warning,
)
from ..runtime.load_result import LoadResult
from .resolution import ENDPOINT_REFRESH
from .result import FAILED, SUCCEEDED, SUCCEEDED_WITH_REJECTS


@dataclass(frozen=True)
class Outcome:
    """One dispatch, normalised: a status, a result and what to say about it."""

    status: str
    result: LoadResult
    messages: tuple = ()


def settle(node, *, returned=None, raised: BaseException | None = None) -> Outcome:
    """The outcome of dispatching one node, however the dispatch ended."""

    if raised is not None:
        return _raised(node, raised)
    # The contract is "it says whether it succeeded", not "it is a LoadResult".
    # A validation returns a judgement about data rather than a count of work,
    # and both are results a run can settle — what is refused is a primitive
    # that returned something which answers neither.
    if not hasattr(returned, "succeeded"):
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
    """

    if isinstance(exc, LoadError):
        carried = getattr(exc, "result", None)
        return Outcome(
            status=FAILED,
            result=(
                carried
                if isinstance(carried, LoadResult)
                else LoadResult.failure(str(exc))
            ),
            messages=(
                error(
                    _failure_code(node),
                    f"{node.node_id} failed: {exc}",
                    source=node.primitive_kind,
                ),
            ),
        )
    return Outcome(
        status=FAILED,
        result=LoadResult.failure(f"{type(exc).__name__}: {exc}"),
        messages=(
            error(
                DISPATCH_EXCEPTION,
                f"{node.node_id} raised {type(exc).__name__}: {exc}",
                source="run.dispatch",
            ),
        ),
    )


def _malformed(node, returned) -> Outcome:
    """Not an exception, but not a dispatch either.

    Nothing ran to completion, so it must never read as a tolerated rejection.
    """

    return Outcome(
        status=FAILED,
        result=LoadResult.failure(
            f"the primitive returned {type(returned).__name__}, not a load result"
        ),
        messages=(
            error(
                RESULT_CONTRACT_INVALID,
                f"{node.node_id} returned {type(returned).__name__} rather than "
                "a load result",
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
