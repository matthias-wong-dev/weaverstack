"""What a run requires of a primitive's answer, and what it raises when it cannot.

The contract is one sentence:

    a result reports whether it succeeded

That is all a Runner needs, and deliberately all it asks. A load returns counts
of work; a validation returns a judgement about data; a semantic-model refresh
will return something else again. Requiring any of them to be the others' type
would mean adding a runtime operation meant importing another operation's
vocabulary into the Runner — which is the thing this package exists not to do.

So there is no protocol hierarchy here, because there is nothing to arrange:
:func:`reports_outcome` is the whole check, and :class:`RunFailure` is what
stands in when a primitive gave no answer at all.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import WeaverError


class RunError(WeaverError):
    """A run could not proceed.

    The runtime's own error, so a run raises without reaching for a load's
    vocabulary. ``result`` carries whatever the failure was holding — a load's
    counts, a validation's judgement — because a reader handed only an exception
    has to go and ask the estate what it was, and that is what they came for.
    """

    def __init__(self, message: str, *, result: object | None = None) -> None:
        super().__init__(message)
        self.result = result


def reports_outcome(result: object) -> bool:
    """Whether this is something a run can settle: does it say if it succeeded?"""

    return hasattr(result, "succeeded")


@dataclass(frozen=True)
class RunFailure:
    """A failure a primitive did not describe, in the shape a result has.

    Used where nothing came back that could report an outcome — a dispatch that
    threw without carrying a result, or one that returned something else
    entirely. Deliberately minimal: inventing counts here would put numbers in a
    report that nothing measured.
    """

    error_message: str
    succeeded: bool = False

    def as_row(self) -> dict:
        return {"succeeded": False, "error_message": self.error_message}


__all__ = ["RunError", "RunFailure", "reports_outcome"]
