"""Select objects by regular expressions over their names.

Each pattern must match a whole name, ignoring case, as identifiers in Fabric
do. Every pattern must select something.
"""

from __future__ import annotations

import re
from typing import Callable, Iterable, Sequence, TypeVar

T = TypeVar("T")


def name_patterns(
    names: str | Iterable[str] | None, *, error: type[Exception]
) -> tuple[tuple[str, re.Pattern], ...]:
    """Each written name and its compiled pattern, in the order given."""

    if names is None:
        return ()
    values = (names,) if isinstance(names, str) else tuple(names)
    found = []
    for written in values:
        text = str(written).strip()
        if not text:
            raise error("a name must be a non-empty regular expression")
        try:
            found.append((text, re.compile(text, re.IGNORECASE)))
        except re.error as exc:
            raise error(f"'{text}' is not a valid regular expression: {exc}") from None
    return tuple(found)


def matching(
    patterns: Sequence[tuple[str, re.Pattern]],
    candidates: Sequence[T],
    *,
    name: Callable[[T], str],
    unmatched: Callable[[str], Exception],
) -> tuple[T, ...]:
    """The candidates any pattern matches, in candidate order.

    A pattern that matches nothing raises ``unmatched(pattern)``.
    """

    chosen = set()
    for text, pattern in patterns:
        found = {
            index
            for index, candidate in enumerate(candidates)
            if pattern.fullmatch(name(candidate))
        }
        if not found:
            raise unmatched(text)
        chosen |= found
    return tuple(candidates[index] for index in sorted(chosen))


__all__ = ["matching", "name_patterns"]
