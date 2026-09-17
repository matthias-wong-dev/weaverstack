"""Semantic status styling shared by CLI reports."""

from __future__ import annotations

import os
import sys

RESET = "\x1b[0m"
GREEN = "\x1b[32m"
RED = "\x1b[31m"
YELLOW = "\x1b[33m"
DIM = "\x1b[2m"

_GREEN_STATUSES = frozenset({"green", "passed", "succeeded"})
_YELLOW_STATUSES = frozenset(
    {
        "amber",
        "warning",
        "pending",
        "blocked",
        "partially_succeeded",
        "succeeded_with_rejects",
    }
)
_RED_STATUSES = frozenset({"red", "failed", "invalid", "error"})


def colour_enabled(stream) -> bool:
    if "NO_COLOR" in os.environ:
        return False
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


def style(text: str, colour: str, *, stream=None) -> str:
    stream = sys.stdout if stream is None else stream
    return (
        f"{colour}{text}{RESET}" if text and colour and colour_enabled(stream) else text
    )


def semantic_colour(status: str) -> str:
    folded = str(status).casefold()
    if folded in _GREEN_STATUSES:
        return GREEN
    if folded in _YELLOW_STATUSES:
        return YELLOW
    if folded in _RED_STATUSES:
        return RED
    return ""


def status_symbol(status: str) -> str:
    folded = str(status).casefold()
    if folded in {"passed", "succeeded", "succeeded_with_rejects"}:
        return "✓"
    if folded in {"failed", "invalid", "error"}:
        return "✗"
    return " "


__all__ = [
    "DIM",
    "GREEN",
    "RED",
    "RESET",
    "YELLOW",
    "colour_enabled",
    "semantic_colour",
    "status_symbol",
    "style",
]
