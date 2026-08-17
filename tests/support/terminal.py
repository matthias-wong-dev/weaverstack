"""A small VT100 screen, for asserting what a session left on a terminal.

A carriage return puts the cursor back at column 0, so what is written next
lands on top of what is already there. Reading the captured stream as text
would show both, which is the class of defect this models.

Only the sequences Weaver's renderers and ``prompt_toolkit`` emit are
interpreted: cursor movement, erase-in-line and erase-in-display.
"""

from __future__ import annotations

import io
import re

_CONTROL = re.compile(r"\x1b\[([0-9;?]*)([A-Za-z])")


class Terminal(io.StringIO):
    """A capture stream that reports itself as a terminal."""

    encoding = "utf-8"

    def isatty(self) -> bool:
        return True


def screen(text: str) -> list[str]:
    """The lines a terminal would be showing after ``text`` was written to it."""

    return _Screen().write(text).lines()


class _Screen:
    def __init__(self) -> None:
        self._rows: list[list[str]] = [[]]
        self._row = 0
        self._column = 0

    def lines(self) -> list[str]:
        return ["".join(row).rstrip() for row in self._rows]

    def write(self, text: str) -> "_Screen":
        index = 0
        while index < len(text):
            character = text[index]
            if character == "\r":
                self._column = 0
                index += 1
            elif character == "\n":
                self._down(1)
                self._column = 0
                index += 1
            elif character == "\x1b":
                match = _CONTROL.match(text, index)
                if match is None:
                    index += 1
                    continue
                self._control(match.group(1), match.group(2))
                index = match.end()
            else:
                self._place(character)
                index += 1
        return self

    def _control(self, parameters: str, final: str) -> None:
        if parameters.startswith("?"):
            return  # a mode change; it moves nothing
        count = int(parameters) if parameters.isdigit() else None
        if final == "A":
            self._down(-(count or 1))
        elif final == "B":
            self._down(count or 1)
        elif final == "C":
            self._column += count or 1
        elif final == "D":
            self._column = max(self._column - (count or 1), 0)
        elif final == "K":
            self._truncate()
        elif final == "J":
            if count == 2:
                self._rows = [[]]
                self._row = self._column = 0
            else:
                self._truncate()
                del self._rows[self._row + 1 :]

    def _truncate(self) -> None:
        del self._rows[self._row][self._column :]

    def _down(self, rows: int) -> None:
        self._row = max(self._row + rows, 0)
        while len(self._rows) <= self._row:
            self._rows.append([])

    def _place(self, character: str) -> None:
        row = self._rows[self._row]
        while len(row) <= self._column:
            row.append(" ")
        row[self._column] = character
        self._column += 1


__all__ = ["Terminal", "screen"]
