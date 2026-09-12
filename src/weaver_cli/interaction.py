"""Whether this invocation may wait for a person, decided in one place.

Two separate answers, spelled as two options. ``--non-interactive`` is the
execution policy: the invocation reads no stdin, waits for no keypress and
opens no browser sign-in. ``--yes`` is authorisation: it grants permission for
a destructive action. Neither implies the other, so an unattended destructive
command carries both.

Absent ``--non-interactive``, a terminal on stdin is what makes an invocation
interactive. A command handler asks :func:`can_prompt` and never reads
``sys.stdin.isatty()`` itself, so a pseudo-terminal cannot make an explicit
``--non-interactive`` run interactive again.
"""

from __future__ import annotations

import argparse
import sys

#: One spelling, one help text, on every executable command.
NON_INTERACTIVE_HELP = (
    "Never ask. Missing authorisation or a missing answer is an error."
)

#: Retry controls for an interactive task failure.
RETRY_PROMPT = "Enter to retry, Esc to exit."

ESC = "\x1b"
INTERRUPT = "\x03"
END_OF_FILE = "\x04"
ENTER = ("\r", "\n")


def add_non_interactive(parser: argparse.ArgumentParser) -> None:
    """Add the CLI-wide interaction policy option to one command."""

    parser.add_argument(
        "--non-interactive", action="store_true", help=NON_INTERACTIVE_HELP
    )


def non_interactive(args: argparse.Namespace | None) -> bool:
    """Whether this invocation declared the non-interactive policy."""

    return bool(getattr(args, "non_interactive", False))


def can_prompt(args: argparse.Namespace | None, stream=None) -> bool:
    """Whether there is a person on this invocation's input to answer."""

    if non_interactive(args):
        return False
    return _isatty(sys.stdin if stream is None else stream)


def authorised(args: argparse.Namespace | None) -> bool:
    """Whether a destructive command has permission already.

    ``--yes`` on the command line, or a workflow whose sequence was confirmed.
    """

    return bool(getattr(args, "yes", False) or getattr(args, "authorised", False))


def confirm(args: argparse.Namespace, question: str, *, stream=None) -> bool:
    """Read one yes or no answer. False where nobody can be asked."""

    if not can_prompt(args, stream):
        return False
    reader = sys.stdin if stream is None else stream
    print(question, end="", flush=True)
    answer = reader.readline()
    return answer.strip().lower() in {"y", "yes"}


def retry_wanted(args: argparse.Namespace) -> bool:
    """Read one retry decision. Enter retries; Esc leaves."""

    if not can_prompt(args):
        return False
    print(f"\n{RETRY_PROMPT} ", end="", file=sys.stderr, flush=True)
    try:
        while True:
            key = read_key()
            if key in ENTER:
                print(file=sys.stderr)
                return True
            if key in (ESC, INTERRUPT, END_OF_FILE, ""):
                # Ctrl-C and Ctrl-D decline the retry without creating another error.
                print(file=sys.stderr)
                return False
    except (EOFError, KeyboardInterrupt):
        print(file=sys.stderr)
        return False


def read_key() -> str:
    """Read one keypress without waiting for a line.

    Preserve complete escape sequences so arrow keys are not treated as Esc.
    """

    try:
        import termios
        import tty
    except ImportError:
        return _read_key_windows()

    descriptor = sys.stdin.fileno()
    try:
        saved = termios.tcgetattr(descriptor)
    except termios.error:  # not a terminal after all
        return sys.stdin.readline()[:1]

    import os
    import select

    try:
        tty.setcbreak(descriptor)
        # Text buffering would hide the remaining bytes of an escape sequence.
        key = os.read(descriptor, 1).decode(errors="replace")
        if key == ESC:
            while select.select([descriptor], [], [], 0.05)[0]:
                key += os.read(descriptor, 1).decode(errors="replace")
        return key
    finally:
        termios.tcsetattr(descriptor, termios.TCSADRAIN, saved)


def _read_key_windows() -> str:
    """One keypress on a console without POSIX terminal control.

    ``msvcrt`` reads a key as it is pressed, so Esc declines a retry here as it
    does elsewhere. Reading a line instead would wait for Enter, which is the
    other answer.

    A function or arrow key arrives as a prefix and then its code. Both are
    returned together, so it matches neither answer and the caller asks again
    rather than reading the code as the next keypress.
    """

    try:
        import msvcrt
    except ImportError:  # neither POSIX nor Windows: read a line and take one key
        return sys.stdin.readline()[:1]

    key = msvcrt.getwch()
    if key in ("\x00", "\xe0"):
        return key + msvcrt.getwch()
    return key


def _isatty(stream) -> bool:
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


__all__ = [
    "NON_INTERACTIVE_HELP",
    "RETRY_PROMPT",
    "add_non_interactive",
    "authorised",
    "can_prompt",
    "confirm",
    "non_interactive",
    "read_key",
    "retry_wanted",
]
