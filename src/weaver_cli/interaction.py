"""Apply the CLI-wide interaction and authorisation policies.

``--non-interactive`` prevents stdin reads, keypress waits and browser sign-in.
``--yes`` authorises destructive work; neither option implies the other.
"""

from __future__ import annotations

import argparse
import sys

NON_INTERACTIVE_HELP = (
    "Do not read stdin, wait for a keypress or open browser sign-in. Missing "
    "authorisation or required input is an error."
)

RETRY_PROMPT = "Enter to retry, Esc to exit."

ESC = "\x1b"
INTERRUPT = "\x03"
END_OF_FILE = "\x04"
ENTER = ("\r", "\n")


def add_non_interactive(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--non-interactive", action="store_true", help=NON_INTERACTIVE_HELP
    )


def non_interactive(args: argparse.Namespace | None) -> bool:
    return bool(getattr(args, "non_interactive", False))


def can_prompt(args: argparse.Namespace | None, stream=None) -> bool:
    if non_interactive(args):
        return False
    return _isatty(sys.stdin if stream is None else stream)


def authorised(args: argparse.Namespace | None) -> bool:
    """Return authorisation from ``--yes`` or a confirmed workflow."""

    return bool(getattr(args, "yes", False) or getattr(args, "authorised", False))


def confirm(
    args: argparse.Namespace, question: str, *, stream=None, prompt_to=None
) -> bool:
    """Read one yes-or-no answer, or return false when the invocation cannot prompt.

    ``stream`` is where the answer is read from and ``prompt_to`` where the
    question is written, stdout by default. A command whose stdout carries one
    JSON document writes the question to stderr.
    """

    if not can_prompt(args, stream):
        return False
    reader = sys.stdin if stream is None else stream
    print(
        question,
        end="",
        flush=True,
        file=sys.stdout if prompt_to is None else prompt_to,
    )
    answer = reader.readline()
    return answer.strip().lower() in {"y", "yes"}


def retry_wanted(args: argparse.Namespace) -> bool:
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
                # Ctrl-C and Ctrl-D decline without replacing the original failure.
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
    """Read one Windows keypress, preserving prefixed function and arrow keys."""

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
