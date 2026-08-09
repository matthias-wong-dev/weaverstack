"""Every program Weaver ships into Fabric is valid Python before it is shipped.

A remote program is a string here and code there. A typo in one costs a Livy
session, a submission and a wait to discover — twenty minutes into a Fabric run,
reported as a Spark error about something else. Nothing else in the suite reads
these bodies at all.

So they are parsed here, cheaply, with no Fabric and no Spark. Interpolations are
replaced with a placeholder: the *shape* is what this checks, because the values
are known only at submission time.

This does not check that the program is correct — only that it is syntax. What
it is really defending is the moment an API those bodies name gets renamed, which
is when a body silently stops matching the wheel it will run against.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: What marks a string as a program rather than prose: it hands a value back the
#: way a submitted body does, and it imports something to produce it.
MARKERS = ("emit(", "import ")

#: Stands in for an interpolated value. A name, so it parses wherever a value
#: does — as an argument, a subscript or the right-hand side of an assignment.
PLACEHOLDER = "_interpolated"

#: Source *builders*, not programs: these assemble a body around a placeholder
#: that is itself a block of code, so they are not valid Python until something
#: fills them in. Named explicitly, because "skip what does not parse" would
#: skip exactly the defects this exists to catch.
BUILDERS = {
    "src/weaver/fabric/livy.py",
    "src/weaver/session/protocol.py",
    "src/weaver_cli/main.py",
}


def _text(node: ast.AST) -> str | None:
    """The program text of a string node, or None if it is not a program."""

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            value.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str)
            else PLACEHOLDER
            for value in node.values
        )
    return None


def _strings(tree: ast.AST):
    """Every string node, without descending into an f-string's own parts.

    An f-string's fragments are Constants too, and a fragment of a program is
    not a program — checking one reports a syntax error in the middle of a
    perfectly good body.
    """

    stack = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.JoinedStr):
            yield node
            continue
        if isinstance(node, ast.Constant):
            yield node
            continue
        stack.extend(ast.iter_child_nodes(node))


def _programs():
    """Every remote program body in the repository, with where it was written."""

    for path in sorted((ROOT / "tests").rglob("*.py")) + sorted(
        (ROOT / "src").rglob("*.py")
    ):
        relative = path.relative_to(ROOT).as_posix()
        if relative in BUILDERS:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # the file's own syntax is another test's problem
            continue
        for node in _strings(tree):
            body = _text(node)
            if body is None or "\n" not in body:
                continue
            if all(marker in body for marker in MARKERS):
                yield relative, node.lineno, body


def test_every_remote_program_body_parses():
    broken = []
    for where, line, body in _programs():
        try:
            ast.parse(body)
        except SyntaxError as exc:
            broken.append(f"{where}:{line}: {exc.msg} (program line {exc.lineno})")

    assert not broken, "these submitted programs are not valid Python:\n" + "\n".join(
        broken
    )


def test_no_remote_program_names_an_abstraction_that_no_longer_exists():
    """A body naming a deleted API is a Fabric failure waiting to happen.

    The console and the published wheel are two halves of one contract, and a
    rename that misses a program body leaves the two disagreeing until something
    runs. The protocol version catches a *stale wheel*; this catches a stale
    program.
    """

    retired = ("InstallationEnvironment", "install_bundle(", "execute_action(")
    offenders = [
        f"{where}:{line}: names {name}"
        for where, line, body in _programs()
        for name in retired
        if name in body
    ]

    assert not offenders, "\n".join(offenders)
