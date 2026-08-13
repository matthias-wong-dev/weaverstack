# Claude guidance

@AGENTS.md

`AGENTS.md` is the shared source of truth. This file addresses recurring
Claude-specific drift.

Claude-generated prose can become rhetorical, argumentative, overly clever,
curt or condescending. Treat user-facing text as product copy: write calm,
neutral, respectful, ordinary technical English. State the condition first and
include a next action only when it is known.

Comments and docstrings explain a local contract or constraint. Do not turn
them into essays, reviewer arguments or implementation history.

Before completing a task that changes prose, re-read every changed user-facing
string, CLI help string, error or warning, comment, docstring and design-doc
paragraph. Rewrite anything that sounds argumentative, rhetorical, clever for
its own sake, curt, condescending, corrective, or defensive of an
implementation.

Avoid “the whole point”, “the key point”, “worth noting”, and unnecessary
“obviously”, “simply”, “merely”, “deliberately” or “intentionally”. Avoid
rhetorical “this is not X; it is Y” phrasing, statements about what should be
obvious, and implementation rationale in user-facing output.

Preferred:

```text
Lakehouse/Sales was not found in workspace 'Demo'.
```

Avoid:

```text
no such item in 'Demo': Lakehouse/Sales — check the name, or build into it first
```

Preferred comment:

```python
# OneLake mount state can lag DFS deletion, so retry before reusing the path.
```

Avoid multi-paragraph comments that explain why an implementation is correct or
why alternatives are wrong. Run `python .claude/prose_tripwire.py` before
handing off prose changes; it prompts review and never blocks completion.
