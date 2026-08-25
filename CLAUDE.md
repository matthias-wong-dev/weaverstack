# Claude guidance

@AGENTS.md

`AGENTS.md` is the shared source of truth. This file covers writing, because
that is where Claude drifts.

## The register

Write descriptions, not arguments. State the thing and stop. Do not contrast it
with what it is not, justify it against a design nobody chose, or emphasise it in
case the reader disagrees.

Six rules. Each one is checkable.

1. **Say what a thing is.** Drop "rather than", "instead of" and "not X" unless
   the alternative is one someone will actually reach for.
2. **No counterfactuals.** If a reason matters, give the constraint that causes
   it. Do not describe the failure that would have followed.
3. **Code has no mental states.** Not "the Session knows" but "the Session
   resolves". No knowing, wanting, asking, refusing, caring or deciding.
4. **No emphasis.** No italics, no "exactly", "genuinely", "the whole point",
   "deliberately". A word that needs stressing means the sentence is built wrong.
5. **No reader in the text.** Never write "a reader asking" or "someone reading".
6. **No asides.** No em dashes. Two ideas are two sentences.

**Name real things.** TDS, Livy, OneLake, REST, a Delta commit, a 403. Abstraction
nouns such as "capability", "surface" and "the implementation" are the same
problem in a quieter voice.

Length follows from these. A function docstring runs one to three sentences, a
module or class docstring about ten lines, a comment block three. Anything longer
belongs in `design/`.

## Examples

Error text:

```text
yes  Lakehouse/Sales was not found in workspace 'Demo'.
no   no such item in 'Demo': Lakehouse/Sales — check the name, or build into it first
```

A comment:

```text
yes  OneLake mount state can lag DFS deletion, so retry before reusing the path.
no   OneLake mount state can lag DFS deletion. Reusing the path immediately would
     find a directory the delete has already accepted, which is why the retry is
     here rather than at the call site.
```

A module docstring:

```text
yes  There is a lag between the T-SQL commit and the Delta log reaching OneLake,
     and a further lag before the new Parquet files are readable. This waits for
     the new commit, then opens each file it added.

no   A Warehouse table settles over TDS, and Fabric then publishes the table's
     Delta log in the background. Until that publication lands and its Parquet
     files can be opened through the consuming shortcut, a Lakehouse consumer
     reading the same table sees either the previous snapshot or a snapshot whose
     files it cannot read. This is the Warehouse-side counterpart of the
     Lakehouse SQL analytics endpoint refresh: the producer has finished writing,
     and the surface the consumer reads has not caught up yet.
```

## User-facing text

CLI help, errors and warnings are product copy. State the condition. Add a next
action when the code knows one. Do not lecture, joke, or explain why the
implementation is right.

## Before handing off

Re-read every changed string, help text, error, comment, docstring and design
paragraph against the six rules. Run `python .claude/prose_tripwire.py`. It
prompts review and never blocks.
