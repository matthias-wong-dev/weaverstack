# Weaver prose

Read this guide before changing user-facing text, documentation, docstrings or
source comments. It governs wording, not product behaviour.

## Principles

1. **Senior engineer to senior engineer.** Assume competence. Be clear without
   teaching down, selling or performing expertise.

2. **Use plain, concrete technical language.** Name the actual thing: Weaver,
   Fabric, the catalogue, the command, the table, TDS or OneLake.

3. **State what it is.** Describe the current behaviour positively. Contrast it
   with what it is not only when that distinction is genuinely easy to miss.

4. **Be succinct.** Give the fact, constraint or reason needed to understand the
   issue. Stop there.

5. **Stay local to the problem at hand.** Especially in user-facing prose,
   include only what affects the current action, error or decision. Do not
   explain adjacent architecture or edge cases unless they matter now.

6. **No defensiveness.** Do not justify the implementation against imaginary
   objections or alternative designs nobody proposed.

7. **No archaeology.** Describe the system as it exists now. Leave out previous
   implementations, old bugs, abandoned approaches and development history
   unless compatibility or migration depends on them.

8. **Keep the real actor and action visible.** Do not make abstractions think,
   know, want, care, decide or refuse. Do not hide the actor behind vague passive
   prose either.

9. **Explain constraints, not hypothetical failure chains.** State the condition
   that matters and its consequence. Do not narrate everything that might
   otherwise go wrong.

10. **Comments explain the non-obvious engineering fact.** Preserve invariants,
    platform constraints, ordering requirements and failure boundaries. Do not
    narrate obvious code or turn comments into design essays.

11. **User-facing prose serves the user.** Say what happened, identify the
    relevant thing and give the next action when one is known. Nothing more.

## Applying the principles

### CLI help

Say what the command or option does. Name its input, target or effect when that
information changes how it is used.

### Errors and warnings

State the condition first. Identify the affected command, item, table, workspace
or path. Give a next action only when Weaver can identify one.

### Reports

Report the result and the facts needed to act on it. Do not reproduce the log or
explain unrelated architecture.

### Docstrings

State the callable's purpose and any non-obvious contract. Describe parameters
or return values only when their meaning is not clear from the signature and
types.

### Comments

Explain the constraint the code cannot express. Put system-wide reasoning in the
relevant document under `design/`.

## Review

Read changed help text, errors, warnings, reports, docstrings and comments in
context. Check that each sentence names the real thing, serves the local problem
and stops when its work is done.
