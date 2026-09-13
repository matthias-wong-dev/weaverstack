# Weaver prose

Read this guide before changing user-facing text, documentation, docstrings or
source comments. It governs wording, not product behaviour.

## Default

Prefer deletion over replacement. Prose must add information that names, types
and code do not already communicate. Delete it when it does not. Rewrite only
when useful information remains.

## Principles

1. **Senior engineer to senior engineer.** Assume competence. Be clear without
   teaching down, selling or performing expertise.

2. **Use plain, concrete technical language.** Name the actual thing: Weaver,
   Fabric, the catalogue, the command, the table, TDS or OneLake.

3. **State the model.** Describe the current behaviour positively. State the
   rule or constraint, not the accident that a different design might cause.
   Contrast it with what it is not only when that distinction is genuinely easy
   to miss.

4. **Be succinct.** Give the fact, constraint or reason needed to understand the
   issue. Stop there.

5. **Stay local to the problem at hand.** Especially in user-facing prose,
   include only what affects the current action, error or decision. Do not
   explain adjacent configuration, architecture or edge cases unless they
   matter now. A fact being true and related is not enough reason to include it.

6. **No defensiveness.** Do not justify the implementation against imaginary
   objections or alternative designs nobody proposed.

7. **No archaeology.** Describe the system as it exists now. Leave out previous
   implementations, old bugs, abandoned approaches and development history
   unless compatibility or migration depends on them.

8. **Keep the real actor and action visible when agency matters.** Do not make
   abstractions think, know, want, care, decide or refuse. Do not hide a relevant
   actor behind vague passive prose. Do not insert Weaver as the subject when a
   label or direct statement of state is clearer.

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
or path. Give a next action only when Weaver can identify one. Do not explain
adjacent configuration or architecture unless the user needs it to act.

### Reports

Report the result and the facts needed to act on it. Do not reproduce the log or
explain unrelated architecture.

### Docstrings

Delete a docstring when the name, signature, types and code already communicate
the callable's purpose. Otherwise state only the non-obvious contract. Describe
parameters or return values only when their meaning is not clear from the
signature and types.

### Comments

Delete comments that narrate the code. Keep a comment only when it explains a
constraint the code cannot express. Put system-wide reasoning in the relevant
document under `design/`.

## Review

For every comment, docstring, error or help string, ask:

1. Does it contain information that is not already obvious from the names, types
   and code?
2. Is that information relevant here?
3. Can it be stated directly in fewer words?
4. Does it describe the current system, or defend and explain how the system got
   here?

If the answer to the first question is no, delete it.
