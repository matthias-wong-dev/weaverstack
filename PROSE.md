# Weaver prose

Read this guide before changing user-facing text, documentation, docstrings or
source comments. It governs wording, not product behaviour.

## Default

For comments and docstrings, preserve the smallest amount of prose that carries
non-obvious engineering information.

For user-facing text, preserve only information needed to understand the current
result or choose the next action. Internal facts do not earn a place merely
because they explain why Weaver behaves that way.

Delete everything else. Do not optimise for deletion count. Optimise for
information density.

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

For every help string, prompt, warning or surfaced error:

1. State the condition in the user's vocabulary.
2. Name the affected thing.
3. Give the next action when Weaver knows it.
4. Delete anything that only explains the implementation.

Errors are not miniature architecture documentation. Avoid internal terms such
as Registry rows, planner structures, bindings, item graphs, four-part naming
and transport boundaries unless the user must interact with that concept to
resolve the error.

Catalogue implementation details such as `[_]` and `[_].[Mirror]` appear in
user-facing prose only when they are actionable. Usually name the user-visible
condition: a Warehouse does not contain a Weaver catalogue, a catalogue is
incompatible with this Weaver version, or a catalogue is already mirrored.

### Reports

Report the result and the facts needed to act on it. Do not reproduce the log or
explain unrelated architecture.

### Docstrings

Identify any non-obvious purpose or contract, then preserve its shortest direct
statement. Delete the docstring when no useful information remains. Describe
parameters or return values only when their meaning is not clear from the
signature and types.

### Comments

Identify any invariant or constraint the code cannot express, then preserve its
shortest direct statement. Delete comments that only narrate the code. Public
product behaviour belongs at `docs.weaverstack.dev`; implementation invariants
belong in `AGENTS.md`, source or tests.

## Review

For every comment, docstring, error or help string, ask:

1. What non-obvious information, if any, is here?
2. Does that information belong here?
3. What is the shortest direct way to preserve it?
4. If there is no useful information, delete it.
