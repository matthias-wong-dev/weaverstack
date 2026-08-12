# How to add an artefact

## Purpose

This is the maintainer procedure for adding a runtime artefact while preserving
Weaver's planning, installation, run, and wipe contracts.

An artefact is anything Weaver installs into a bound target: a table, a view, a
folder, an alias, a deployed module, a generated procedure. Adding one touches
six places, and the suite is arranged so that skipping any of them fails with a
test that names what is missing rather than a symptom several layers downstream.

Work in this order. Each step's test should be failing before you start it and
passing when you finish, and the *next* step's test should then be the one that
fails.

---

## 1. Define it as an artefact

Give it an identity, a signature and a place in the catalogue's vocabulary.

- an identity — a schema and an object inside an item
  ([`WeaverDocumentId`](../src/weaver/declaration/model.py)). If the two halves
  are not table-shaped, they get a *shape*, which decides validation and
  spelling. Nothing is encoded to fit an existing validator;
- an `object_type` in [`OBJECT_TYPES`](../src/weaver/catalogue/tables.py), and an
  `object_role` — `data` if it holds or shapes rows, `load` if it does the work
  that fills one;
- claim rules in [`claims.py`](../src/weaver/catalogue/claims.py), naming every
  catalogue table it may populate. The mapping is exhaustive on purpose: an
  unregistered type cannot reconcile at all;
- a signature: whatever would make an installed copy *wrong*. Its own bytes, or
  the document it renders plus the version of the generator that rendered it.

**Fails until done:** `test_catalogue_from_repository_has_all_artefacts` —
`OBJECT_TYPES` and the projection disagree.

## 2. Give it a lifecycle in pure Python

The artefact must be derivable from the repository alone, with no target
inspection: current contents determine the complete set of claims, which is what
lets a deleted source produce an ordinary prune.

- derive it during interpretation, so a rename is an old claim and a new one and
  nothing needs to know they are related;
- make sure `Catalogue.from_repository` registers it — it is the *desired* state
  the whole build reasons from;
- make sure `FixtureInventory.from_repository` predicts it, and that
  `TargetInventory` can *see* it. A type the inventory cannot observe is
  disproved by every reconciliation and silently rebuilt on every build.

**Fails until done:** `test_every_declared_object_and_artefact_is_registered`,
then the convergence tests in `test_fixed_point.py`.

The inventory half is the one that fails *quietly*, and it is worth knowing the
shape it takes. Adding `_.Log` — an ordinary Folder in the built-in control-plane
item — surfaced as a build that recreated the folder on every run, because
`read_lakehouse_inventory` excluded the whole Files area for `_weaver` and so
could never observe it. Nothing reported a fault: an unobservable artefact is
disproved by every reconciliation and rebuilt each time, exactly as this step
warns. If a new artefact lives somewhere an inventory reader currently skips,
that reader is part of the change.

## 3. Add it to the planner, and declare what it means

Render its actions, and beside them state their effect on the target — the
objects added and removed, in the vocabulary an inventory reports.

Declare the effect; do not infer it. Deriving "what does `prune_schema` do" from
the action kind is a model of executor semantics living where no executor can
correct it, and it drifts in silence. Every change names the action that produces
it, so the two cannot fall out of step.

**Fails until done:** `test_every_action_that_touches_a_target_is_declared`, and
its converse `test_every_declared_change_names_an_action_that_runs`.

## 4. Check the bundle still installs

A bundle carrying the new action must still execute against a real target, in
order. Add the artefact to the estate in
[`test_bundle_can_install.py`](../tests/fabric/test_bundle_can_install.py) and to
the existence assertions there.

**Fails until done:** `test_a_whole_bundle_installs_in_its_own_order_against_a_real_lakehouse`.

## 5. Write the targeted action test

One test per action kind, executing it against a real engine and inspecting what
it made.

```text
tests/spark/boundary/test_actions_delta.py       real Spark
tests/fabric/test_warehouse_action_primitive.py  real Fabric, over TDS, no Livy
```

Named `test_<kind>_action_<what it proves>`, so a test names both the kind and
its claim. Then add the kind to
[`test_action_checklist.py`](../tests/targeted/test_action_checklist.py) — either
covered, naming the test, or deferred, with a reason. That file prints the whole
list:

```text
pytest --collect-only -q tests/targeted/test_action_checklist.py
```

The subject is always the **Weaver document**, never the engine. Not "can Spark
create a view" but "the view this document declares is the view that appears".

**Fails until done:** `test_every_action_kind_is_covered_or_deliberately_deferred`
names the unplaced kind; `test_the_named_execution_test_exists` catches a
checklist that points at nothing.

## 6. Make sure a wipe removes it

A wipe clears a target completely. If the artefact is a new physical *object
type* — a procedure, a function — the wipe enumerates types explicitly and will
walk straight past it.

Add one to the wipe fixture's seed, so the claim is "a wipe removed the thing we
put there" rather than "the SQL mentions the word".

**Fails until done:** nothing, today — which is why this step is written down.
The Warehouse wipe drops procedures by enumerating `sys.procedures`, and that is
asserted as *text* only; nothing seeds one and wipes it.

---

## Why the order matters

The failures are arranged so the *first* one names the artefact. A missing
catalogue registration surfaces as "this object type has no rows", not as an
install error four layers away with a stack trace about a payload.

That ordering is verified rather than hoped for. Adding a hypothetical
`semantic_model` to `OBJECT_TYPES` fails step 1's test and only that one;
suppressing the load layer's declarations fails step 3's and, in consequence,
step 2's convergence.

## What each layer is allowed to cost

| layer | cost | what belongs there |
|---|---|---|
| pure Python | milliseconds | every decision: derivation, signatures, selection, planning, intent |
| local Spark | seconds | does the engine agree — types, nullability, a view that resolves |
| Fabric, TDS | seconds | the same, where only Fabric can answer, without a session |
| Fabric, Livy | ~40s a session | one thing: does a whole bundle install, in order |

A claim belongs at the cheapest layer that can answer it. If you find yourself
reaching for a session to check a decision, the decision is testable in pure
Python and the session is hiding that.

## See also

- [test-architecture.md](test-architecture.md) — what each layer claims
  today, and the known gaps
- [how-does-build-work.md](how-does-build-work.md) — §11a on declared intent,
  §11c on load artefacts as a worked example of all six steps
- [build-philosophy.md](build-philosophy.md) — why the bundle is the contract
