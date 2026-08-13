# Test architecture

## Purpose

This document explains how to select the cheapest test layer that can prove a
claim, and what the more expensive Fabric layers add.

Where a claim belongs, and why. The organising rule is one sentence:

> **Prove a claim at the cheapest layer that can answer it, and use the
> expensive layers only for what only they can answer.**

The rest of this document applies that rule to the repository test suite.

## The layers

| command | needs | collects | what it is for |
|---|---|---|---|
| `pytest` | nothing | ~2040 | contracts, rendering, planning, binding — the development loop |
| `pytest -m spark` | a JDK | ~250 | Delta, the Spark catalogue, deployed-module execution |
| `pytest -m "fabric and remote"` | a workspace | ~80 | one narrow question a real Fabric can answer, with nothing published |
| `pytest -m "fabric and hosted"` | a workspace **and a published wheel** | ~30 | Weaver running *inside* Fabric — the product's own position |
| `pytest -m full_integration` | a workspace and a published wheel | 1 | composition only |
| `pytest -m provision` | permission to create and delete items | 4 | Fabric's resource management, not Weaver's |

Markers are peers; none implies another. Each says *what a test needs*, so a
selection is honest about its cost.

The first three are the development loop, and **none of them publishes
anything** — that is the point. A five-minute `weaver install` between a
developer and finding out a REST body was malformed is a five-minute penalty on
every mistake.

## Directories describe cost and fixture family, never claim

```text
tests/
  targeted/          pure Python, by seam — narrow fixture constructors live here
  support/           shared harness: build env, observation, Livy ledger, claims
  spark/             local Spark, incl. boundary/ for interface fidelity
  fabric/            a real workspace, and nothing that does not need one
```

Marker, directory, fixture and transport describe the same thing. That is not
tidiness: a module under `tests/fabric` that answered to `-m spark` loaded the
Fabric conftest, and with it a workspace, a credential and a session — so the two
names described different sets and neither was honest.

`tests/support` holds what belongs to neither transport: the build environment a
test drives, and the claims two transports both make. A module that spans both is
a thin wrapper over a shared claim, not a parametrised module — a parametrised
one can only be honest about one of its markers.

## Modules name their claim

`test_<subject>_<claim>.py`, where the claim is one of:

| claim | the module proves |
|---|---|
| `declaration` | what a contract accepts and refuses |
| `render` | the exact text or payload something generates |
| `binding` | logical intent bound to physical reality — planning, selection, dispatch |
| `primitive` | one installed thing, run directly, with nothing orchestrating it |
| `lifecycle` | a sequence of transitions, asserted at each one |
| `invariant` | a property the estate must keep, enforced rather than trusted |

`tests/test_test_architecture_invariant.py` holds the suite to it. Legacy modules
are grandfathered *individually and classified*, so an exception cannot appear
because somebody added a file; renaming one removes its entry.

## Choosing a layer

Ask what would have to be true for the cheaper layer to be unable to answer.

**Pure Python** answers anything that is a decision: which action is emitted,
what SQL is rendered, what a signature covers, what a status means, which
primitive is dispatched. A fake at the boundary is not a compromise here — it is
what lets the case be one nobody could arrange in a real estate, like a procedure
that must be called exactly once, or a frame that refuses to be collected.

**Local Spark** answers what only an engine can: does Delta actually do this,
does the catalogue read back into the object a fixture builds, does a deployed
module import and run. Its job after the logic is proven above is *fidelity* —
does a real read produce the same object a fixture constructs.

**Fabric** answers what only the platform decides. Not "does the feature work" —
that is settled below — but "does this engine accept this, and mean what we
assumed". Every Fabric test should be reducible to a sentence of that shape.

**The journey** proves composition and nothing else. It should rarely be where a
defect is found first: syntax, selection, planning, rendering, execution and
reconciliation are all meant to be proven beneath it.

## Two claim shapes worth naming

**Claims about what the code does not do.** These cannot be made by observing a
correct outcome, because a correct outcome is what both the right and the wrong
implementation produce. They need a collaborator that *refuses*: a frame whose
`collect()` raises proves a suppressed run never materialised a row; a counting
executor proves a procedure was executed once rather than twice. Both live in
pure Python, because inventing an uncooperative collaborator is exactly what a
real estate cannot do.

**Round-trip pairing.** Build from a repository, read the state back, and assert
it equals what the fixture constructor produces. It is the strongest form of
boundary claim, because it justifies every pure-Python assertion that uses the
same constructor. `spark/boundary/test_inventory_fidelity.py` and
`test_catalogue_fidelity.py` are these.

## What only Fabric has caught, and why

Each one names a *shape* of gap rather than a bug.

**A Warehouse prune dropping the `_` schema it had just created.** Every pure
caller of `item_prune_stage` used a *Lakehouse* estate, and the two sides are not
symmetric: a Lakehouse's generated `_` is a folder document and reaches the
keep-set through the ordinary path, while a Warehouse's `_` is a schema no
document declares. The defect could only exist on the side nothing covered.

> **An asymmetry between the two physical sides is where a Lakehouse-only
> fixture stops being representative.** Prune, schemas and inventory all behave
> differently across that line.

It was reachable because the keep-set's load half arrived as a *defaulted*
argument — production passed it, a direct caller did not. The fixed-point test
does not catch it and never could: the planner passed the argument correctly, and
the bug lived in the default on a path only a direct caller took.

> **A seam with a destructive default cannot be covered from above.** A defaulted
> argument is two contracts, and a composed test proves only the one the
> composition uses. Either the default goes, or the seam is tested directly on
> every shape it accepts.

**A `pathlib.Path` wrapped around an `abfss://` URL.** Correct locally, where the
value is a directory; silently wrong in Fabric, where `Path` collapses `abfss://`
to `abfss:/`. Nothing caught it because no test had ever *run* those objects'
`read()`.

> **A fixture written to be parsed is not a fixture proven to execute**, and the
> two look identical until something executes one.

**A control item that could not see its own folders.** `read_lakehouse_inventory`
excluded the whole Files area for `_weaver`, which stopped being true the moment
the control plane declared a folder there — and an artefact the inventory cannot
observe is disproved by every reconciliation, so the build recreated it forever.

**A mount that outlived what was done to OneLake behind it.** A wipe over DFS is
not necessarily visible through a cached `synfs` mount. Nothing local can see
this: storage there *is* a filesystem, there is no mount, and there is one view
of it.

> "The same code runs either side" says nothing about the *storage* underneath.

**A `snapshot=` keyword in a Livy body.** A string sent to a session, executing
only against the installed wheel — no pure test could run it and no import check
could see it. The one category where `-m "fabric and hosted"` is the first
possible sight of the defect, and the reason to grep test *bodies* after a
signature change rather than trust a mechanical rewrite.

## The action checklist

```bash
pytest --collect-only -q tests/targeted/test_action_checklist.py
```

lists every action Weaver can perform against a target and the test that executes
it, one `[<kind>-<test>]` per line. Test names carry both halves —
`test_<kind>_action_<what it proves>` — so the list reads as a checklist without
giving up the claim. `test_action_checklist.py` holds the estate to it: every kind
the product defines is either covered, naming its test, or deferred with a reason.

## Adding an artefact: what fails, and in what order

The suite is arranged so a new artefact type is caught by a *sequence* of tests,
each naming a different thing left undone.

| what is missing | what fails |
|---|---|
| the catalogue does not register it | `test_catalogue_from_repository_has_all_artefacts` |
| a build emits no action for it | `test_converges_from_nothing_to_the_declared_estate` |
| an action is emitted but declares no effect | `test_every_action_that_touches_a_target_is_declared` |
| the effect is declared but no action performs it | `test_every_declared_change_names_an_action_that_runs` |
| the inventory cannot see it | `test_inventory_fidelity.py`, and every claim about it becomes vacuous |

The order matters as much as the coverage: the first failure names the artefact,
not a symptom several layers downstream. See
[how-to-add-an-artefact.md](how-to-add-an-artefact.md).

## Fabric economics

A Livy call is an architectural decision, not an implementation detail. One
submission costs seconds; the statements inside it cost almost nothing.

> A remote state transition produces **one** evidence payload. Assertions stay
> local.

Gather every question about one moment into one body, submit it once, assert
against what comes back — see `tests/fabric/observation.py`. Split calls only
where the *boundary between them* is the subject: before versus after a
transition, a failure stopping later work, prune changing the estate.

**No test asserts a Livy call count.** A number that has to be edited whenever a
probe legitimately changes teaches the suite to raise the budget rather than ask
why; `tests/fabric/livy_telemetry.py` prints a breakdown at the end of a run and
puts a regression in front of whoever caused it.

## Conventions

- Take the narrowest fixture that can answer the question. Reaching for a richer
  one is the smell `tests/targeted/factories.py` exists to remove.
- Assert the *transition*, immediately, not the final state. A journey mutates a
  live estate, so evidence read later is evidence about a different estate than
  the one the assertion names.
- A claim proven at one layer is not re-proven at another. Two copies are free to
  disagree, and the slower one is the one nobody runs.
- Pure-Python tests must not request Spark fixtures; Spark tests must not request
  Fabric fixtures. Currently a convention, not enforced.
