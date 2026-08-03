# Test architecture refactor — handover

Written at the end of the load-primitives work (PR #23, merged as `ab64faf`),
which is what prompted it. Branch `claude/test-architecture` carries the first
commit; everything else below is still to do.

## Why

The load work surfaced four defects that no local test could have found, and the
reason each time was the same: **there was no level that could see them.** Not a
missing test — a missing *layer*. A probe answered one of those questions in 69
seconds, was thrown away, and the next four discoveries each cost a seven-minute
wheel publish they never needed.

Two tests also passed *while agreeing with bugs*, one of them against a wheel
older than the fix it claimed to cover.

So the goal is not tidiness. It is **affordance**: the names and markers should
make it obvious what a test proves, what it costs, and what is missing — without
anyone having to remember a rule. The rules that held during that work were the
ones with teeth (`test_core_boundary`, the generator fingerprint test); every
rule that was prose got broken, sometimes by the person who had just read it.

## The two vocabularies

The core decision. These are orthogonal and must not share a namespace — mixing
them is what made `fabric` mean both "a place" and "needs nothing published",
which is how a test came to be marked with both `fabric` and `published_weaver`
and silently broke the fast loop's one promise.

**Claim — what a test proves. Carried in the *module name*.**

| claim | proves |
|---|---|
| `declaration` | the contract is validated to exhaustion — parsing, references, naming, contradictions |
| `render` | what Weaver generates from a valid declaration — DDL, load payloads, catalogue DML, manifests |
| `binding` | logical mapped to physical — plan, incremental selection, projection, prune, reconcile |
| `primitive` | one independently runnable operation, executed for real against an engine |
| `lifecycle` | an artefact through declare → project → install → fixed point → repair → remove |
| `invariant` | architecture rules, enforced (core boundary, neutrality, public API) |

Convention: `test_<subject>_<claim>.py`. Function names stay prose —
`test_a_developer_can_run_a_deployed_folder_load_primitive` reads well and should
not be forced into a prefix. The module name carries the level; the failure
output then names the level before anyone reads the assertion.

**Cost — what must exist for a test to run. Carried in *markers*.**

| marker | needs |
|---|---|
| *(none)* | nothing |
| `spark` | a JDK |
| `fabric` | a real workspace. **Every** Fabric test carries it |
| `remote` | Weaver runs on this machine and reaches in — the desktop CLI's position |
| `hosted` | Weaver runs inside the session, as the installed wheel |
| `provision` | creates and deletes Fabric items |
| `full_integration` | the one composed lifecycle journey |

`remote` and `hosted` are about **where Weaver runs**, not where any code runs.
`tests/fabric/test_onelake_mount_contract.py` submits a Spark body but imports
nothing of ours, so it is `remote` — we are driving Fabric, not hosted in it.

Exactly one position accompanies `fabric`, always. That is what makes the parity
question answerable by selection rather than by memory.

## Done already

On `claude/test-architecture`, one commit, suite green (1394 pure-Python):

- `fabric` now carried by every Fabric test; `remote`/`hosted` state the position
  on each; `provisioning` renamed `provision`; `published_weaver` retired.
- `pyproject.toml` marker descriptions rewritten to say *why* each exists.
- Selection verified: 67 `fabric` = 49 `remote` + 16 `hosted` + 1
  `full_integration` + 1 `provision`. No Fabric test has an unstated position.

## To do

1. **Add the naming invariant.** A test asserting every test module matches
   `test_<subject>_<claim>.py` with a claim from the set above. This is what
   gives the convention teeth — without it, it is prose, and prose got broken
   every time during the load work. Expect to allow a grandfathered list at
   first and shrink it.

2. **Rename load and build first**, not the whole suite. They are the subjects
   just worked on, so a mismatch will be recognisable. Known renames:
   - `tests/targeted/test_convergence.py` → `test_artefact_lifecycle.py`
     (already on the author's list; "convergence" is the wrong word for
     declare → install → fixed point → repair → remove)
   - `tests/targeted/test_load_generation.py` → `test_load_render.py`
   - `tests/targeted/test_load_artefacts.py` + `test_load_plan.py` → the
     `binding` claim
   - `tests/spark/test_python_load_primitive.py`,
     `tests/spark/test_spark_load_primitive.py`,
     `tests/fabric/test_warehouse_load_primitive.py` — already correct

3. **Split `tests/fabric/test_actions_warehouse.py`.** It mixes two claims:
   assertions about declared types, primary-key nullability and native identity
   are `primitive`; only "the action reached its executor and created the
   object" is about the action seam. Keep the module-scoped estate shared.

4. **Decide what the 70 root files are.** They are not one layer needing a name
   — they are four claims that were never distinguished: `declaration`
   (`test_declaration_*`, `test_weaver_model`), `render`
   (`test_declaration_create_ddl`, `test_catalogue_render`,
   `test_build_bundle_model`), `invariant` (`test_core_boundary`,
   `test_neutrality`, `test_public_api`), and small-surface `declaration` tests
   of components (SQL pool, tokens, locations, store). That is why the layer
   grew accidentally: it had no stated purpose, and it is the largest and
   cheapest one.

5. **Do markers first, files second.** Markers and module names give selection
   immediately. Moving files churns history — only move where the name has
   already made the misplacement obvious.

## Gaps the new markers already surface

Worth confirming rather than assuming; they come from file names and selection,
not from reading every test.

- **`Folder.load()` is proven `hosted` and locally, and not `remote` at all** —
  exactly the pairing that broke during the load work. Either a gap or a
  deliberate limit of the CLI; nobody has decided which.
- **Prune** is proven only as a decision in pure Python. It is the most
  destructive operation Weaver has, and nothing exercises it against a real
  target outside the journey.
- **Build** has no Fabric module of its own. It is covered by
  `test_bundle_can_install` and the journey, which may be fine — but it cannot
  be seen from the names.
- `tests/spark/boundary/test_inventory_fidelity.py` was missed by a file-name
  sweep, which is itself a small signal about the current structure.

## Spark's remit — the reframe that matters most

**Spark is not a cheap Fabric.** Treating it as a faithful mirror is what made
"green locally" read as "green in Fabric", and that assumption cost most of the
expensive discoveries in the load work.

Its honest remit is *the engine facts we are confident transfer, at minimal
distortion*:

- **Transfers** — Delta and Spark semantics: merge behaviour, null-safe
  comparison, `DELETE` refusing subqueries, `MERGE ... UPDATE SET *` requiring
  every target column, column mapping for spaced names. Local is version-matched
  by design (`pyspark>=3.5,<3.6`, `delta-spark>=3.2,<3.3`), which is *why* they
  transfer. Every engine fact found locally held in Fabric.
- **Does not transfer** — anything about where bytes live or how code is
  deployed. Locally `Files` is a real directory and modules sit flat in a temp
  dir; in Fabric storage is object storage and the tree is a deployed package.
  All four Fabric-only defects were of this kind.

Two consequences:

- **Stop parametrising one body across both transports.** It asserts an
  equivalence that does not hold and is costly on both sides. Let each transport
  prove what only it can.
- **Verify the version match rather than assume it.** The whole "engine facts
  transfer" argument rests on local Delta being Fabric's Delta, which is a
  design intent in `pyproject.toml` and a checked fact nowhere. A `fabric and
  remote` test reporting the session's Spark and Delta versions against the
  local pins would close it cheaply, without a wheel.

## Hazards a fresh session should know

- **A `hosted` test can pass against a stale wheel.** Build generation runs
  inside the session using the installed package, so a test can agree with a bug
  the checkout has already fixed. This happened. Nothing in the harness detects
  it; a version check between the installed package and the checkout would.
- **Rerun `weaver install` after any change to `weaver`** before trusting a
  `hosted` result, and expect ~7 minutes.
- **A Livy session started immediately after a build often dies** on capacity.
  Retrying once works.
- **One decision computed in two places** caused three separate bugs during the
  load work (the tolerance gate, the delete key set, the stability threshold).
  Each fix was the same: decide once, record it, read it. Worth watching for.

## Still open from the load work

Not part of this refactor, but unresolved and recorded so it is not lost:

- Fabric's DDL is transactional, so an intolerant Warehouse `throw` rolls back
  the reject table — raising natively and preserving the rejection evidence
  currently conflict. `tests/fabric/test_warehouse_load_primitive.py` asserts
  what actually happens and says why.
