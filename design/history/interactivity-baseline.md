# Interactivity baseline — before Run and Build decomposition

The plan gates decomposition on measurement: *"no decomposition phase proceeds
without before/after timings"*. This is the *before*.

Captured against the real `Weaver Example` workspace on 2026-08-10, from the
checked-in example repository, through one `weaver session`:

```text
weaver session --workspace-config "examples/weaver_example.yml" --timings
weaver> wipe Lakehouse/Sales Warehouse/Reporting --yes
weaver> build "examples/.../repository" --bind Lakehouse/Sales=Lakehouse/Sales --bind Warehouse/Reporting=Warehouse/Reporting
weaver> load Lakehouse/Sales Warehouse/Reporting
weaver> test Lakehouse/Sales Warehouse/Reporting
weaver> exit
```

Everything succeeded: six load nodes, three validations, exit 0.

## Task and Step timings

```text
Wipe (dry run)                                          4.8s
  Lakehouse/Sales                                       4.8s
  Warehouse/Reporting                                   0.0s

Wipe                                                   2m22s
  Lakehouse/Sales                                       2.8s
  Warehouse/Reporting                                   4.0s
  Unbind catalogue claims                              2m15s

Build                                                  1m14s
  Read physical state                                  27.5s
  Build bundle                                          0.1s
  Upload bundle                                         1.3s
  Install                                              44.8s

Load                                                   2m47s
Test                                                   21.2s
```

## Transport ledger

```text
session lifetime 410.6s
  livy.load                   1 calls    167.6s
  livy.unbind                 1 calls    102.3s
  livy.install_bundle         1 calls     44.8s
  livy.acquire                1 calls     44.0s
  livy.read_build_state       1 calls     27.5s
  livy.test                   1 calls     21.2s
  livy.version                1 calls      0.6s
  tds.Reporting.acquire       1 calls      0.5s
  auth.acquire                1 calls      0.2s
  resolve.item                4 calls      0.0s
  resolve.item.cache_hits     4
  session.scopes              1
```

## What this already says

**One Livy acquisition for four commands.** `livy.acquire 1 calls 44.0s` is the
Session-reuse fix visible as a number. Before it, `wipe`'s unbind, `load` and
`test` each opened a session of their own, so the same journey paid roughly
three further cold starts — about two minutes, on a capacity that permits one
concurrent session and would have queued them regardless.

**"Build bundle" is 0.1s.** The planning that a coarse label would call *the
build* is a tenth of a second. What the Build Task actually spends is 27.5s
reading physical state and 44.8s installing — which is the plan's point about
labels hiding remote state reads, confirmed rather than assumed.

**Load is the largest single crossing.** 167.6s in one Livy submission, with no
visibility inside it: the whole Runner runs remotely, so the six nodes it
executed have no individual timings here. Phase 4 is what makes those Sub-steps
real, and this number is what it must not regress.

**Unbind is 102.3s for a catalogue write.** Worth understanding before Build
decomposition rather than after: it is a Spark statement against the control
Lakehouse, and the Step around it (2m15s) also absorbs the tail of the Livy
acquisition it was the first command to need.

**Resolution is free and already shared.** Four lookups, four cache hits, 0.0s.

## How to re-capture

The same commands, and `--timings`. Compare Task totals for shape and the
transport ledger for cause; a decomposition that moves seconds from
`livy.<operation>` into many smaller calls has to show the total did not grow.

---

# After Run decomposition — 2026-08-11

The same journey, with `load` and `test` decomposed: the estate is read across,
the graph is built on the desktop, and each node dispatches to whatever can run
it — TDS for the Warehouse procedure, the run's remote scope for a deployed
Python module.

```text
Load                                                   4m08s
  Read catalogue                                       1m30s
  Build run graph                                       7.6s
  Execute                                              2m27s
    load:Lakehouse/Sales/Sales.OrderExport              5.9s
    load:Lakehouse/Sales/Sales.Customer                34.2s
    load:Lakehouse/Sales/Sales.Order                   30.3s
    load:Lakehouse/Sales/Sales.OrderSummary            27.0s
    refresh:Lakehouse/Sales                             1.1s
    load:Warehouse/Reporting/Reporting.CustomerRevenue 41.9s

Test                                                   27.7s
  Read catalogue                                       10.6s
  Execute                                              13.1s
    Lakehouse/Sales/Sales.OrderCustomerExists           4.5s
    Lakehouse/Sales/Sales.OrderSummaryReconciliation    7.2s
    Warehouse/Reporting/Reporting.CustomerRevenuePresent 0.7s
```

```text
  livy.dispatch_python        4 calls     95.5s
  livy.read_catalogue         2 calls     61.3s
  livy.acquire                1 calls     39.9s
  livy.dispatch_validation    2 calls     11.1s
  livy.read_inventories       1 calls      4.1s
  livy.begin_run              2 calls      2.6s
  livy.end_run                2 calls      1.4s
```

## The comparison

**Per-node timing exists now.** The baseline's `load` was one 167.6s Livy call
with nothing visible inside it. The same work is now six timed Sub-steps, and
`Sales.Order` taking 30s is a fact somebody can act on rather than a share of an
opaque total.

**One scope per run, and it closes.** `begin_run` and `end_run` are one call
each per run — two runs, two of each — with four `dispatch_python` between them.
That is the guarantee the decomposition most had to preserve, measured rather
than asserted.

**Transport overhead is not material.** The six nodes account for ~140s of the
147s `Execute` step, so dispatching each node separately costs roughly a second
apiece including `begin_run`/`end_run`. The plan's question — whether per-node
Livy submit overhead is worth batching away — is answered *no* for an estate of
this shape. Do not batch.

**The first crossing is cold, and this run paid for it.** `read_catalogue` cost
90s here and 10.6s in the `test` that followed, against the same session and the
same catalogue. The Environment had just been republished, so the first
statement bore the interpreter's warm-up. Read the 10.6s as the steady-state
figure and the 90s as a one-off; the honest reading of `Load 4m08s` against the
baseline's `livy.load 167.6s` is that execution is comparable and the difference
is warm-up, not decomposition.

**Caveat.** One run of each. The read_catalogue spread (90s against 10.6s) is
itself the evidence that single-run Fabric timings carry real variance, so treat
these as a shape rather than a benchmark.

---

# Build state observation on the desktop — 2026-08-11

The first step of Build decomposition (§6.2). The state read was one opaque
crossing returning a whole `BuildState`; it now goes through the readers, which
ask for only what each part needs — Warehouse inventories over TDS from here,
the catalogue and the Lakehouse inventories across, each its own Step.

```text
Build                                                  1m47s
  Read target inventories                              57.0s
  Read catalogue                                       43.9s
  Build bundle                                          0.1s
  Upload bundle                                         1.5s
  Install                                               4.1s
```

against the baseline's

```text
Build                                                  1m14s
  Read physical state                                  27.5s
  Build bundle                                          0.3s
  Upload bundle                                         1.3s
  Install                                              44.8s
```

## What this does and does not show

**The totals are not comparable.** The baseline built an estate from empty after
a wipe; this rebuilt one already installed, so `Install 4.1s` is a build with
nothing to do rather than a faster install. Only the *read* is like for like.

**Two crossings pay the cold cost twice, and this run paid it.** The read went
from 27.5s in one crossing to 101s in two — but the Environment had just been
republished, so both were first statements. The `load` that followed in the same
session read the catalogue in 17.6s and the inventories in 3.9s, against the
same estate. Warm, the split costs about 22s against the coarse 27.5s: no worse,
and possibly better because the Warehouse inventory no longer crosses at all.

**So the split is free when warm and doubles the fixed cost when cold.** That is
the honest statement. It buys what §6.1 asks for — catalogue and inventories
attributed separately, which is what makes "read physical state, 27.5s"
answerable — and if the cold case ever matters more than the attribution, the
two reads can be combined into one crossing that returns both without changing
anything above them.

## A finding worth naming

**Every symbol a submitted body imports must exist in the published wheel.**
Both Fabric runs of this change failed first with

```text
error: read_inventories could not run: the Weaver published in Weaver Example is
older than this console and does not carry cannot import name
'_lakehouse_inventories_here' from 'weaver.build_bundle.workflow'.
Publish the current wheel with `weaver fabric environment publish ...`
```

The diagnosis is good and names the fix, and the failure closed every open frame
correctly. But the coupling is real and the decomposition increases it: each new
remote entry point is another symbol the desktop and the wheel must agree on,
and Environment publication costs about five minutes. Nothing here is wrong — §5.6
accepts using the installed runtime — but a development loop that needs a
republish per new crossing is worth watching, and is an argument for keeping the
remote surface small and stable rather than growing an entry point per action.

## Still to do in this phase

Install is still one crossing. Routing each action through the cheapest
capability — files through OneLake, T-SQL through TDS in parallel by topological
layer, Spark-only work through Livy — is the remainder, and the executors that
need a real `DataFrame` (`spark_table` reads `frame.schema.fields`) are the part
that needs a purpose-built remote helper rather than a statement.

---

# Desktop-driven install — 2026-08-11

The Installer now runs on the desktop. Each action goes to the capability it
needs — files straight to OneLake, T-SQL straight to TDS, control operations
over REST — and only actions that genuinely need Spark cross. The archive is
gone: packing, uploading and unpacking a zip existed to get payloads to where
the Installer was, and the Installer is here.

Three runs of the same from-empty build of the Weaver Example estate:

```text
                        archive      desktop,      desktop,
                        + remote     per action    batched
  Read target inventories   4.4s         4.1s          4.1s
  Read catalogue           24.4s        23.8s         27.2s
  Build bundle              0.1s         0.1s          0.1s
  Upload bundle             1.6s            —             —
  Install                  51.2s        78.0s         60.0s
✓ Build                    1m22s        1m46s         1m32s
```

## What the numbers said, and what was done about it

**Per-action crossing was the problem, and it was measurable.** Six Spark
actions crossed individually and the small ones took about four seconds each —
`spark_schema` 4.0s, `spark_sql` 3.8s and 4.3s, `spark_table` 4.0s — for
statements that are a line of DDL. That is submission overhead, not work: six
actions paid roughly twenty-four seconds of pure transport, which is most of the
regression against shipping an archive.

**So the physical effect is batched while the semantic unit is not.** A batch's
Spark actions cross in one submission; each still gets its own result, timing and
status, so nothing above can tell they shared a trip. That is §6.10's own
prescription — *keep InstallAction as the semantic unit but allow the executor
layer to batch compatible physical effects; do not force one InstallAction = one
network call* — applied because the timings asked for it rather than in advance.

Install fell from 78.0s to 60.0s. Five crossings remain, one per batch that
contains Spark work, and each still carries about four seconds of fixed cost.
Batches are target-bound and sequences are barriers, so five is the floor
without changing what a batch means.

## Does removing the unpack pay for the transport?

**On this estate, not quite: 60.0s against 51.2s.** The honest answer is that
the desktop install is about nine seconds worse here, and it is worth being
precise about why that is not the whole story.

The archive's cost scales with the *deployed tree* — pack, upload, unpack. The
crossing count scales with the *number of batches containing Spark work*, which
is structural and does not grow with the repository. This estate has a handful of
small Python files, so the archive was nearly free and the fixed transport cost
dominates. A repository with a substantial `lib/` tree pays the archive on every
build and pays the same five crossings, so the balance should tip the other way —
and that is a prediction, not a measurement, because the estate to test it on
does not exist yet.

What is *not* in doubt is the attribution: `Install` was one opaque number and
is now five, each naming what it ran. The 29.2s `spark_sql_batch` is a fact
somebody can act on.

## One sample

One run of each shape, on a small estate, with `read_catalogue` varying 23.8s to
27.2s between otherwise identical runs. Treat the differences under about ten
seconds as noise.

---

# Phase 6 has a prerequisite: the markers no longer mean what they say

Before any test moves from `hosted` to `remote`, the two words have to be
settled, because the decomposition broke the distinction they were built on.

`AGENTS.md` currently promises:

```text
pytest -m "fabric and remote"   # Weaver runs here; no published wheel
| remote | Weaver runs on this machine and reaches into Fabric |
| hosted | Weaver runs inside Fabric as the wheel in the Environment |
```

Those were the same question when an operation ran entirely on one side. They
are not any more. A decomposed `weaver load` **orchestrates here and imports the
published wheel there** — `weaver.run.remote` and
`weaver.build_bundle.remote` are in the Environment's package, not in a body the
desktop submits. So the new product path is:

```text
Weaver runs on this machine        → remote, by the table
requires the published wheel       → hosted, by the parenthesis
```

The plan already decides which half wins. §7.2, on `await_addressable`:

> The `await_addressable` test may still use Livy, but that does not make it
> hosted: Weaver orchestration remains in the desktop test process.

So **`hosted` is about where the orchestration is, not about whether the wheel is
imported** — and the "no published wheel" gloss on `remote` is what has to go.
That is a documentation change with teeth: it means `-m "fabric and remote"`
starts requiring Weaver to have been published to its Environment, which is a
real cost the marker
was explicitly promising to avoid.

Worth deciding deliberately rather than discovering per test, because it changes
what a contributor can run without publishing. Two coherent answers:

1. **Follow the plan.** `remote` means orchestration here; drop the "no published
   wheel" promise and say the far-side helpers need the Environment current.
   Simple, and matches where the product went.
2. **Keep the promise and split the marker.** Something like `remote` for
   genuinely wheel-free crossings (REST, OneLake, TDS, a Spark body that imports
   no Weaver) and a third word for decomposed operations. More honest about cost,
   but a third marker earns its keep only if somebody actually runs the subsets
   separately.

## What is ready to move, once that is settled

By the plan's rule — hosted only where the claim depends on Fabric-in-process
semantics — the current `hosted` suite divides cleanly:

```text
stays hosted      test_developer_load_primitive   the wheel's own API in a session
                  test_authored_object_attachment reads the session's attachment
                  test_published_weaver           about the published wheel
                  test_livy_import                the import protocol itself

becomes remote    test_bundle_can_install         a desktop Installer now does this
                  test_lakehouse_journey          desktop-driven build lifecycle
                  test_load_orchestration_cycle   desktop Runner, TDS + Livy
                  test_item_catalogue_fabric      catalogue read from the desktop
                  test_alias_discovery            §7.2 names this one explicitly
```

`test_alias_discovery`'s own docstring is already stale for a second reason: it
says the discovery wait "is guarded by ``context.spark is not None``, so an
action executed from a desktop — where there is no session — skips it entirely".
The alias action now crosses whole, so the wait does run. The claim is sound; the
reasoning under it describes a Weaver that no longer exists.

Each of these is a rewrite rather than a marker change — `test_bundle_can_install`
builds its Installer inside a submitted Livy body, and moving it means driving
the Installer from the checkout and keeping one observation crossing. That is
the work, and it should not start until the markers mean something.

---

# The largest thing in the loop was a catalogue delete — 2026-08-11

Across every measured journey the biggest single Livy call was not a build, a
load or an install. It was `unbind`, the catalogue deletion at the tail of a
wipe:

```text
  livy.unbind                 1 calls    106.5s
  livy.install.spark          5 calls     48.5s
  livy.acquire                1 calls     36.4s
  livy.read_catalogue         1 calls     27.2s
```

`prune_installation` rendered one `DELETE` per catalogue table **per
installation**, and a wipe of this estate covers three logical items — so
thirty-three statements where eleven say the same thing. `InstallationScopes`
already existed for exactly this and unbind was not using it.

```text
one scope at a time : 33 statements
all scopes together : 11 statements
```

Measured on Fabric: **106.5s → 82.9s**, about 23 seconds.

## What the number says that the statement count did not

A 3× reduction in statements bought 1.3× in time, so the cost is *not* mostly
per-statement overhead as assumed — before, thirty-three deletes averaged 3.2s
each; after, eleven average 7.5s. Whatever dominates scales with the work a
delete does rather than with how many are issued, which means further batching
will not help much and the next question is a different one: what those eleven
Delta transactions actually spend their time on, and whether an estate that is
being wiped needs row-level deletes at all rather than dropping and recreating
the catalogue tables.

Recorded rather than pursued, because the guess that produced a 23-second win
was wrong about *why* it won, and the next step should start from a measurement
instead.

For a developer the wipe now reads: about 40s waiting for the Livy session it is
the first command to need, and about 83s deleting catalogue rows. The first is
paid once per `weaver session`; the second is paid per wipe.

---

# The four seconds were not transport — 2026-08-13

The section above attributes about four seconds per install action to submission
overhead, and roughly twenty-four seconds of "pure transport" to six actions
crossing individually. A probe against the pytest workspace measured the wire
directly, and it does not cost that.

| Question | Result |
| --- | --- |
| Warm round trip, trivial statement | 0.59s mean over 8 calls, range 0.50s to 0.71s |
| Session start, paid once | 37.9s |
| Eight statements in one submission | 3.82s, against 4.8s submitted individually |
| Submission overhead, by difference | about 0.11s per statement |

So a warm submission costs about 0.6s, and each extra statement inside one costs
about 0.11s. The repository's own later measurement agrees: per-node dispatch in
the Run decomposition is around a second on the same mechanism.

Whatever the remaining three seconds were, they were not the wire. The likely
answer is the work the far side did per submission — `install_actions`
constructed an `Installer` and re-resolved every target the plan declared before
running an action — but that is read from the code rather than measured, and the
code is gone, so it stays a reading.

## What followed from it

Cross-action batching existed to avoid a cost that turned out to be about 0.11s
per statement, so it was removed with the routing it belonged to. Every build
action now runs in the Installer wherever that is, and reaches Spark through the
Session. Statements belonging to *one* action still travel together, because
that is semantic rather than economic: a setup registering a temporary view and
the query that reads it only mean anything in the same session.

The two positions this leaves are worth stating plainly, because the earlier
sections predate them:

```text
install actions   no wheel required — statements cross, nothing imports Weaver
build as a whole  wheel still required — reading the catalogue and the target
                  inventories crosses as a program that imports Weaver
```
