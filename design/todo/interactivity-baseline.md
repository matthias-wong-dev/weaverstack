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
