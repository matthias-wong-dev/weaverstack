# Build journal

A running record of what weaverstack actually is, and why.

## How this relates to the plan

[`backlog/weaverstack-step-by-step-implementation-plan.md`](../backlog/weaverstack-step-by-step-implementation-plan.md)
is a **guide**, written before construction started. This journal is the
**record**, written as construction happens.

Where the two disagree, this journal is right and the plan is stale. The
checkpoint numbering is kept because it is a useful spine, but scope has already
moved and will keep moving.

## What is actually being built

Weaver is not a new idea being invented here. The underlying system —
the object contract, the backing table and view shape, the generated load
procedure, the reconciliation semantics — has run in production on SQL Server
for years and is battle-tested. The first weaver implementation established that
the same model works on Microsoft Fabric: OneLake, Spark, Delta and Warehouse.

**Weaverstack is therefore implementation, not invention.** Two consequences
worth holding onto:

1. Where a proven algorithm exists, port it. The SQL DDL and ETL generation
   especially encode years of accumulated correctness. The plan's caution
   against "blindly preserving every legacy detail" means *don't carry
   incidental structure*; it does not mean the semantics are open for redesign.
2. What is genuinely new is the **control plane** — central catalogue,
   central repository installation, one global dependency graph, per-object
   certification. That is where design attention belongs.

---

## Standing architecture

**Four levels, named as SQL names them.**

| Level | Fabric | Local |
|---|---|---|
| 4 | workspace | root directory |
| 3 | Lakehouse / Warehouse / Environment | subdirectory |
| 2 | schema | schema directory |
| 1 | table, view, folder, procedure | table or folder |

Level 4 is the only level written down. Level 3 needs no configuration because
an item is uniquely identifiable within its host — so it is named directly.
Uniqueness, **not** invariance: promoting one Lakehouse to another inside one
workspace is ordinary, so level-3 names are always supplied explicitly at the
call site and never inferred.

**The host decides where work executes**, not where it was requested.
`--to MyFabric` runs in that workspace whether invoked from a notebook or a
desktop shell; only the transport differs.

**Three transports, each with one jurisdiction.**

| What | How |
|---|---|
| files and directories | OneLake DFS REST — identical from desktop and inside Fabric |
| Delta tables | Spark with explicit `abfss://` roots |
| Warehouse | `mssql-python` |

The Fabric FUSE mount (`/lakehouse/default/…`) is never used. It only exposes
the *attached* Lakehouse, which is precisely the dependence being removed.
weaver's `runtime/load.py:415` documents relying on it for Folder I/O; that is
the coupling weaverstack breaks.

**Config is a convenience, never a layer.** Every host is constructible in
Python. The `hosts:` file is a named lookup that can express nothing the
constructors cannot — asserted by test.

---

## Log

### Checkpoint 0 — skeleton

`weaverstack` distribution, `weaver` import, Python 3.11, hatchling.

**Core and CLI are separate top-level packages**, CLI behind an optional extra.
The one-way dependency is then enforced by packaging: a core import of
`weaver_cli` breaks any install that did not ask for the CLI. A convention plus
a lint rule would not have that property.

**One error hierarchy.** weaver had two unrelated roots (`CommandError(ValueError)`
and `WeaverError(Exception)`); everything here descends from `WeaverError`.

**Dependencies are declared at the checkpoint that needs them**, not in advance.
Base install is `pyyaml`.

### Checkpoint 1 — vocabulary

The correction that mattered: level 3 is *uniquely identifiable*, not
*invariant*. An earlier draft claimed level-3 names stay the same across
environments, which would have forbidden same-workspace promotion — a normal
deployment. Uniqueness is what removes the need for configuration; invariance
was never required and would have been a real constraint.

**Kind comes from the slot, never the string.** `DeltaTarget.parse("Shared")`
and `WarehouseTarget.parse("Shared")` produce the same `ItemRef`. What an item
must *be* is decided by the parameter it is passed to.

**`Files` is written, `Tables` is implicit.** Asymmetric on purpose: `Files` is
what a user sees in the Fabric UI, and a folder target may carry a subpath
beneath it. A Delta target names a Lakehouse and the area follows from the
object kind.

**Host entries are keyword-argument bags.** `configurable_keys()` derives from
the record, so a new host field is configurable with no parser change, while an
unknown key is still refused by name. Open in what it accepts, closed against
typos.

### Checkpoint 2 — resolution and transport

Wider than the plan's version, deliberately: the plan scoped this to local path
arithmetic, but the *type* that crosses host boundaries had to be settled before
anything consumed it.

**`Location`, because `pathlib` cannot be the currency.**
`Path("abfss://ws@host/lh")` collapses the double slash and returns a broken
root with no error. Locations always join by string; `.path` is available only
when the location is genuinely a filesystem path. There is a test asserting the
corruption, so the reason survives.

**`Store` is transport, never policy.** `move_within_store` is one operation,
not read + write + delete. Within a Lakehouse a move is a metadata rename, and
an implementation can only choose that if the intent survives the call.
Listing returns size and modification time, because every incremental strategy
needs them.

**No generic `sync()`.** Push, deployment and Folder reconciliation differ in
*deletion policy* — push owns its destination subtree and deletes what is
missing; reconciliation deletes only within its `File key` scope, and under
`Incremental` deletes nothing. Collapsing those into `sync(delete_missing=…)`
puts a data-correctness decision behind a transport flag. weaver keeps them in
separate modules (`fabric/transfer.py`, `runtime/folders.py`) and that line
holds.

**Staging, provisionally.** The author writes into a real local temp directory
(true `Path`, any library), Weaver uploads to the lakehouse staging sibling,
then promotes staging → destination by rename. Three legs, three mechanisms.
The lifecycle is not settled; the *paths* are.

### Checkpoint 3 — the SES contract

The heart of the system: a contract validated to exhaustion before anything
physical happens.

**Unknown keys are refused by name.** The highest-value guard and absent from
weaver. A mistyped `Primary Key` previously parsed as *no primary key*, which
silently converts an upsert into a full replacement — data loss presenting as
"why did the table shrink".

**References are whole-value or nothing.** `$Sales.Order` and
`$Sales.Order[Order date]` resolve to the target's corresponding field, so the
field being resolved decides what is fetched and no direction marker is needed.
`See $Sales.Order` is refused: a contract that is only sometimes
machine-readable is not a contract. `$$` escapes a literal dollar. Resolution
itself needs sibling documents and waits for the repository reader — including
cycle detection, since the lookup is recursive.

**Column sets are comma-separated; column lists are YAML lists.** `Primary key`
and `Comparison columns` are *sets* — one key, one comparison tuple. `Not null`
is several independent facts. The distinction is semantic, so the syntax marks
it.

**Audit columns follow the representation.** `Row insert/update/delete datetime`
are never authored. Warehouse keeps the spaced form already in weaver's
`sql/ddl.py`; Delta uses underscores. A live row carries a sentinel delete
datetime, hence not-null. `schema` stays exactly what the author wrote;
`effective_schema` adds them.

**Validation deferred is recorded, not skipped.** A Warehouse object infers its
shape from its query, so its column references cannot be checked here.
`defers_column_validation` says so rather than leaving the distinction in
someone's head.

**`Load mode` retired.** Behaviour follows from `Incremental` and `Primary key`.

**Layout convention:** a blank line between subsections. Unenforced, but
documented and followed by fixtures — the header is the contract a reader meets
first.

---

## Open questions

| Question | Raised | Status |
|---|---|---|
| Which `weaver` revision is the port baseline — the plan's `a97ba8a` or current `fee2025`? | CP0 | open |
| Path-like *reader* for Folder dependencies during ETL. `staging_folder()` is the write side; the read side is unresolved. Likely: materialise to temp, hand back a `Path`. | CP2 | open, due CP4/23 |
| Does OneLake DFS implement ADLS Gen2 `x-ms-rename-source`? Determines whether desktop-initiated moves are cheap. Ten-minute experiment. | CP2 | open, due CP7 |
| Should `Identity` imply `Incremental: true`? Left free deliberately. | CP3 | deferred until identity is implemented |
| Control-table names, and whether they sit under a schema. | CP2 | due CP16 |
| Does `build` move any files at all? In the central architecture, source stays central and load imports it — the case may be empty. | CP2 | due CP12 |

## Divergences from the plan

| Checkpoint | Divergence |
|---|---|
| 2 | Widened to include the location type and the file-transport protocol. |
| 3 | Substantially extended: references, `Prohibit rebuild`, `Not null`, `Identity`, `Comparison columns`, `Column notes`, `Notes`, `Revision notes`, audit columns, unknown-key rejection. `Load mode` removed. |
