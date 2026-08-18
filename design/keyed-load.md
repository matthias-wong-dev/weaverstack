# The keyed table load

How Weaver reconciles a table that declares a primary key, in both engines. A
table with no key is replaced wholesale and none of this applies to it.

Two implementations, one model. `weaver.declaration.tsql_load` generates a
Warehouse procedure; `weaver.runtime.table_load` runs the Delta path for both
Python-authored and Spark-SQL-authored tables. Where they differ is physical.
Where they differ semantically, one of them is wrong.

## The state machine

```text
raw staging                      the authored output, business columns only
      ↓
discover every refusal           one statement
      ↓
the rejection gate               fault_tolerant decides whether to continue
      ↓
purge the refused rows           staging becomes the clean incoming state
      ↓
the delete set                   from clean staging, or the explicit claim
      ↓
the upsert set                   new and signature-changed rows, nothing else
      ↓
merge uniqueness                 incremental with unique keys only
      ↓
the stability gate               how much of the target is about to move
      ↓
delete, update, insert
```

Every gate is reached before anything is written, so refusing is a decision not
to start rather than an unwind. Each phase is *settled* before the next reads it:
that is what lets a gate read the size of a change before a row moves, and it is
a property of the model rather than of any way of storing one.

## Two refusals, and they are different in kind

A **bad incoming row** is recoverable. It is refused with the reason, and
`fault_tolerant` decides whether the surviving rows load or the whole load stops.
Four reasons, shared by both engines
(`weaver.runtime.load_contract`):

| reason | what it means |
|---|---|
| `blank_primary_key` | the key is null, empty or only whitespace |
| `null_column: <name>` | a column the declaration marked `Not null` was empty |
| `duplicate_primary_key` | another incoming row already claimed this key |
| `duplicate_unique_key: <columns>` | another incoming row already claimed this key's value |

One reason per refused row. A row that is wrong twice over is still one row the
load will not take, and counting it twice would let it weigh twice against the
rejection threshold.

A set of **proposed changes that would leave the target invalid** is not
recoverable. There is no row to refuse — each incoming row is fine — so the load
stops before it writes anything. This is fatal whatever `fault_tolerant` says:
that governs incoming rows, and this is the target's own validity.

## Validation is sequential, and one statement says so

The stages are ordered — unusable rows, then the primary key, then each unique key
in declaration order — because a row an earlier stage refused must not go on to be
the arbitrary survivor of a later group. Both engines express the whole thing as
one chain of common table expressions, where each unique key reads the relation
the ones before it left. Splitting it would mean either mutating staging before
the gate or reading a half-written reject table to find out what had been refused.

Every scan is narrow. A duplicate is found by grouping (`group by … having
count(*) > 1`) and the only window is over rows already known to sit in a
duplicate primary key group. Nothing ranks the whole staging population.

Which row survives a duplicate group is arbitrary and the declaration does not
order them, so tests assert cardinality and validity, never which row it was.

A null in a unique key tuple does not claim the value: a null is not a value, so
two rows carrying one are not two rows claiming the same thing. `group by` would
put them in one group, which would refuse rows the declaration permits.

## The purge

Only reached when something was refused, so an ordinary load performs no staging
write at all. Both engines reproduce exactly what discovery decided, and each does
it the way its engine allows.

**Warehouse** deletes in stages: the unusable rows, then the duplicate keys, then
each unique key in turn — so each unique key sees the staging table the ones
before it have already been taken out of. The duplicate-key step deletes through a
ranked common table expression, because rows sharing a key may be identical in
every column and no predicate can tell one of them from the other. Fabric allows
that only when the expression reads a single base table, so the rank cannot be
narrowed by joining; it is narrowed by not running unless a duplicate was found.
It is ordered by the row signature, so the row it keeps is the row discovery kept.

**Delta** cannot delete through a ranked expression at all, so it assembles the
survivors from the same chain discovery ran, and that relation becomes clean
staging from there on. Nothing is deleted and nothing is overwritten: the raw
relation is superseded rather than edited, and it is released once the survivors
are settled and its evidence written.

## The delete set

Non-incremental, it is the target keys clean staging no longer carries — read
after the purge, so a target row whose only staged proposal was refused is retired
by the same rule as any other absence, and no later repair pass is needed.

Incremental, it is the object's explicit claim, narrowed twice. First to keys the
target actually holds, because a delete for a row that was never there is not a
deletion and counting it would make the stability guard protect against work the
load was never going to do. Then, once staging is clean, to keys the source no
longer produces.

That second narrowing is what settles a key that is both claimed and staged. The
source still producing a row means the row stays, whether or not it changed — so
the claim gives the key up and the row is loaded as an ordinary upsert. Deleting
and re-inserting it would reach the same contents, but it would reset the row's
insert time and rewrite a row that may not have changed at all.

Narrowed after the purge, so a staged row that was refused does not protect the
key it named.

## The row signature

A keyed target carries one internal column holding a SHA-256 digest of the row's
comparison columns, and change detection is one equality test against it. It
replaces a correlated full-row `EXCEPT` on the Warehouse and a per-column
null-safe comparison on Delta.

*What* is hashed is `comparison_columns` — every non-key business column unless
the declaration named fewer. Not the audit columns, which move on every write, and
not the signature itself.

*How* each value is written has to be unambiguous, because both ways of getting it
wrong are silent — the row never updates. Each value enters the payload as
its length, a colon, and its canonical text; a null enters as `~`, which no
present value can produce because a present value always begins with a digit. So
a null, an empty string, and text containing whatever separator was chosen are all
distinct.

A type whose default text is not stable is spelled explicitly: a timestamp's
default rendering moves with the session time zone, and a boolean's or a binary's
with the cast the engine happens to choose. A signature that moved with the
session would report every row as changed.

The two engines are **not** required to produce the same bytes. A Warehouse stores
`varbinary(32)` from `hashbytes`; Delta stores the hex text `sha2` returns. A
signature is only ever compared with another signature from the same table. The
Warehouse payload is assembled at install time rather than by the generator,
because it names each column's physical type and an inferred table's types are
settled by the build.

Not every keyed table has a load. `Has load procedure: false` says something other
than Weaver populates this one, and such a table gets neither a load artefact nor
a signature column, because both exist to serve a load it does not have — what it
declares is a structure. Weaver's own catalogue tables declare it: they hold a
primary key and are written by the catalogue's DML. Giving them a signature column
made every catalogue publication fail on a not-null violation, and `Prohibit
rebuild: true` meant an installed one could never acquire the column anyway.

SQL and Spark SQL can both declare a structure that way. A Python table cannot,
and says so: its authored module is the load, so there is no separate artefact to
decline.

The column is physical, so introducing or changing it has to rebuild the tables
that carry it. `SourceDocument.physical_signature` salts the source hash with
`KEYED_TABLE_VERSION` for exactly the tables that carry the column, and the
desired catalogue and incremental selection read the same value. A table with no
signature column gains nothing and is not rebuilt.

## Merge uniqueness

Run only when the load is incremental **and** the table declares at least one
unique key. A non-incremental load never asks: it leaves the target equal to clean
staging, and staging has already been made unique.

One question:

> If every surviving delete and upsert were applied, would a declared unique key
> still be held by another target row?

A holder gives up its value in exactly two ways. The load deletes it, or the load
moves it off that value — which includes moving to a null, because a null claims
nothing. Being in the upsert set is not one of them: a row may be changing another
column entirely and keeping the value it has.

So these pass:

```text
the holder is explicitly deleted
the holder moves off the value
a two-way swap
a multi-row cycle whose proposed state is unique
```

and these stop the load:

```text
a claim on a value an untouched target row holds
a claim on any one of several declared keys
```

Any remaining collision stops the whole load before it writes. There is no partial
application, no closure to compute, and no fixed point to iterate towards: the
proposed target state is either valid under the declared keys or it is not.

One case the plan behind this work listed cannot actually arise. A holder that is
in the upsert set while keeping the value a claimant wants means both rows carry
that value in staging, which incoming uniqueness refuses first. The predicate
still distinguishes the two, because that is what lets a genuine swap through.

## Working state, and the physical strategies that differ

The model needs each phase *settled* before the next one reads it. It does not say
that a phase has to be a table, and the two engines answer that differently
because their engines make different things cheap.

**Warehouse** uses physical working tables, `_Staging`, `_Reject`, `_Delete` and
`_Upsert` in the object's own schema, because they are its native execution
state. A procedure has nowhere else to put a settled relation, the purge deletes
from staging in stages, and a T-SQL batch cannot hold a relation across
statements any other way.

**Delta/Spark** uses persisted Spark relations. Each phase is one statement whose
result is cached and named by a temporary view, so the next phase reads exactly
the rows this one settled. Nothing durable is written, and every relation is
released in a `finally`: on a clean load, on a refusal at any gate, and on an
unexpected failure.

A Delta table per phase was execution machinery rather than a requirement of the
model. Each cost a write, a transaction-log commit, a metadata refresh and a drop,
for state nothing outside the load ever read, and the fixed cost dominated
entirely at the row counts a typical load moves.

Being persisted rather than durable is a real difference, and it is bounded. A
lost cache is recomputed from the source, not lost, and by the time the target is
being mutated staging is no longer read: the delete and upsert relations are
settled and counted before the first write. The one path that does write staging
to Delta is the unkeyed wholesale replace, because it empties the target before
inserting, so it is the only phase whose source could be read after the table it
may depend on is gone.

## Diagnostic evidence

Separate from execution state, and that is the distinction the Delta path makes
explicit: a durable artefact exists only when an outcome has something to
troubleshoot.

```text
_Staging   what did the source propose?
_Reject    what did Weaver refuse, and why?
_Delete    what was Weaver proposing to remove?
```

A clean load writes none of them. A run that refused rows writes staging and the
rejects, because the rejects alone do not explain themselves. A run that stopped
at the stability gate or at merge uniqueness writes what it was proposing. Each is
written once, and staging is written before the purge supersedes it.

A run can also end in a failure Weaver has no outcome for: an engine error while
it was reconciling or writing the target. The load tracks the relations it has
settled as it goes, and on that exit it writes whichever of them are not already
written: staging once it is materialised, the rejects and the delete set where
they exist, and never the upsert set, which describes work rather than a
proposal. The failure on its way out is the one worth reporting, so a write that
cannot be made is left out and the original error is raised unchanged. The
relations are released afterwards as they are on every other exit.

Stale evidence from an earlier faulted run is dropped before a new run for that
object writes any of its own, so what stands afterwards describes the run that
just finished. Attempted rather than looked up: a missing table is the ordinary
case, and reading an inventory to avoid asking for a drop that does nothing would
cost more than the drop.

There is no upsert artefact, no affected-key table, no loser table per constraint,
no participant table and no merge-conflict recovery table. What Weaver was going
to write is answerable from staging and the delete set; the rest are
implementation mechanics, not artefacts.

## Where it is proved

Rendering and contract claims run on every commit
(`tests/targeted/test_load_representation.py`,
`test_load_contract_declaration.py`, `test_row_signature_representation.py`).

What the Delta path *submits* also runs on every commit, against a double that
records statements and answers cardinalities
(`tests/targeted/test_delta_load_execution_boundary.py`): that a clean load writes
no working tables, that a phase which decided on no rows submits no mutation for
it, that the relations are released whatever happened, and that evidence appears
only for an outcome that owes one, including an injected engine failure, whose
evidence and release are asserted there too. It evaluates nothing, so what a
statement *means* is not asked there.

Behaviour needs an engine, and both are exercised against a real tenant:
`tests/fabric/test_warehouse_load_primitive.py` and
`tests/fabric/test_delta_table_load_primitive.py` run the same matrix, claim for
claim. If they disagree the model has diverged.

Large-scale performance is manual and lives outside the suite. The benchmark work
behind this design established that narrow grouped scans beat global staging
windows and that a large target join is cheap in Fabric Warehouse. None of that
belongs in a CI threshold.

What the suite's own external-resource telemetry showed when the Delta path moved
off durable working tables, on the desktop journey's three keyed loads of three
rows each:

| | before | after |
|---|---|---|
| `DWG.NamedCustomer` | 95.5s | 21.6s |
| `DWG.Order` | 86.3s | 24.6s |
| `DWG.Customer` | 67.6s | 21.2s |
| the journey's Livy time | 317.6s | 127.8s |

Livy submission counts are identical, which is the point: the cost was inside one
submission, in the writes and commits that carried state nobody read. Treat these
as implementation evidence. Fabric timing is noisy and nothing here is asserted.
