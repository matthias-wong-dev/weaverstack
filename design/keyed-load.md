# The keyed table load

How Weaver reconciles a table that declares a primary key, in both engines. A
table with no key is replaced wholesale and none of this applies to it.

Two implementations, one model. `weaver.declaration.tsql_load` generates a
Warehouse procedure; `weaver.runtime.table_load` runs the Delta path for both
Python-authored and Spark-SQL-authored tables. Where they differ is physical.
Where they differ semantically, one of them is wrong.

## The state machine

```text
raw _Staging                     the authored output, business columns only
      ↓
discover every refusal           one statement, into _Reject
      ↓
the rejection gate               fault_tolerant decides whether to continue
      ↓
purge the refused rows           _Staging becomes the clean incoming state
      ↓
_Delete                          from clean staging, or the explicit claim
      ↓
_Upsert                          new and signature-changed rows, nothing else
      ↓
merge uniqueness                 incremental with unique keys only
      ↓
the stability gate               how much of the target is about to move
      ↓
delete, update, insert
```

Every gate is reached before anything is written, so refusing is a decision not
to start rather than an unwind.

## Two refusals, and they are different in kind

A **bad incoming row** is recoverable. It goes to `_Reject` with the reason, and
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
survivors from the same chain discovery ran and overwrites staging with them.
Through a table rather than straight from staging: overwriting a table from a read
of itself is not something Spark guarantees.

## The row signature

A keyed target carries one internal column holding a SHA-256 digest of the row's
comparison columns, and change detection is one equality test against it. It
replaces a correlated full-row `EXCEPT` on the Warehouse and a per-column
null-safe comparison on Delta.

*What* is hashed is `comparison_columns` — every non-key business column unless
the declaration named fewer. Not the audit columns, which move on every write, and
not the signature itself.

*How* each value is written has to be unambiguous, because both ways of getting it
wrong are silent — the row simply never updates. Each value enters the payload as
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

The column is physical, so introducing or changing it has to rebuild the tables
that carry it. `SourceDocument.physical_signature` salts a keyed table's source
hash with `KEYED_TABLE_VERSION`, and the desired catalogue and incremental
selection read the same value. An unkeyed table gains nothing and is not rebuilt.

## Merge uniqueness

Run only when the load is incremental **and** the table declares at least one
unique key. A non-incremental load never asks: it leaves the target equal to clean
staging, and staging has already been made unique.

One question:

> If every surviving delete and upsert were applied, would a declared unique key
> still be held by another target row?

A holder gives up its value in exactly two ways. The load deletes it, or the load
moves it off that value — which includes moving to a null, because a null claims
nothing. Being in `_Upsert` is not one of them: a row may be changing another
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
in `_Upsert` while keeping the value a claimant wants means both rows carry that
value in staging, which incoming uniqueness refuses first. The predicate still
distinguishes the two, because that is what lets a genuine swap through.

## The working artefacts

`_Staging`, `_Reject`, `_Delete` and `_Upsert`, in the object's own schema beside
it. A run that refused nothing drops them; a run that refused rows keeps them all,
because the reject table alone does not explain itself. A run that stopped at a
gate never reaches the cleanup, so its tables stand too.

There is no affected-key table, no loser table per constraint, no participant
table and no merge-conflict recovery table. Constraint-specific expressions are
implementation mechanics, not artefacts.

## Where it is proved

Rendering and contract claims run on every commit
(`tests/targeted/test_load_representation.py`,
`test_load_contract_declaration.py`, `test_row_signature_representation.py`).

Behaviour needs an engine, and both are exercised against a real tenant:
`tests/fabric/test_warehouse_load_primitive.py` and
`tests/fabric/test_delta_table_load_primitive.py` run the same matrix, claim for
claim. If they disagree the model has diverged.

Large-scale performance is manual and lives outside the suite. The benchmark work
behind this design established that narrow grouped scans beat global staging
windows, that a large target join is cheap in Fabric Warehouse, that helper-table
materialisation has a real fixed cost, and that `_Upsert` is the useful
materialisation boundary. None of that belongs in a CI threshold.
