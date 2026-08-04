# A worked sales estate

One repository exercising every object form Weaver builds and loads, so the
whole chain can be read in one place:

The buildable repository is `repository/`; this README sits beside it because a
repository root contains only `Lakehouse/` and `Warehouse/`.

```text
Lakehouse/Sales
  Files/Sales__OrderExport.py    Folder        *.csv, incremental
  Sales__Customer.py             Delta table   Python, keyed, replaced
  Sales__Order.py                Delta table   Python, keyed, incremental
  Sales.OrderSummary.sql         Delta table   Spark SQL, keyed
  lib/dates.py                   helper        deployed, declares nothing

Warehouse/Reporting
  alias.yml                      makes the Lakehouse summary addressable
  Reporting.CustomerRevenue.sql  Warehouse table, keyed, identity
```

The names are deliberately generic. Nothing here says which Lakehouse or
Warehouse it lands in — that is decided at build time by `--bind`, which is why
the same repository builds into any estate.

## The chain

Files arrive in the export folder; the two Python tables read them; the Spark
SQL table summarises those; the alias carries the summary into the Warehouse,
where the reporting table adds a surrogate key for a BI model.

```text
Sales.OrderExport ──> Sales.Customer ──┐
                 └──> Sales.Order   ───┴──> Sales.OrderSummary
                                                   │  (alias)
                                                   └──> Reporting.CustomerRevenue
```

## What each declaration is demonstrating

**`Sales.OrderExport` is incremental** because the export is a nightly drop and
the sales system keeps only thirty days. A load adds tonight's file and must not
retire the ones before it.

**`Sales.Order` is incremental too**, and it is the interesting case: an order
missing from tonight's file has not been cancelled, it is simply older than the
window. So absence deletes nothing, and a cancellation is reported as an
*explicit* delete instead — the object stating that a row is gone rather than
Weaver inferring it.

**`Sales.Customer` is not incremental.** The export is the whole customer list
every night, so absence really does mean the customer went away.

**`Reporting.CustomerRevenue` declares an `Identity`.** That is a Warehouse-only
declaration: build emits `bigint identity`, the engine assigns the values, and
no load ever inserts into the column. The values are Fabric's to choose — not
consecutive, not ordered — so nothing may read sequence into them. A Delta table
cannot declare one, because no Delta version Weaver runs on generates them.

## Build it locally with the exact CLI

The local emulator has Lakehouses but no Warehouse SQL engine, so bind the Sales
Lakehouse item. These commands use only the checked-in example repository:

```bash
demo_root="$(mktemp -d)"
weaver initialise --exists-ok \
  --workspace-type local --workspace "$demo_root" --weaver-lakehouse Play_Weaver
weaver build examples/sales-estate/repository \
  --bind Lakehouses/Play_LH=Lakehouse/Sales \
  --workspace-type local --workspace "$demo_root" --weaver-lakehouse Play_Weaver
weaver wipe --lakehouse Play_LH --yes \
  --workspace-type local --workspace "$demo_root" --weaver-lakehouse Play_Weaver
```

## Build it in Fabric from the desktop

The same explicit repository builds both logical items into the named Play
destinations. Source stays on the desktop: the CLI reads Fabric state, plans
locally, and submits the completed build bundle for execution inside Fabric.

```bash
weaver build examples/sales-estate/repository \
  --bind Lakehouses/Play_LH=Lakehouse/Sales \
  --bind Warehouses/Play_WH=Warehouse/Reporting \
  --workspace PYTEST_WORKSPACE \
  --weaver-lakehouse Play_Weaver \
  --environment weaver
```

## Loading it

Build creates structure; load puts rows in it. Each primitive runs on its own,
with no orchestrator:

```python
Sales__OrderExport(spark).load()
Sales__Customer(spark).load()
```

```python
run_load_program(spark, installed_sql, fault_tolerant=False)
```

```sql
exec [_].[Load Reporting.CustomerRevenue] @fault_tolerant = 0;
```

Every one returns the same `LoadResult`: whether it succeeded, and how many rows
were read, inserted, updated, deleted and rejected.
