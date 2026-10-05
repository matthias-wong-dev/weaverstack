# Stress estate

A 3,000-object estate shaped like a heavy production warehouse, for measuring
what Weaver itself costs to build, load and test it. The authored code is
straight selects, so the time a load takes is Weaver's work and Fabric's, not
the estate's.

Two Weaver projects are generated:

| Project | Objects | Role |
|---|---|---|
| `source` | about 1,000 | Produces the data. Not measured. |
| `estate` | 3,000 | Reads the sources. What the results report. |

## The shape

Real estates are long-tailed. In a study of the Amazon Redshift fleet, each
decade of table size from one row to 10M rows held a similar share of tables,
and above that the share fell away. Most tables saw no inserts or deletes in
the period studied, and most tables that received inserts saw no deletes
([van Renen et al. 2024](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf),
tables 8 and 9). The sources follow that shape below 10M rows and cap it above,
so the estate measures Weaver rather than capacity:

| Source tier | Sources | Rows | Each change |
|---|---|---|---|
| 500M | 1 | 500,000,000 | appends 1,000,000 rows |
| 100M | 4 | 100,000,000 | appends, or updates and deletes |
| 50M | 12 | 50,000,000 | appends, or updates and deletes |
| 10M | 30 | 10,000,000 | inserts, updates, deletes |
| 1M to 1k | 913 | 1,000,000 to 1,000 | inserts, updates, deletes |
| folders | 40 | a file per change | a new file |

Sources of 10M rows or more deliver only what changed. Smaller sources deliver
their whole contents. A source changes every 1 to 20 days according to its
size, so a typical day changes about one source in seven.

The estate reads them through four layers in each of three items:

| Item | Layers | Languages |
|---|---|---|
| `Lakehouse/Lake` | landing, curated, refined | Python tables and folders, Spark SQL tables and views |
| `Warehouse/Core` | core, conformed | T-SQL tables and views |
| `Warehouse/Mart` | mart, serving | T-SQL tables and views |

Every table loads one of Weaver's ways:

| Behaviour | Declared as | Reads |
|---|---|---|
| append | `Incremental: true`, no primary key | rows newer than it holds |
| incremental | `Incremental: true`, `Primary key: Id` | rows newer than it holds |
| incremental with deletes | the same, with a delete query | rows newer, and the keys deleted |
| upsert | `Primary key: Id` | everything, so a missing row is deleted |
| replace | no primary key | everything, replacing what it holds |
| static | `Static: true` | everything, once |

A table over 10M rows only ever appends or merges what changed. An incremental
table never reads a parent that removes rows, because a removed row leaves no
change behind. Each item carries one Test per behaviour, which compares a small
table with what its parent says it should hold.

## The day

`Clock.Days` in the source Lakehouse gains one file each time the source
project loads, and every source computes its rows from that day. Loading the
source project again is therefore one simulated day of change, and the same
day always produces the same rows.

## Running it

Generate the projects. The names of the physical items have neutral defaults;
`--source-workspace` puts the sources in a workspace of their own.

```bash
python examples/stress/generate.py stress --workspace "Stress" --environment weaver
```

Then, with the published Environment named in `--environment`:

```bash
python examples/stress/run.py stress prepare
```

```bash
python examples/stress/run.py stress populate
```

```bash
python examples/stress/run.py stress measure --cycles 3
```

```bash
python examples/stress/run.py stress perturb --days 2
```

`prepare` creates the items. `populate` empties the sources, builds them and
loads day 0. `measure` empties the estate and then builds it, loads it in full,
builds and loads it again with nothing changed, and then perturbs the sources
and loads the estate once per cycle. `perturb` adds days on its own.

Each step prints one line and appends it to `stress/results.jsonl`: its time,
and for a load the nodes it ran, the rows it read and changed, node time
percentiles and any failures.

`--objects 300 --scale 0.01` generates a small estate that runs the same cycle
in minutes.

## Reading the results

- **Unchanged build and unchanged load** are almost entirely Weaver: the
  catalogue, planning, dispatch and each object's comparison with its source.
- **Day loads** show how load time grows with the rows that changed.
- **First load** is mostly Fabric moving rows, about 3.1 billion of them at
  scale 1.

A load runs at most four Python primitives at once, and four procedures per
Warehouse, which is why the T-SQL objects are split across two Warehouses.
