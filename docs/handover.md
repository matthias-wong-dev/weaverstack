# Handover

Where weaverstack stands, what is proven, and what comes next.

## The one thing to read first

[`AGENTS.md`](../AGENTS.md) opens with **the core abstraction** — two independent
axes, *where things are* (the host) and *where the code runs* (the executor).
Nothing else here makes sense without it. The short version:

| | host | code runs | what it is |
|---|---|---|---|
| 1 | Local | laptop | development, most of the suite |
| 2 | Fabric | laptop | the desktop CLI |
| 3 | Fabric | **in Fabric** | **the product** |

All three are now proven. Row 3 — `import weaver` succeeding inside a Fabric
Spark session — was the open question, and it is answered.

## What is proven

**Row 1.** 510 tests, under a second, no JVM and no tenant. Plus 15 local
Spark/Delta tests behind `-m spark`.

**Row 2.** Fabric tests behind `-m fabric`: capacity, workspace and item
resolution, OneLake as a `Store`, desktop `mssql-python`, and independent
inspection of disposable Lakehouses and Warehouses.

**Row 3.** Weaver shipped into a workspace, imported inside a Livy session, and
used there — including the actual Warehouse wipe through Fabric-session
authentication.

## The vertical slice that exists

```text
weaver capacity resume …                 turn Fabric on
weaver wipe --lakehouse-target X --host MyFabric   clear a Lakehouse remotely
weaver wipe --warehouse-target X --host MyFabric   clear a Warehouse remotely
weaver capacity suspend …                turn it off
```

and, inside Fabric:

```python
import weaver                             # from the shipped copy, or a Fabric Environment
weaver.wipe(host, folder_target=…)        # the same code, running there
```

## Checkpoint position

Roughly **checkpoint 6**, with **wipe as the guinea pig** — the feature chosen
to prove the substrate end to end before build and load are written on top of
it. Done: 0 to 6, plus much of 7 and 9 from the plan's numbering, out of order.

| | | |
|---|---|---|
| 0–2 | skeleton, vocabulary, resolution and transport | done |
| 3–5 | the SES contract, authoring, the repository reader | done |
| 6 | dependency extraction and the graph | done |
| 7 | Fabric resources, OneLake, Livy | done, ahead of order |
| 9 | capacity, sync | done, ahead of order |
| 8 | generic program execution | partly — `LivySession` exists |
| 10 | `mssql-python` | done |
| 11–16 | the build package | one piece of work, not six |

## Next

**1. Confirm the desktop CLI against a remote host.** The SQL and storage
capability boundaries are now in place, so this is the same calls with argument
parsing in front.

That leaves three foundations in place: a local Lakehouse for testing, a
standard way of installing Weaver remotely, and a suite that creates, populates
and wipes both Lakehouses and Warehouses. Simple, and everything else builds on
them.

**Then the build package** — checkpoints 11 to 16 as one piece. The shape is
settled: `build` copies the repository into the Weaver Lakehouse, then
`generate_build_package` writes a folder of ordered scripts to inspect, and
`install_build` runs them. Incremental build by signature comparison is
deliberately deferred.

## Facts learned by running, not by reasoning

Each of these cost an experiment and would have cost more as a wrong assumption.

**A Livy session has no FUSE mount.** `/lakehouse` exists but is empty, unlike a
notebook where the default Lakehouse appears at `/lakehouse/default`. The
bootstrap therefore copies the package from its explicit `abfss` root with
`notebookutils.fs.cp` before putting it on `sys.path`. That works in a notebook
too, so there is one bootstrap rather than two.

**OneLake does not support directory rename.** `x-ms-rename-source` returns
`400 UnsupportedHeader`, so `move_within_store` copies there. The operation
stays whole so a session-side implementation can do better.

**A Lakehouse grows a `SQLEndpoint` sibling** of the same name shortly after
creation, so item names are unique *per type*, not across types.

**Fabric holds a deleted item's name for minutes** (`409
ItemDisplayNameNotAvailableYet`).

**`DefaultAzureCredential` does not always settle on the identity you are signed
in as**, so `AZURE_TOKEN_CREDENTIALS=AzureCliCredential` is pinned.

**A capacity resume takes about 30 seconds** and the ARM call returns before the
state changes.

## Open questions

Carried in [`journal.md`](journal.md) with the reasoning; the ones that will
bite soonest:

- `--delta-target` or `--spark-target`? The command sketch says Spark, the code
  says Delta.
- Does deleting a `Tables/` directory de-register the table from the Lakehouse
  metastore, or leave a phantom? If it leaves one, Delta wipe needs `DROP TABLE`
  through Spark and stops being a pure remote operation.
- Can `deltalake` (delta-rs) write Delta to OneLake without a JVM? If so,
  catalogue writes and empty-table creation stop needing a Spark session at all.

## Running it

```bash
python3.11 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest                       # 473, under a second
.venv/bin/python -m pytest -m spark              # 15, needs Java
.venv/bin/weaver doctor                          # what this machine can do

.venv/bin/weaver capacity resume --resource-group <rg> --capacity-name <cap>
WEAVER_FABRIC_WORKSPACE=<workspace> .venv/bin/python -m pytest -m fabric
.venv/bin/weaver capacity suspend --resource-group <rg> --capacity-name <cap>
```

See [cli-usage.md](cli-usage.md), [local-setup.md](local-setup.md) and
[fabric-testing.md](fabric-testing.md).
