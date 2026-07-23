# Where your SES repository lives

An SES repository is **just a folder of files**. Weaver has no opinion about
where that folder sits, what its parent is called, or how it got there.

```text
sales-etl/
├── Sales__OrderExport.py          a Folder
├── Sales__Order.py                a Delta table
├── Sales.OrderSummary.spark.sql   a Delta table, in Spark SQL
├── Reporting.OrderReport.sql      a Warehouse table
└── _helpers/
    └── dates.py
```

That is the whole contract. Everything below is about how the folder reaches
Fabric, and all of it is optional.

## Build is what locks it in

Whatever route the files take, `build` copies them into the Weaver Lakehouse at
`Files/repos/<name>` and certifies **that copy**.

```text
your folder ──build──> Weaver/Files/repos/sales-etl ──load──> targets
```

This matters more than it looks. The snapshot is the artifact the catalogue
certifies against, so:

- editing your working copy after a build changes nothing until you build again;
- `load` never needs to know where the source came from, or reach back to it;
- a scheduled load, a Livy submission and a notebook all read the same copy.

So the routes below are not alternatives that behave differently. They are
different ways of putting files somewhere `build` can read.

## Route 1 — the CLI, from your machine

The ordinary one. Develop in VS Code, install the CLI, build.

```bash
pip install 'weaverstack[cli]'
weaver build --source ./sales-etl --host MyFabric --hosts env.yml …
```

Your local files go up to the Weaver Lakehouse and the build proceeds. Nothing
else is needed.

## Route 2 — a deployment pipeline

On merge to `main`, a pipeline puts the folder in place and runs the build. The
same command, run somewhere else.

## Route 3 — Fabric Git integration

Fabric's Git integration versions **items** — notebooks, reports, item metadata
— not the contents of a Lakehouse's `Files` area. So a repository sitting in
`Files/repos` is not Git-tracked by Fabric.

A notebook's **resources** are. If you want branches, pull requests and history
managed inside Fabric rather than around it, put the SES folder in a notebook's
resources and let the notebook run the build:

```python
%pip install weaverstack

import weaver
weaver.build(source=f"{resource_path}/sales-etl", host=..., delta_target="Sales_LH")
```

A notebook carrying its own repository needs nothing else — no Livy, no upload
step. Run it by hand or on a schedule.

> Use whatever resources folder name your workspace produces when it serialises
> a notebook to Git. That is a Fabric detail, not a Weaver one.

## The free habit

These routes are the same command with a different `--source`, so **you can keep
your options open at no cost.** Weaver does not care what the parent directory
is called — so if you author the repository *inside* a notebook's resources
folder in your local checkout, every route works, unchanged:

```text
my-fabric-workspace/
└── SalesEtl.Notebook/
    └── <resources>/
        └── sales-etl/          ← an ordinary SES repository
            ├── Sales__Order.py
            └── …
```

```bash
weaver build --source ./SalesEtl.Notebook/<resources>/sales-etl --host MyFabric …
```

That is the identical build to route 1. But because the folder happens to live
where Fabric's Git integration can see it, you can adopt route 3 later without
moving a file or changing a command.

**This is a convenience, not a requirement.** If you never want Fabric Git, a
plain folder anywhere is exactly as good and nothing in Weaver treats it
differently. The only argument for starting early is that starting early is free
and starting late means a move.

## See also

- [CLI usage](cli-usage.md) — hosts, capacity, and the commands
- [Agent guide](../AGENTS.md) — the object contract itself
