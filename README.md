# Weaverstack

**Microsoft Fabric, made easier.**

Weaver brings your Lakehouse and Warehouse work into one repository, and you
keep writing Python and T-SQL as you already do. It reads your code to work out
what depends on what, builds the Fabric objects, and keeps track of what has
run.

Full documentation is at **[weaverstack.dev](https://weaverstack.dev)**:
[get started](https://weaverstack.dev/get-started/),
[how it works](https://weaverstack.dev/how-it-works/), and
[Weaver vs dbt](https://weaverstack.dev/vs-dbt/).

```bash
pip install weaverstack
weaver initialise
```

## What it does

You describe your estate in one repository, with objects under the Fabric item
that owns them. Weaver reads your Python imports and your SQL references,
builds one dependency graph across Lakehouses and Warehouses, and installs the
result into Fabric.

- **Build** creates the schemas, tables, views, shortcuts and load code your
  repository describes, and records all of it in the Catalogue. It changes
  structure and moves no data.
- **Load** runs the data work in dependency order, across Python, Spark SQL and
  T-SQL, tracking how far each object got.
- **Test** runs your Tests and Assumptions and records the results.
- **Health** tells you where the estate stands now: what failed, what is
  blocked, and what is behind its sources.

The **Weaver catalogue** is where it keeps all of that. It lives as ordinary
tables in the `_` schema of a Fabric Warehouse you name. Give it a Warehouse of
its own or use one you already have, since Weaver owns `_` and nothing else in
there. Your destination Lakehouses and Warehouses hold materialised output
only, with no copied runtime and no per-target catalogue.

## Installation

The same package runs in a Fabric notebook and drives Fabric from your
desktop, and the `weaver` command comes with it.

Requires Python 3.11 or later. Tested on macOS, Linux and Windows across Python
3.11 and 3.12.

You need no JDK and no local Spark. Fabric supplies Spark where your runtime
code executes, and a desktop reaches it through the Session.

## Getting started

From a desktop, with a Fabric workspace you can already reach:

```bash
weaver initialise
```

It asks which items you want, creates the ones that are missing, writes the
project, and can build, load and test a small Sales example so you can watch
the whole thing run:

```text
Fabric workspace: My Fabric Workspace
Catalogue [Catalogue]:

Fabric Environment:
  1. Use an existing Environment
  2. Create a new Environment

Choose [1/2]: 2
Environment name [Weaver]:

Lakehouse [skip]: Landing
Warehouse [skip]: Curated
Would you like to create and run a small Sales example? [Y/n]:
```

Every project runs against a Fabric Environment with Weaver installed in it.
Use one your workspace already has or create a new one. If Weaver is not in it
yet, you are asked once before anything changes:

```text
Environment 'Weaver' will be created and Weaver will be installed in it.

Installing Weaver in Fabric can take about 5 minutes.

Would you like to continue? [Y/n]:
```

The same run, written out, which is the form to keep in a script:

```bash
weaver initialise \
  --workspace "My Fabric Workspace" \
  --catalogue Catalogue \
  --environment Weaver \
  --lakehouse Landing \
  --warehouse Curated \
  --example
```

`--dry-run` shows what it would set up and changes nothing. Name an item and
Weaver uses it if it exists, or creates it if it does not, so a rerun picks up
whatever the last attempt left.

Written out like that, Weaver asks you nothing, so the Environment already
needs Weaver in it. Run `weaver initialise` on its own to be asked, or prepare
the Environment first.

From a Fabric notebook the project is the same, and the workspace is the one the
notebook is running in:

```python
%pip install weaverstack
```

```python
from pathlib import Path
import weaver

weaver.initialise(
    Path("builtin") / "repository",
    catalogue="Catalogue",
    environment="Weaver",
    lakehouse="Landing",
    warehouse="Curated",
    example=True,
)
```

Weaver uses the notebook's own workspace. Pass `install_weaver=True` if the
Environment is missing or has no Weaver in it yet. That is the same
confirmation the prompt asks for, and it takes about five minutes.

From the project directory, the ordinary commands need nothing else:

```bash
weaver build
weaver load
weaver test
```

Each reads `workspace-config.yml` beside it. `--workspace` and
`--workspace-config` still win where you give them.

## Signing in

`pip install weaverstack` is the whole prerequisite. Weaver uses your Azure CLI
sign-in if you have one, and otherwise opens Microsoft sign-in in a browser and
remembers it, so the next command opens nothing. On a machine with no keyring
it signs in each time instead of leaving the token on disk in the clear.

If something cannot connect:

```bash
weaver doctor
```

That proves your sign-in and the Fabric REST API. From a project directory it
also checks the endpoints that project uses:

```text
  Fabric REST                       OK
  Workspace My Fabric Workspace     OK
  Warehouse/Catalogue TDS           OK
  Lakehouse/Landing OneLake         OK
  Spark session                     OK
```

Checking a Lakehouse starts a Fabric Spark session, which takes a minute.
`weaver check` is the other half: it reads your repository and contacts nothing.

## CLI lifecycle

One Workspace configuration can abbreviate the full desktop lifecycle:

```bash
weaver fabric environment publish weaver --workspace-config workspace.yml
weaver build ./estate --workspace-config workspace.yml --item Lakehouse/Sales
```

Publication changes Weaver's own libraries and leaves the rest of the
Environment alone. `--path <Name>.Environment` publishes a local definition
instead, creating the Environment when it is absent, and `--dev` supplies Weaver
as a wheel built from the checkout.

A Weaver **item** is logical, `Lakehouse/Sales`. A **target** is the Fabric item
you deploy it to, `Sales_Dev`. Build, load, test and health all name items:
build takes the target from your workspace configuration, and the rest take it
from the Weaver catalogue. So one sequence runs against development and
production, and only `--workspace-config` changes.

See [CLI usage](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/cli-usage.md) for the full syntax, repository sources,
wipe and unbind.

## Documentation

Start at [weaverstack.dev](https://weaverstack.dev). The design notes below go
deeper than the site does, while the developer documentation is being written.

- [Design documentation map](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/README.md)
- [How Weaver build works](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/how-does-build-work.md): incremental selection, bundle order and certification
- [Where your Weaver document repository lives](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/weaver-repository.md): a folder of files, and how it reaches Fabric
- [CLI usage](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/cli-usage.md): signing in, workspaces, capacity, wipe
- [Fabric integration tests](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/fabric-testing.md)
- [Agent guide](https://github.com/matthias-wong-dev/weaverstack/blob/main/AGENTS.md)

## Licence

Mozilla Public License 2.0. See [LICENSE](https://github.com/matthias-wong-dev/weaverstack/blob/main/LICENSE).
