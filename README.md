# Weaverstack

A data-engineering runtime for Microsoft Fabric, built around a central
catalogue.

**[weaverstack.dev](https://weaverstack.dev)** has the guided introduction:
[get started](https://weaverstack.dev/get-started/),
[how it works](https://weaverstack.dev/how-it-works/), and
[Weaver vs dbt](https://weaverstack.dev/vs-dbt/).

The **Weaver catalogue** is Weaver's own operational metadata, under the `_`
schema of a Fabric Warehouse you name. It may have a Warehouse to itself or sit
alongside your own schemas in one you already have; Weaver owns `_` and nothing
else there. Destination Lakehouses and Warehouses hold only materialised
output, with no copied runtime, no per-target catalogue and no attachment
requirements.

Folder, Delta and SQL Warehouse are materialisation targets. You
describe objects in one repository; Weaver routes them to the physical targets
you name, builds one global dependency graph across all three forms, and
certifies each object in the central catalogue only once it has built.

## Installation

```bash
pip install weaverstack
```

One install, both positions: the same package runs in a Fabric notebook and
drives Fabric from a desktop, and the `weaver` command comes with it.

Requires Python 3.11 or later. Tested on macOS, Linux and Windows across Python
3.11 and 3.12.

No JDK and no Spark install: Fabric supplies Spark where authored runtime code
executes, and a desktop reaches it through the Session.

## Getting started

From a desktop, with a Fabric workspace you can already reach:

```bash
weaver initialise
```

It asks which items you want, creates the ones that are missing, writes the
project, and can build, load and test a small Sales example so you can see the
whole thing work:

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

Every project runs against a Fabric Environment with Weaver installed in it. You
can use one your workspace already has or create a new one; where Weaver is not
in it yet, you are asked once before anything changes:

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

`--dry-run` shows what would be set up and changes nothing. Naming an item is
the request to have it, so a missing one is created; a rerun reuses whatever is
already there.

Written out like that, nothing is asked, so the Environment has to be one that
already has Weaver in it. Run `weaver initialise` on its own to be asked, or
prepare the Environment first.

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

The workspace is the notebook's own. Pass `install_weaver=True` where the
Environment is missing or has no Weaver in it yet, which is the same consent the
prompt asks for and takes about five minutes.

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
sign-in where you have one, and otherwise opens Microsoft sign-in in a browser
and remembers it, so the next command opens nothing. On a machine with no
keyring it signs in each time instead, rather than leaving the token on disk in
the clear.

If something cannot connect:

```bash
weaver doctor
```

That proves sign-in and the Fabric REST API. From a project directory it also
checks the endpoints that project uses:

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
it is deployed to, `Sales_Dev`. Build, load, test and health name items: a build
reads the target from workspace configuration, the rest read it from the Weaver
catalogue. So one sequence runs against development and production, and only
`--workspace-config` changes.

See [CLI usage](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/cli-usage.md) for the full syntax, repository sources,
wipe and unbind.

## Documentation

- [Design documentation map](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/README.md)
- [How Weaver build works](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/how-does-build-work.md): incremental selection, bundle order and certification
- [Where your Weaver document repository lives](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/weaver-repository.md): a folder of files, and how it reaches Fabric
- [CLI usage](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/cli-usage.md): signing in, workspaces, capacity, wipe
- [Fabric integration tests](https://github.com/matthias-wong-dev/weaverstack/blob/main/design/fabric-testing.md)
- [Agent guide](https://github.com/matthias-wong-dev/weaverstack/blob/main/AGENTS.md)

## Licence

Mozilla Public License 2.0. See [LICENSE](https://github.com/matthias-wong-dev/weaverstack/blob/main/LICENSE).
