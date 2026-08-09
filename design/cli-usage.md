# Using the Weaver CLI

```bash
pip install 'weaverstack[cli]'
weaver --help
```

Fabric commands use the identity from `az login`. Local commands need no Azure
credentials.

## Workspace resolution

Commands accept the applicable subset of:

```text
--workspace <Fabric-name-or-local-folder>
--workspace-type <fabric|local>
--workspace-config <path>
--environment <Fabric-Environment>
--weaver-lakehouse <control-Lakehouse>
```

`workspace_type` defaults to `fabric`. Explicit CLI values override the one
Workspace described by the configuration file. A local Workspace is simply a
folder path:

```bash
weaver build ./estate \
  --workspace .local \
  --workspace-type local \
  --weaver-lakehouse Weaver \
  --bind Lakehouse/Sales=Lakehouse/Sales
```

A configuration is shorthand for the same values. Physical target names are
the keys and their default logical bindings are the values:

```yaml
workspace: Analytics
workspace_type: fabric
environment: Runtime
weaver_lakehouse: Control

lakehouses:
  Sales_Dev: Lakehouse/Sales
warehouses:
  Reporting_Dev: Warehouse/Reporting
```

See [`examples/env.yml`](../examples/env.yml) for the expanded form.

## Session

A one-shot command pays for a credential, item resolution and — for anything
touching Spark — a Livy session, and then throws all three away. Four commands
pay four times. `weaver session` pays once:

```bash
weaver session --workspace "Weaver Example" --environment weaver
```

```text
Weaver · Weaver Example
Starting resources in the background...
Commands are the ordinary CLI commands. `exit` to leave.

weaver> wipe Lakehouse/Sales Warehouse/Reporting --yes
weaver> build . --bind Lakehouse/Sales=Lakehouse/Sales
weaver> load --targets Lakehouse/Sales Warehouse/Reporting
weaver> test Lakehouse/Sales
weaver> exit
```

The commands are the ordinary CLI commands, parsed by the same parser — there
is no second grammar to learn or to keep correct.

**A workspace is not required to start.** `weaver session` on its own is valid;
each command then names its own workspace, and the session keeps one set of
resources per workspace it is asked about.

**A workspace given at startup is inherited** by commands that name none, which
is why the example above repeats no `--workspace`. Flags a command *does* give
are applied on top, so `build --weaver-lakehouse Other` overrides the control
Lakehouse without restating the workspace. Naming a different `--workspace`
addresses that one instead, with its own resources.

Inheritance is only ever from what the session was started with. A default
picked up from whichever command ran last would mean the next command silently
borrowing another workspace's Environment.

**The prompt does not wait for Fabric.** Where a workspace is known at startup,
the credential and the Livy session are acquired in the background; the first
command that needs Spark waits on that startup rather than beginning a second
one, which matters on a capacity that permits exactly one. A local workspace
warms its JVM the same way.

**An ordinary failure keeps the session.** A build that fails, a Spark error, a
typo: the command reports and the prompt returns with the resources still up.

## Install and control-plane bootstrap

Install Weaver into a Fabric Environment:

```bash
weaver install --workspace Analytics --environment Runtime
```

There is no separate initialise lifecycle. The package-owned catalogue is built
by the ordinary build: `Lakehouse/_weaver` is composed into every parsed
repository, bound to the configured Weaver Lakehouse, and its tables are created
by ordinary planned actions. A full reset is therefore a wipe followed by a
build.

The Weaver Lakehouse itself must already exist. It is a Fabric workspace item,
so creating one is provisioning rather than building, and a build against a
missing Weaver Lakehouse fails preflight instead of quietly making one. A
desktop build proves it — along with the Environment and every bound Lakehouse
and Warehouse — from a single workspace listing before it starts a Livy session.

## Push (compatibility utility)

Push validates the complete authored repository before replacing
`Files/weaver_items/`:

```bash
weaver push ./estate --workspace-config examples/env.yml
```

Push is whole-repository only. It does not build targets or mutate catalogue
rows, and the local source folder name is not added as another remote level.
`Lakehouse/_weaver` is package-owned and is composed in memory; it must not be
authored or uploaded. Build does not consume this destination.

## Build

Bindings are physical-first:

```bash
weaver build \
  ./estate \
  --workspace-config examples/env.yml \
  --bind Lakehouse/Sales_Dev \
  --bind Warehouse/Reporting_Dev=Warehouse/Alternative
```

Without `=`, the physical target uses its configured logical default. With `=`,
the right side is an invocation-only logical override. Lakehouse and Warehouse
types must match.

Every build adds the implicit binding from `Lakehouse/_weaver` to the configured
Weaver Lakehouse. Catalogue publication is mandatory and registry certification
is last. Every build treats the repository as authoritative: a document removed
from it loses its catalogue claims and its physical object is pruned. The build
planner compares effective signatures with the reconciled Registry.
Unchanged objects receive no physical action; selected changes use an explicit
drop followed by a strict create. `Prohibit Rebuild` protects an existing
physical object while allowing its incoming catalogue metadata to advance.

For a local CLI targeting Fabric, parsing and request validation happen first;
one Environment-backed Livy session then returns authoritative build state,
planning happens locally, and a completed archive is uploaded under
`Files/cli/<execution-id>/` for one in-session install call. Native Fabric builds
still prepare, plan, and install in-session. Local targets run in-process against
the emulator. Warehouses remain Fabric-only.

Add `--bundle` to retain a timestamped `.weaver.zip` build record, or
`--bundle <name>` to choose its name.

## Test

Run the installed Tests and Assumptions in one or more physical targets:

```bash
weaver test Lakehouse/Sales --workspace-config examples/env.yml
```

The exit code is the verdict — non-zero when anything failed or could not be
evaluated — and the output is the evidence, which is what makes the command
usable in a pipeline. `--json` emits the whole report.

A whole-target run reports **counts only**. Diagnostic rows may be large and may
carry sensitive business data, so they are never transferred and never logged;
Warehouse procedures are called with `@suppress_result_set = 1` and Spark
validations are counted without collecting.

Name one to see the rows:

```bash
weaver test Lakehouse/Sales --name Sales.OrderSummaryReconciliation
```

The counts and the rows come from **one** execution. A Test run twice would
compare data that could have changed in between, and could be expensive twice
over.

Run a validation that has not been built:

```bash
weaver test Lakehouse/Sales --file tests/Sales.OrderSummaryReconciliation.sql
```

`--file` compiles the source with the same compiler a build uses and executes
the result without installing it — no Registry row, no `TestDictionary` row, no
task log. From a desktop the file's *content* crosses into the session, not its
path. `--name` and `--file` are mutually exclusive.

Python validation is not run through `--file`, deliberately: it is already
directly runnable in a notebook.

```python
from tests.Sales__OrderCustomerExists import Sales__OrderCustomerExists

Sales__OrderCustomerExists(spark).read()
```

Everything is available from Python as well, with the same meaning:

```python
weaver.test("Lakehouse/Sales")
weaver.test("Lakehouse/Sales", name="Sales.OrderSummaryReconciliation")
weaver.test("Lakehouse/Sales", file="tests/Sales.OrderSummaryReconciliation.sql")
```

There is deliberately no `weaver assumption` command. One operation runs both
kinds, because a caller asking whether an estate holds up is not asking two
questions. See [validation](validation.md).

## Unbind

Unbind removes catalogue state for explicitly named physical targets without
inspecting or deleting those targets:

```bash
weaver unbind \
  --workspace-config examples/env.yml \
  Lakehouse/Sales_Dev \
  Warehouse/Reporting_Dev
```

It works even when the physical target has already disappeared. Unrelated
installations remain.

## Wipe

Wipe is intentionally broader: it clears everything in each selected typed
target. Physical wipe does not require catalogue access; immediate catalogue
cleanup is selected separately with `--unbind-from` (or the configured control
Lakehouse).

```bash
weaver wipe \
  Lakehouse/Sales_Dev \
  Warehouse/Reporting_Dev \
  --workspace-config examples/env.yml \
  --unbind-from Control \
  --dry-run
```

Lakehouse wipe clears its Files and Tables areas. Warehouse wipe removes all
user-created object types covered by Weaver's Warehouse wipe implementation,
not only objects previously registered by Weaver. Use `--yes` for unattended
execution; otherwise a non-interactive process refuses the destructive action.

A Lakehouse's **shortcuts go first**, and they are reported as
`shortcut:<path>/<name>` so a dry run distinguishes a pointer being taken away
from a directory being deleted. Only the pointer goes: the data belongs to the
item that produced it, and wiping one Lakehouse never reaches through a shortcut
into another.

## Capacity and diagnostics

```bash
weaver capacity resume  --resource-group <rg> --capacity-name <capacity>
weaver capacity status  --resource-group <rg> --capacity-name <capacity>
weaver capacity suspend --resource-group <rg> --capacity-name <capacity>

weaver doctor
```

`doctor` reports whether local Spark, Delta and a supported JDK are available.
