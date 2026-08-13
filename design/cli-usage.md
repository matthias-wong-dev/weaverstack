# Using the Weaver CLI

## Purpose

This document explains command use and workspace configuration. It describes
the public CLI, not the implementation of command handlers.

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
weaver> load Lakehouse/Sales Warehouse/Reporting
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

## Progress and timings

Every command is a **Task**, made of **Steps**, and where it is useful, of
**Sub-steps** — one per physical unit. Each frame reports what it cost as it
closes, on stderr, so stdout stays the command's answer:

```text
Build

  Read physical state                                  8.4s
  Build bundle                                         0.3s
    Sales.Customer                                     3.2s
    Sales.Order                                        4.1s
  Install                                             18.6s
✓ Build                                               40.7s
```

Children appear above their parent with the parent's own total underneath — a
roll-up, the way `du` reads. An error is content attached to whichever frame
failed, not a level of its own, and a failure closes every frame it unwound so
a stopped run still reports what it spent.

**While work is in flight**, a line below the completed ones names the innermost
open frame and how long it has been running, rewritten in place:

```text
⋯ Unbind catalogue claims                              1m47s
```

It is erased before anything permanent is written, so it never lands in the
transcript, and it needs a terminal to rewrite — piped, redirected or captured,
the output is exactly the completed lines. The elapsed figure ticks, which is
the half that says a two-minute wait is alive rather than hung.

That is the *logical* ledger. The transport one is separate, and neither can be
derived from the other — "the load took forty seconds" and "thirty-eight of them
were one Livy startup" call for opposite changes:

```bash
weaver session --timings
weaver compose dev --timings
```

```text
session lifetime 61.2s
  livy.start                  1 calls     40.9s
  livy.load                   1 calls     14.1s
  resolve.item                6 calls      1.2s
  resolve.item.cache_hits     4
```

## What a command needs

This section describes command behaviour. The Session and resource-boundary
rationale is defined in [Code architecture](code-architecture.md).

Each command declares its coarse resource requirements from its parsed
arguments — `auth`, `resolver`, `onelake`, `tds`, `livy` — and the Session
starts exactly those, in the background, before the command wants them:

```text
weaver load Warehouse/Reporting   → auth, resolver, tds
weaver load Lakehouse/Sales       → auth, resolver, onelake, livy
weaver build ./repository         → auth, resolver, onelake, livy, tds
```

A Warehouse load therefore never waits on a Spark session, which on a capacity
permitting one concurrent session is the difference between running and
queueing.

Commands declare resources; the Session prepares them without taking ownership
of build or run planning.

Declarations are coarse and are a **superset** — arguments cannot know what a
repository or a catalogue turns out to contain. Exact routing comes later, from
the BuildBundle or the RunGraph. So **preparing is not using**: a declaration
gives a head start to an acquisition that is coming anyway and never causes one.
A run that declares `livy` and turns out to be all T-SQL opens no Spark session
and no remote runtime scope.

`weaver compose` takes the union of every parsed command's requirements and
warms that once, so a sequence ending in a load does not wait for Spark at the
end of the build in front of it.

## Wiping a whole estate

Name the Weaver Lakehouse alongside the destinations:

```bash
weaver wipe Lakehouse/Sales Warehouse/Reporting Lakehouse/Weaver --yes
```

`wipe` removes the physical contents of what it is given, then deletes the
catalogue claims of anything it emptied — unless the control Lakehouse is among
them, in which case it skips that entirely, because the catalogue tables are
going with it and deleting rows from a table about to be removed is work nobody
needs.

That is worth knowing, because the catalogue tidy is not cheap. Measured against
a real workspace, a wipe of two destination targets spent about **two minutes**,
almost all of it deleting rows the next build would immediately rewrite. Naming
the control Lakehouse as well brought the same wipe to **4.4 seconds**.

So for a from-scratch loop, wipe the control Lakehouse too. Keep it out only
when you mean to preserve the catalogue — decommissioning one target out of an
estate that carries on.

## Compose

The development loop is the same four commands every time, each carrying the
bindings and targets the last one had. `compose.yml` writes the sequence down:

```yaml
compose:
  dev:
    - weaver wipe Lakehouse/Sales Warehouse/Reporting
    - weaver build ./repository --bind Lakehouse/Sales=Lakehouse/Sales
    - weaver load Warehouse/Reporting
    - weaver test Warehouse/Reporting
```

```bash
weaver compose dev
weaver compose dev --file path/to/compose.yml
```

The sequence is displayed and confirmed before anything runs:

```text
Compose: dev  (compose.yml)

1. weaver wipe Lakehouse/Sales Warehouse/Reporting
2. weaver build ./repository --bind Lakehouse/Sales=Lakehouse/Sales
3. weaver load Warehouse/Reporting
4. weaver test Warehouse/Reporting

Execute this sequence? [y/N]
```

The default is no, and only `y`/`yes` proceeds. **That one answer authorises the
whole sequence** — a `wipe` inside it does not stop to ask again, because having
agreed to four commands, being asked about the first of them is not a second
safeguard. Without a terminal to ask, nothing runs.

**Entries are ordinary Weaver command lines**, parsed by the same parser and run
by the same handlers, so an option means here what it means at a prompt. Nothing
shell-shaped is accepted — no pipes, no redirection, no `&&`, no variables, no
other executables — and neither is `session`, `doctor` or a nested `compose`.

**One Session runs the whole sequence**, which is the point: authentication,
item resolution and Livy are paid for once rather than four times. Run inside
`weaver session`, the composition joins the Session already open.

It is not a workflow engine, and is not meant to become one: no conditionals,
no parallelism, no variables, no retries, no project-root discovery. Commands
run in order and stop at the first failure.

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

`weaver test` runs both Tests and Assumptions. See [validation](validation.md).

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

Wipe clears everything in each selected typed target. Physical wipe does not
require catalogue access; immediate catalogue cleanup is selected separately
with `--unbind-from` (or the configured control Lakehouse).

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
