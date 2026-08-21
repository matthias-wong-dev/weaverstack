# Using the Weaver CLI

## Purpose

This document explains command use and workspace configuration. It describes
the public CLI, not the implementation of command handlers.

```bash
pip install weaverstack
weaver --help
```

Commands use the identity from `az login`.

## Workspace resolution

Commands accept the applicable subset of:

```text
--workspace <Fabric-workspace-name>
--workspace-config <path>
--environment <Fabric-Environment>
--catalogue <control-Lakehouse>
```

Explicit CLI values override the one Workspace described by the configuration
file:

```bash
weaver build ./estate \
  --workspace Analytics \
  --catalogue Warehouse/Weaver \
  --bind Lakehouse/Sales=Sales
```

A configuration is shorthand for the same values. Physical target names are
the keys and their default logical bindings are the values:

```yaml
workspace: Analytics
environment: Runtime
catalogue: Warehouse/Control

lakehouses:
  Sales_Dev: Lakehouse/Sales
warehouses:
  Reporting_Dev: Warehouse/Reporting
```

See [`examples/weaver_example.yml`](../examples/weaver_example.yml) for the
expanded form, including per-target execution settings.

## Session

A one-shot command pays for a credential, item resolution and, for anything
touching Spark, a Livy session, and then throws all three away. Four commands
pay four times. `weaver session` pays once:

```bash
weaver session --workspace "Weaver Example" --environment weaver
```

```text
Weaver · Weaver Example
Starting: Fabric credential

Available: build, compose, load, test, wipe.
Commands are written as they are in a terminal; the leading `weaver` is optional. `help` for options, `exit` to leave.

weaver> wipe Lakehouse/Sales Warehouse/Reporting --yes
weaver> build . --bind Lakehouse/Sales=Sales
weaver> weaver load Lakehouse/Sales Warehouse/Reporting
weaver> weaver test Lakehouse/Sales
weaver> compose all
weaver> exit
```

**A session command is a Weaver command line.** It is written the way it is
written in a terminal, in `compose.yml` and in this document, parsed by the
same parser and run by the same handlers. A line copied from any of them runs
here unchanged, and what the session adds is underneath: one Session, held
open, so a credential, item resolution and Livy are paid for once rather than
per command. The leading `weaver` is optional, so `build .` and
`weaver build .` are the same command, and `weaver --help` and
`weaver --version` answer here as they do in a terminal.

**A session offers the workspace lifecycle**: `build`, `load`, `test`, `wipe`
and `compose`. `install` publishes Weaver into a Fabric Environment and
`weaver fabric` manages the estate underneath a workspace; both are run from a
shell rather than from a prompt holding one workspace open.

**Quoting holds a value together; nothing escapes.** `--workspace "Research &
Development"` is one workspace name, and a backslash is an ordinary character
wherever it appears, so `weaver build C:\Users\Matthias\repo` reaches the
command with that path. An argument containing a space is quoted rather than
escaped. Outside quoting, `|`, `>`, `<`, `&`, `;`, `$` and a backtick are
refused, because a Weaver command line is not run by a shell.

**Several complete commands can be pasted at once**, one per line:

```text
weaver build ./repository --bind Lakehouse/Sales=Sales
weaver load Lakehouse/Sales Warehouse/Reporting
weaver test Lakehouse/Sales
```

They run in order in the one session. Blank lines and lines beginning with `#`
are skipped, quoting is preserved, and a failure stops the rest of the block —
the commands after a failed build were written expecting it to have succeeded.
The prompt returns either way. A pasted block is a batch of Weaver commands and
nothing more: no pipes, no redirection, no `&&`, no variables.

**A workspace is not required to start.** `weaver session` on its own is valid;
each command then names its own workspace, and the session keeps one set of
resources per workspace it is asked about.

**A workspace given at startup is the session's**, and stays the session's.
Commands that name none inherit it, which is why the example above repeats no
`--workspace`. A command is still an ordinary Weaver command line and gives its
own configuration within that workspace, so `--catalogue`, `--environment`,
targets and bindings are the command's:

```text
weaver session --workspace "Weaver Example" --environment weaver
weaver> weaver load Lakehouse/Sales Warehouse/Reporting --catalogue Warehouse/Curated
```

That load reads `Warehouse/Curated`, in `Weaver Example`, with the Environment
the session was started with. `--catalogue` names a Warehouse inside the
workspace the session holds; it is not a way to reach another workspace.

**One session is one Fabric workspace.** Naming the workspace the session is
already open on is accepted, because it says what is already true. Naming a
different one is refused:

```text
This session is open on workspace 'Weaver Example', so 'Reporting' cannot be
reached from it. Open a session on 'Reporting' to run there.
```

Inheritance is only ever from what the session was started with. A default
picked up from whichever command ran last would mean the next command silently
borrowing another workspace's Environment.

**The prompt does not wait for Fabric.** Where a workspace is known at startup,
the credential is acquired in the background, and each command starts what it
declared before it runs. The first command that needs Spark waits on that
startup rather than beginning a second one, which matters on a capacity that
permits exactly one.

**Spark starts with the first command that needs it.** Opening a session warms
the credential every command needs, and nothing else: a Livy session costs a
minute and a capacity's only slot, and Fabric attaches one to a Lakehouse, which
before a command is typed there is none of. So a Warehouse-only sequence starts
no Spark session at all, the first command naming a Lakehouse starts one against
one of the Lakehouses it was asked for, and later commands in the same workspace
share it. The Lakehouse is where the session lives rather than where work lands:
every generated statement names its own target in full, so a workspace
configuring no `lakehouses` builds into one perfectly well.

**An ordinary failure keeps the session.** A build that fails, a Spark error, a
typo: the command reports and the prompt returns with the resources still up.
`Ctrl-C` abandons what is being typed, or interrupts the command that is
running, and leaves the session and its resources where they were.

The prompt has editing, history and arrow keys, and each new prompt begins on a
line of its own — the shell takes down the live progress line before drawing
it, so output and prompt never share a line.

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

A command that declares `livy` also names the physical Lakehouses it is for,
read from the same arguments, because Fabric creates a Livy session against a
Lakehouse and puts its id in the Livy URL. The first of them is where the
session lives. It is not where work lands: every generated statement names its
own target in full, so which one is picked does not matter, and a workspace
configuring no `lakehouses` needs none added to build into one. A command that
names no Lakehouse declares no `livy` and starts no session.

## Wiping a whole estate

Name the catalogue Warehouse alongside the destinations:

```bash
weaver wipe Lakehouse/Sales Warehouse/Reporting Warehouse/Weaver --yes
```

`wipe` removes the physical contents of what it is given, then deletes the
catalogue claims of anything it emptied — unless the catalogue Warehouse is among
them, in which case it skips that entirely, because the catalogue tables are
going with it and deleting rows from a table about to be removed is work nobody
needs.

That is worth knowing, because the catalogue tidy is not cheap: it deletes a row
per claim, and for a from-scratch loop those are rows the next build rewrites
immediately.

So for a from-scratch loop, wipe the catalogue Warehouse too. Keep it out only
when you mean to preserve the catalogue — decommissioning one target out of an
estate that carries on.

## Compose

The development loop is the same four commands every time, each carrying the
bindings and targets the last one had. `compose.yml` writes the sequence down:

```yaml
compose:
  dev:
    - wipe Lakehouse/Sales Warehouse/Reporting
    - build ./repository --bind Lakehouse/Sales=Sales
    - load Warehouse/Reporting
    - test Warehouse/Reporting
```

```bash
weaver compose dev
weaver compose dev --file path/to/compose.yml
weaver compose dev --yes                        # unattended
```

The sequence is displayed and confirmed before anything runs:

```text
Compose: dev  (compose.yml)

1. wipe Lakehouse/Sales Warehouse/Reporting
2. build ./repository --bind Lakehouse/Sales=Sales
3. load Warehouse/Reporting
4. test Warehouse/Reporting

Execute this sequence? [y/N]
```

The default is no, and only `y`/`yes` proceeds. **That one answer authorises the
whole sequence** — a `wipe` inside it does not stop to ask again, because having
agreed to four commands, being asked about the first of them is not a second
safeguard. Without a terminal to ask, nothing runs unless `--yes` said so
already; `--yes` carries the same authority to each command in the sequence.

**Entries are ordinary Weaver command lines**, read by the same function the
session prompt reads a typed line with, parsed by the same parser and run by
the same handlers, so an option means here what it means at a prompt. The
leading `weaver` is optional, as it is at a prompt. Nothing shell-shaped is
accepted — no pipes, no redirection, no `&&`, no variables, no other
executables — and neither is `session` or a nested `compose`.

**One Session runs the whole sequence**, which is the point: authentication,
item resolution and Livy are paid for once rather than four times. Typed at a
`weaver session` prompt as `weaver compose dev`, it joins the Session already
open rather than acquiring a second set of resources.

It is not a workflow engine, and is not meant to become one: no conditionals,
no parallelism, no variables, no retries, no project-root discovery. Commands
run in order and stop at the first failure.

## Fabric Environment publication and catalogue bootstrap

Install Weaver into a Fabric Environment:

```bash
weaver fabric environment publish Runtime --workspace Analytics
```

There is no separate initialise lifecycle. The package-owned catalogue is built
by the ordinary build: `Warehouse/_weaver` is composed into every parsed
repository, bound to the configured catalogue Warehouse, and its tables are created
by ordinary planned actions. A full reset is therefore a wipe followed by a
build.

The catalogue Warehouse itself must already exist. It is a Fabric workspace item,
so creating one is provisioning rather than building, and a build against a
missing catalogue Warehouse fails preflight instead of quietly making one. A
desktop build proves it — along with every bound Lakehouse and Warehouse, and
the Environment where one is named — from a single workspace listing before it
starts a Livy session.

## Build

Bindings are physical-first:

```bash
weaver build \
  ./estate \
  --workspace-config examples/weaver_example.yml \
  --bind Lakehouse/Sales_Dev \
  --bind Warehouse/Reporting_Dev=Alternative
```

Without `=`, the physical target uses its configured logical default. With `=`,
the right side is an invocation-only logical override. Lakehouse and Warehouse
types must match.

Every build adds the implicit binding from `Warehouse/_weaver` to the configured
catalogue Warehouse. Catalogue publication is mandatory and registry certification
is last. Every build treats the repository as authoritative: a document removed
from it loses its catalogue claims and its physical object is pruned. The build
planner compares effective signatures with the reconciled Registry.
Unchanged objects receive no physical action; selected changes use an explicit
drop followed by a strict create. `Prohibit Rebuild` protects an existing
physical object while allowing its incoming catalogue metadata to advance.

From a desktop, parsing and request validation happen first; the build state is
then read across — the catalogue and a Lakehouse's views as Spark SQL, its
objects as storage, a Warehouse over TDS — and planning happens here against
that state. Every build action runs in the Installer, wherever that is. Weaver
running inside Fabric prepares, plans and installs in the session it is already
in.

A build needs no `--environment`. What it submits to Spark is SQL that imports
nothing, so it runs on the workspace's default runtime; `load`, `test` and the
other commands that run Weaver inside Fabric name the Environment
`weaver fabric environment publish` published to. A build binding only Warehouses starts no Spark
session at all.

An ordinary build does not retain an artifact. For controlled handoff, use the
advanced split workflow:

```bash
weaver build ./estate --bundle-only --bundle-path ./dist/estate-bundle
weaver install ./dist/estate-bundle --workspace-config examples/weaver_example.yml
```

`weaver check [repository]` is also available for agents, CI and editor tooling
that need to validate source without contacting Fabric. It is not a prerequisite
for `weaver build`, which always checks source itself.

## Test

Run the installed Tests and Assumptions in one or more physical targets:

```bash
weaver test Lakehouse/Sales --workspace-config examples/weaver_example.yml
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

`--file` supports SQL validation. Python validation is directly runnable in a
notebook.

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

## Wipe

Wipe clears everything in each selected typed target. Physical wipe does not
require catalogue access; immediate catalogue cleanup is selected separately
with `--unbind-from` (or the configured catalogue).

```bash
weaver wipe \
  Lakehouse/Sales_Dev \
  Warehouse/Reporting_Dev \
  --workspace-config examples/weaver_example.yml \
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

## Fabric estate

`weaver fabric` manages the estate Weaver runs on rather than anything Weaver
built. Nothing under it reads or writes the catalogue.

```bash
weaver fabric capacity resume  --resource-group <rg> --capacity-name <capacity>
weaver fabric capacity status  --resource-group <rg> --capacity-name <capacity>
weaver fabric capacity suspend --resource-group <rg> --capacity-name <capacity>

weaver fabric notebook push ./notebooks/Refresh.py --workspace "Analytics"
weaver fabric notebook run Refresh --workspace "Analytics" --lakehouse "Sales"
```
