# Using the Weaver CLI

## Purpose

This document explains command use and workspace configuration. It describes
the public CLI, not the implementation of command handlers.

```bash
pip install weaverstack
weaver --help
```

## Signing in

Weaver uses the Azure CLI identity where `az login` has produced one, and
otherwise opens Microsoft sign-in in a browser. The browser token is kept where
the machine keeps secrets, a Keychain item on macOS, libsecret on Linux, DPAPI
on Windows, so later commands sign in without opening anything. A machine with
none of those signs in each time and says so once; the token is never written to
disk in the clear. Only the recognised ways a platform reports having nowhere
secure are worked around. Any other failure is reported as itself.

A chain, not a probe: the Azure CLI is tried inside the token acquisition a
command was going to make anyway, so a signed-in user pays nothing to have the
fallback available. There is no `weaver auth` command: signing in is how Weaver
reaches Fabric.

The choice belongs to the CLI. The importable core accepts a credential and
installs none, and the Fabric test suite pins the Azure CLI explicitly, so an
unattended run can never be sent to a browser.

## Checking that Weaver can connect

```bash
weaver doctor
```

Proves sign-in and the Fabric REST API together: a workspace listing that comes
back means a token was issued and the control plane accepted it.

```bash
weaver doctor --workspace Analytics
weaver doctor --workspace-config workspace-config.yml
```

A workspace is resolved as well. A configuration names the items, so each
endpoint that project is reached through is opened and no others:

```text
  Fabric REST                       OK
  Workspace Analytics               OK
  Warehouse/Control TDS             OK
  Warehouse/Reporting TDS           OK
  Lakehouse/Sales OneLake           OK
  Spark session                     OK
```

A Warehouse-only project starts no Spark session. A Lakehouse-only project still
opens TDS, because the Weaver catalogue is a Warehouse. Checking a Lakehouse
starts a Fabric Spark session, which takes a minute.

A failed check prints the reason and a next action on stderr, and the command
exits non-zero. It is a connectivity check: what the estate holds is
`weaver health`, and whether a repository parses is `weaver check`.

## Setting a project up

```bash
weaver initialise
```

Creates a Weaver project and the Fabric items it needs. The workspace must
already exist; a Catalogue, Environment, Lakehouse or Warehouse that does not is
created. Naming an item is the request to have it, so nothing is asked per item.

Options are collected at a prompt when a terminal is there to answer and a
required value is missing. `--interactive` asks for the optional names too,
`--no-input` never asks and names the options a run is short of, and `--dry-run`
shows what would be set up without changing anything.

### The Fabric Environment

Every project runs against a Fabric Environment with Weaver installed in it. The
prompt offers the choice, because a name on its own does not say which is meant:

```text
Fabric Environment:
  1. Use an existing Environment
  2. Create a new Environment
```

Choosing the first lists the Environments the workspace has. Either way, where
Weaver is not installed there yet, one question is asked before anything changes:

```text
Weaver needs to be installed in the Fabric Environment 'Data Engineering'
before this project can run.

Installing Weaver in Fabric can take about 5 minutes.

Would you like to install it now? [Y/n]:
```

An existing Environment keeps everything else it declares. Only Weaver's own
libraries are added.

A run that cannot ask settles nothing implicitly. `--no-input`, and any run with
no terminal, stops before mutation and says what to do:

```text
The Fabric Environment 'Data Engineering' does not have Weaver installed.

Run `weaver initialise` interactively to install Weaver in this Environment,
or prepare the Environment before running this command again.
```

```bash
weaver initialise ./project \
  --workspace Analytics \
  --catalogue Catalogue \
  --environment Weaver \
  --lakehouse Landing \
  --warehouse Curated \
  --example
```

`--example` writes a small Sales example and runs `build`, `load` and `test`
against it, so a successful run means the chosen items have actually built,
loaded and tested.

What is written:

```text
project/
├── workspace-config.yml
├── compose.yml
├── Environment/Weaver.Environment/
├── Lakehouse/Landing/{Files,Tables}/
└── Warehouse/Curated/
```

The catalogue Warehouse gets no authored folder. Weaver owns the `_` schema
there and the first ordinary build creates its tables, so `Warehouse/Catalogue/`
would invite authoring into the item Weaver keeps its own tables in.

A new Environment is created from a definition written into the project, at
`Environment/<Name>.Environment`. One the workspace already has is not the
project's to describe, so no definition is written for it.

Provisioning is initialise's, and building is `build`'s. A build performs a
read-only preflight and creates no Fabric item, so a missing target is a build
failure naming the item.

## Workspace resolution

Commands accept the applicable subset of:

```text
--workspace <Fabric-workspace-name>
--workspace-config <path>
--environment <Environment | Workspace/Environment>
--catalogue <control-Lakehouse>
```

Explicit CLI values override the one Workspace described by the configuration
file:

```bash
weaver build ./estate \
  --workspace Analytics \
  --catalogue Warehouse/Weaver \
  --item Lakehouse/Sales=Lakehouse/Sales_Dev
```

A configuration is shorthand for the same values. Items are the keys and the
Fabric item each one is deployed to is the value:

```yaml
workspace: Analytics
environment: Runtime
catalogue: Warehouse/Control

targets:
  Lakehouse/Sales: Sales_Dev
  Warehouse/Reporting: Reporting_Dev
```

The key's type says whether the value names a Lakehouse or a Warehouse, so one
mapping serves both. Per-item execution settings hang off the key:

```yaml
targets:
  Warehouse/Reporting:
    name: Reporting_Dev
    execution:
      parallel_workers: 4
```

An unqualified Environment belongs to `Analytics` in this example. A shared
Environment is written as `environment: Platform/Runtime`. CLI overrides use
the same grammar.

See [`examples/weaver_example.yml`](../examples/weaver_example.yml) for the
expanded form, including per-item execution settings.

**`workspace:` is the one required key.** This is a complete configuration:

```yaml
workspace: Analytics
catalogue: Warehouse/Weaver
```

`targets:` may be absent or empty, and a workspace whose items deploy to Fabric
items of their own name needs none. `load` and `test` read `_.Installation`, so
an installed estate runs without one.

Every other key is validated where it is written. An unknown top-level key, an
unknown key inside `execution:` or a target declaration, and a value of the wrong
shape are each refused with a configuration error naming the field:

```text
Workspace configuration has unknown keys: targtes
targets['Lakehouse/Sales'] has unknown keys: exec
execution has unknown keys: paralell_workers
targets['Lakehouse/Sales'].name must be a non-empty string, got 7
```

**A named configuration that cannot be read is an error, not an absent
workspace.** `--workspace-config bad.yml` reports the key that is wrong. Naming
no workspace at all is a state, and a command or composition that can proceed
without one proceeds.

**A command naming no workspace reads `workspace-config.yml` beside it.** From a
project directory the ordinary commands need nothing else:

```bash
cd project
weaver build
weaver load
weaver test
```

The file is the last resort. `--workspace`, `--workspace-config` and the
workspace a `weaver session` is open on are each consulted first, so a command
already naming one reads no file it was not given.

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

weaver> wipe Lakehouse/Sales_Dev Warehouse/Reporting_Dev --yes
weaver> build . --item Lakehouse/Sales
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
weaver build ./repository --item Lakehouse/Sales
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
own configuration within that workspace, so `--catalogue`, `--environment` and
targets are the command's:

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
weaver load Warehouse/Reporting          → auth, resolver, tds
weaver load Lakehouse/Sales              → auth, resolver, tds, onelake, livy
weaver load                              → auth, resolver, tds, onelake, livy
weaver build ./repository                → auth, resolver, onelake, livy, tds
weaver health                            → auth, resolver, tds, onelake
weaver health --item Warehouse/Reporting → auth, resolver, tds
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
items the last one had. `compose.yml` writes the sequence down:

```yaml
compose:
  dev:
    - wipe Lakehouse/Sales_Dev Warehouse/Reporting_Dev
    - build ./repository --item Lakehouse/Sales --item Warehouse/Reporting
    - load Lakehouse/Sales Warehouse/Reporting
    - test Lakehouse/Sales Warehouse/Reporting
```

```bash
weaver compose dev
weaver compose dev --file path/to/compose.yml
weaver compose dev --yes                        # unattended
```

The sequence is displayed and confirmed before anything runs:

```text
Compose: dev  (compose.yml)

1. wipe Lakehouse/Sales_Dev Warehouse/Reporting_Dev
2. build ./repository --item Lakehouse/Sales --item Warehouse/Reporting
3. load Lakehouse/Sales Warehouse/Reporting
4. test Lakehouse/Sales Warehouse/Reporting

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

The qualified form names the owning workspace directly:

```bash
weaver fabric environment publish Platform/Runtime
```

Two switches, independent of each other. `--path` says where the definition
comes from, `--dev` says how Weaver itself is supplied.

```bash
weaver fabric environment publish Runtime --workspace Analytics
weaver fabric environment publish Runtime --workspace Analytics --dev
weaver fabric environment publish --path deploy/Runtime.Environment --workspace Analytics
weaver fabric environment publish --path deploy/Runtime.Environment --workspace Analytics --dev
```

Without `--path`, the Environment in the workspace is authoritative and must
already exist. Weaver changes its own libraries and nothing else: Spark compute,
every other custom library and every other entry in the external library list
are left as they are, including staged edits that have not been published.

With `--path`, a local `<Name>.Environment` directory is authoritative. The
directory names the Environment, so `Sales.Environment` publishes `Sales`
whatever workspace configuration names, and the definition is sent whole. A
missing Environment is created from it. The directory is never written to.
`Environment/` is a repository convention; the path may point anywhere.

Released publication leaves exactly one effective PyPI `weaverstack`
requirement in the external library list and removes any Weaver custom wheel. An
authored specifier is kept, so `weaverstack==0.4.0` stays pinned; a second entry
for the same distribution goes, because which one installs would otherwise be
pip's choice.

`--dev` builds a wheel from the checkout, removes the PyPI requirement, and names
Weaver's own Fabric requirements, because a Fabric custom wheel installs no
dependencies of its own. A requirement the Environment already carries keeps its
authored specifier where that specifier can satisfy Weaver's. One that cannot,
such as `sqlparse==0.5.3` against a Weaver requirement of `sqlparse>=0.6.0`, is
reported before anything is staged. It is a comparison of two specifiers, so
Weaver resolves nothing; Fabric resolves the external libraries when it
publishes.

A publication that would change nothing does not republish. A Weaver wheel
compares by filename, because the version is content addressed and the zip
around it is not reproducible; every other custom library compares by its bytes.
The three text parts compare as parsed documents, because Fabric returns them
with the line endings it stores: a checkout written on Windows sends CRLF and
reads back LF. Scalar types are part of that comparison, so `1` and `"1"` are
different definitions. `runtime_version` is the one field compared as text,
because Fabric returns `'1.3'` unquoted.

There is no separate initialise lifecycle. The package-owned catalogue is built
by the ordinary build: `Warehouse/_weaver` is composed into every parsed
repository, bound to the configured catalogue Warehouse, and its tables are created
by ordinary planned actions. A full reset is therefore a wipe followed by a
build.

The catalogue Warehouse itself must already exist. It is a Fabric workspace item,
so creating one is provisioning rather than building, and a build against a
missing catalogue Warehouse fails preflight instead of quietly making one. A
desktop build proves it, every bound Lakehouse and Warehouse, and a locally
owned Environment from one workspace listing before it starts a Livy session.
A qualified Environment is resolved in its owning workspace.

## Items and targets

An item is a Weaver identity such as `Lakehouse/Sales`. A target is the Fabric
item it deploys to, named by kind and display name, so `Lakehouse/Shared` and
`Warehouse/Shared` are two targets. Which one a command names, and where the
physical half comes from:

| Operation | Caller names | Physical source |
|---|---|---|
| build | item, `--item` | an explicit `ITEM=TARGET`, or `targets:` in workspace configuration |
| load | item, positional, or every installed item | the catalogue's `_.Installation` |
| test | item, positional, or every installed item | the catalogue's `_.Installation` |
| health | item, `--item`, or the whole estate | the catalogue's `_.Installation` |
| wipe | target, positional | the caller |
| unbind | target, positional | the caller |
| catalogue | Warehouse target | `--catalogue`, or workspace configuration |

A build establishes the installation, so it is the one operation that names both
halves. Once an item is built the catalogue is authoritative: a load, test or
health item carrying `=` is refused, and workspace configuration is not consulted
for it either.

`load` and `test` name their items positionally, and `build` with a repeated
`--item`, because a build's positional argument is the repository source and a
build item may carry an `=`. `--item` still selects a run item, so a line written
either way runs:

```bash
weaver load Warehouse/Reporting
weaver load Lakehouse/Sales Warehouse/Reporting
weaver load --item Warehouse/Reporting
weaver load Lakehouse/Sales --item Warehouse/Reporting
```

**A run naming no item covers every installed item.** `weaver load` and
`weaver test` on their own run the whole estate, and the scope comes from
`_.Installation`. An item a workspace configuration declares and no build has
installed is not in it. An estate with no installation at all says so and names
the catalogue it read.

Because the item kinds are the catalogue's answer and are read after the command
line, an unscoped run declares the superset of resources it may want: `auth`,
`resolver`, `tds`, `onelake` and `livy`.

`wipe` and `unbind` address a Fabric item whether or not an installation exists,
so they name a target and have no item to resolve.

Configuration may map two items to one target, which a constrained environment
does. Only one of them is installed there at a time: a build into a target
another item is installed to is refused, and `wipe` then `unbind` releases it.

So one repository, one set of logical names and one command sequence run against
development and production, and the only thing that changes is the workspace
configuration:

```bash
weaver build ./estate --item Lakehouse/Sales --item Warehouse/Reporting \
  --workspace-config dev.yml
weaver load Lakehouse/Sales Warehouse/Reporting \
  --workspace-config dev.yml
weaver test Lakehouse/Sales Warehouse/Reporting \
  --workspace-config dev.yml
```

## Build

A build target is `LOGICAL` or `LOGICAL=PHYSICAL`:

```bash
weaver build \
  ./estate \
  --workspace-config examples/weaver_example.yml \
  --item Lakehouse/Sales \
  --item Warehouse/Reporting=Warehouse/Reporting_Alternative
```

Without `=`, the target comes from the configuration's `targets:` mapping.
With `=`, it is supplied here and no configuration is consulted for that item.
Both sides are typed and the two types must agree. Naming no `--item` at all
builds every logical item the configuration declares.

Every build adds the implicit binding from `Warehouse/_weaver` to the configured
catalogue Warehouse. Catalogue publication is mandatory and registry certification
is last. Every build treats the repository as authoritative: a document removed
from it loses its catalogue claims and its physical object is pruned. The build
planner compares effective signatures with the reconciled Registry.
Unchanged objects receive no physical action; selected changes use an explicit
drop followed by a strict create. `Prohibit Rebuild` protects an existing
physical object while allowing its incoming catalogue metadata to advance. Its
protection comes from target inventory, so losing the object's Registry row does
not authorize replacing it.

From a desktop, parsing and request validation happen first; the build state is
then read across — the catalogue and a Lakehouse's views as Spark SQL, its
objects as storage, a Warehouse over TDS — and planning happens here against
that state. Every build action runs in the Installer, wherever that is. Weaver
running inside Fabric prepares, plans and installs in the session it is already
in.

A build needs no `--environment`. What it submits to Spark is SQL that imports
nothing, so it runs on the workspace's default runtime; `load`, `test` and the
other commands that run Weaver inside Fabric name the Environment
`weaver fabric environment publish` published to. A build of Warehouse items
only starts no Spark session at all.

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

Run the installed Tests and Assumptions the named items own:

```bash
weaver test Lakehouse/Sales --workspace-config examples/weaver_example.yml
```

The output is the verdict. A Test or an Assumption may pass, fail, or be unable
to run, and each of those is reported and recorded. None of them is a failure of
the command: a run that produced a report exits zero, so a pasted block or a
composition carries on to the next command. `--json` emits the whole report, and
`status` in it carries the verdict. A command that could not get that far, an
unusable `--name` or an estate it could not read, exits non-zero.

A whole-item run reports **counts only**. Diagnostic rows may be large and may
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

The item names the installed environment the source validation runs against.

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

## Health

`weaver health` reports the installed estate's operational state, in three
sections over one installed graph. It names items, resolving each one's target
through `_.Installation` as a load does. Naming none reports on every target the
catalogue binds an item to:

```bash
weaver health
weaver health --item Lakehouse/Sales
```


```bash
weaver health --workspace-config examples/weaver_example.yml
```

```text
Weaver Health  Amber

Load    Amber
  Last load activity   6h 14m ago
  17 succeeded · 1 pending
  Amber  Lakehouse/Sales/Tables/Sales.OrderSummary
         no load has settled since this object was built

Tests   Green
  6 succeeded

Build   Green
  Installed estate consistent (48 objects)

Slowest loads
  Lakehouse/Sales/Tables/Sales.Order       31.2s
```

The status words are the contract, so the output reads the same redirected to a
file as it does on a terminal. Overall health is the worst section. Exit status
is `0` for Green and `1` for Amber or Red, which is what makes it a scheduled
check; a configuration or transport failure takes the ordinary command error
path.

With no target, the whole installed estate. Naming targets restricts the
subjects reported on, and managed ancestry outside the selection is still read,
because whether a selected object is behind its sources is a question about the
whole graph.

`--as-of` is the instant a settled load must be no older than, an ISO-8601
instant carrying a zone. It defaults to 24 hours before the report started.
`--no-inventory` skips the physical read that proves each certified object is
there.

Health runs no authored load or test Python, so there is no `--environment` and
a Warehouse-only report starts no Spark session. A Lakehouse is read over
storage for the same reason, which is why a Lakehouse view is not among the
objects proven present: it exists in the Spark catalogue and nowhere in storage.

`--json` writes the report to stdout and nothing else, at `format_version` 1,
with `green | amber | red` as the machine vocabulary and UTC ISO-8601
timestamps. Arrays stay present when empty, so a consumer never branches on a
missing key. Publish it as a daily health artefact.

```python
report = weaver.health()
report.status  # "green" | "amber" | "red"
report.load.status
report.tests.findings  # each with a stable `code`
report.to_mapping()  # what --json prints
```

## Wipe

Wipe clears everything in each named physical target. It needs no catalogue, and
where one resolves it also removes that catalogue's claims for the wiped targets:

```bash
weaver wipe Lakehouse/Sales_Dev                       # physical only
weaver wipe Lakehouse/Sales_Dev --catalogue Warehouse/Weaver
weaver wipe Lakehouse/Sales_Dev --workspace-config dev.yml
```

The last two also remove the claims. Wiping the Warehouse the catalogue itself
lives in skips that, because deleting rows from tables that are about to be
removed is work nobody needs.

```bash
weaver wipe \
  Lakehouse/Sales_Dev \
  Warehouse/Reporting_Dev \
  --workspace-config examples/weaver_example.yml \
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
