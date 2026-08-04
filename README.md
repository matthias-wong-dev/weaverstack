# Weaverstack

A data-engineering runtime for Microsoft Fabric, built around a central control
plane.

One mandatory **Weaver Lakehouse** holds your installed source repositories and
the authoritative catalogue. Destination Lakehouses and Warehouses hold only
materialised output — no copied runtime, no per-target catalogue, no attachment
requirements.

Folder, Delta and SQL Warehouse are materialisation targets. You
describe objects in one repository; Weaver routes them to the physical targets
you name, builds one global dependency graph across all three forms, and
certifies each object in the central catalogue only once it has built.

> **Status: pre-alpha.** The current CLI, Workspace and catalogue work follows
> [the Weaver master plan](design/weaver_master_cli_plan.md).

## Installation

```bash
pip install weaverstack        # core, for a Fabric Environment or notebook
pip install 'weaverstack[cli]' # plus the optional desktop CLI
```

Requires Python 3.11 or later. Tested on macOS, Linux and Windows across Python
3.11 and 3.12.

Local Spark development additionally needs a JDK, and on Windows runs under
[WSL](https://learn.microsoft.com/windows/wsl/install) — Spark's local writes
need a `winutils.exe` that Windows does not carry. Everything else, including
the CLI and the whole Fabric path, runs natively on all three.

## Local development

Weaver runs against a local filesystem standing in for Lakehouses, so build and
load can be developed without touching a workspace. It needs a JDK and a matched
Spark/Delta pair — all optional, none of it required to use Weaver on Fabric.

```bash
weaver doctor
```

reports what is present and what to install. See
[design/local-setup.md](design/local-setup.md).

## CLI lifecycle

One Workspace configuration can abbreviate the full desktop lifecycle:

```bash
weaver install     --workspace-config workspace.yml
weaver build ./estate --workspace-config workspace.yml --bind Lakehouse/Sales_Dev
```

The same commands work against a local folder with `--workspace-type local`,
except Warehouse work, which remains Fabric-only. See
[CLI usage](design/cli-usage.md) for repository sources, physical-first bindings, wipe and
unbind.

## Documentation

- [Authoritative master plan](design/weaver_master_cli_plan.md)
- [How Weaver build works](design/how-does-build-work.md) — incremental selection, bundle order and certification
- [Where your Weaver document repository lives](design/weaver-repository.md) — a folder of files, and how it reaches Fabric
- [CLI usage](design/cli-usage.md) — signing in, workspaces, capacity, wipe
- [Local development setup](design/local-setup.md)
- [Fabric integration tests](design/fabric-testing.md)
- [Agent guide](AGENTS.md)

## Licence

Apache 2.0. See [LICENSE](LICENSE).
