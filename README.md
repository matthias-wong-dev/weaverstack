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

> **Status: pre-alpha.**

## Installation

```bash
pip install weaverstack        # core, for a Fabric Environment or notebook
pip install 'weaverstack[cli]' # plus the optional desktop CLI
```

Requires Python 3.11 or later. Tested on macOS, Linux and Windows across Python
3.11 and 3.12.

No JDK and no Spark install: Fabric supplies Spark where authored runtime code
executes, and a desktop reaches it through the Session. The CLI and the whole
Fabric path run natively on all three platforms.

## CLI lifecycle

One Workspace configuration can abbreviate the full desktop lifecycle:

```bash
weaver install     --workspace-config workspace.yml
weaver build ./estate --workspace-config workspace.yml --bind Lakehouse/Sales_Dev
```

See [CLI usage](design/cli-usage.md) for repository sources, physical-first
bindings, wipe and unbind.

## Documentation

- [Design documentation map](design/README.md)
- [How Weaver build works](design/how-does-build-work.md) — incremental selection, bundle order and certification
- [Where your Weaver document repository lives](design/weaver-repository.md) — a folder of files, and how it reaches Fabric
- [CLI usage](design/cli-usage.md) — signing in, workspaces, capacity, wipe
- [Fabric integration tests](design/fabric-testing.md)
- [Agent guide](AGENTS.md)

## Licence

Apache 2.0. See [LICENSE](LICENSE).
