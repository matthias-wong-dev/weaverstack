# Weaverstack

A data-engineering runtime for Microsoft Fabric, built around a central
catalogue.

The **Weaver catalogue** is Weaver's own operational metadata, under the `_`
schema of a Fabric Warehouse you name. It may have a Warehouse to itself or sit
alongside your own schemas in one you already have; Weaver owns `_` and nothing
else there. Destination Lakehouses and Warehouses hold only materialised
output — no copied runtime, no per-target catalogue, no attachment
requirements.

Folder, Delta and SQL Warehouse are materialisation targets. You
describe objects in one repository; Weaver routes them to the physical targets
you name, builds one global dependency graph across all three forms, and
certifies each object in the central catalogue only once it has built.

> **Status: pre-alpha.**

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

## CLI lifecycle

One Workspace configuration can abbreviate the full desktop lifecycle:

```bash
weaver fabric environment publish weaver --workspace-config workspace.yml
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
