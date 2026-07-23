# Weaverstack

A data-engineering runtime for Microsoft Fabric, built around a central control
plane.

One mandatory **Weaver Lakehouse** holds your installed source repositories and
the authoritative catalogue. Destination Lakehouses and Warehouses hold only
materialised output — no copied runtime, no per-target catalogue, no attachment
requirements.

```python
import weaver

weaver.initialise_weaver_lakehouse("Weaver")
```

Folder, Delta and SQL Warehouse are materialisation targets. You
describe objects in one repository; Weaver routes them to the physical targets
you name, builds one global dependency graph across all three forms, and
certifies each object in the central catalogue only once it has built.

> **Status: pre-alpha.** Under construction against
> [a step-by-step checkpoint plan](backlog/weaverstack-step-by-step-implementation-plan.md).
> The public API above is the destination, not yet the current surface.

## Installation

```bash
pip install weaverstack        # core, for a Fabric Environment or notebook
pip install 'weaverstack[cli]' # plus the optional desktop CLI
```

Requires Python 3.11 or later.

## Local development

Weaver runs against a local filesystem standing in for Lakehouses, so build and
load can be developed without touching a workspace. It needs a JDK and a matched
Spark/Delta pair — all optional, none of it required to use Weaver on Fabric.

```bash
weaver doctor
```

reports what is present and what to install. See
[docs/local-setup.md](docs/local-setup.md).

## Documentation

- [Architecture summary](backlog/weaver-architecture-summary.md)
- [Implementation plan](backlog/weaverstack-step-by-step-implementation-plan.md)
- [Where your SES repository lives](docs/ses-repository.md) — a folder of files, and how it reaches Fabric
- [CLI usage](docs/cli-usage.md) — signing in, hosts, capacity, wipe
- [Local development setup](docs/local-setup.md)
- [Fabric integration tests](docs/fabric-testing.md)
- [Agent guide](AGENTS.md)

## Licence

Apache 2.0. See [LICENSE](LICENSE).
