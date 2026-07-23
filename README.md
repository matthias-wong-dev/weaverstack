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

Folder, Delta and SQL are materialisation forms, not architectural tiers. You
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

## Documentation

- [Architecture summary](backlog/weaver-architecture-summary.md)
- [Implementation plan](backlog/weaverstack-step-by-step-implementation-plan.md)
- [Agent guide](AGENTS.md)

## Licence

Apache 2.0. See [LICENSE](LICENSE).
