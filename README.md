# Weaverstack

Weaver is a data-engineering framework for Microsoft Fabric. It coordinates
Lakehouses, Warehouses, OneLake, Spark and T-SQL from one project, with a shared
dependency graph and persistent operational state.

## Installation

Weaver requires Python 3.11 or later.

```bash
python -m pip install weaverstack
```

## Minimal example

Create a project containing the Sales example, then build it into a Fabric
workspace:

```bash
weaver initialise \
  --workspace Analytics \
  --project-folder ./analytics \
  --lakehouse Landing \
  --example \
  --non-interactive
cd analytics
weaver build
```

The CLI uses an existing Azure CLI sign-in or opens Microsoft sign-in in a
browser.

## Documentation

- [Website](https://weaverstack.dev)
- [Documentation](https://docs.weaverstack.dev)
- [Source](https://github.com/matthias-wong-dev/weaverstack)

## Licence

Mozilla Public License 2.0. See [LICENSE](LICENSE).
