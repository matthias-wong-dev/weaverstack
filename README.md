# Weaverstack

Weaver is a data-engineering framework for Microsoft Fabric. It coordinates
Lakehouses, Warehouses, OneLake, Spark and T-SQL from one project, with a shared
dependency graph and persistent operational state.

## Installation

Weaver requires Python 3.11 or later and is tested on Python 3.11 and 3.13.

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

With `--non-interactive`, Weaver uses a configured service principal or Azure
CLI sign-in and does not open a browser.

## Semantic model Build

A semantic item lives under `SemanticModel/<logical-name>/`. It can contain a
PBIP project and `addon.yml`, or just `addon.yml`. `SemanticModel/addon.yml`
applies first across the project; item properties override it. Named native collections merge by
name, and ordinary lists replace the previous list.

For a new addon-only model in an existing workspace:

```bash
weaver initialise \
  --workspace Analytics \
  --project-folder ./reporting \
  --semantic-model Reporting \
  --catalogue-item Catalogue \
  --environment Weaver \
  --non-interactive
weaver build ./reporting --item SemanticModel/Reporting --non-interactive
```

`initialise` creates or reuses the typed item and writes its target configuration.
Build updates an existing item; it never creates one. A different physical name
can be selected with
`--item SemanticModel/Reporting=SemanticModel/Reporting_Dev`.

Edit `reporting/SemanticModel/Reporting/addon.yml` to define calculated content:

```yaml
tables:
  Calendar:
    .dax: CALENDAR(DATE(2026, 1, 1), DATE(2026, 12, 31))
    measures:
      Days:
        expression: COUNTROWS(Calendar)
```

The same Build is available from Python:

```python
import weaver

result = weaver.build("./reporting", items="SemanticModel/Reporting")
assert result.succeeded, result.errors
```

Build preserves source files and untouched authored partitions during assembly.
It does not fetch or reconcile the existing target definition before deployment.
It reads back
the deployed native definition, including inferred calculated columns, before
publishing certification to the shared Warehouse catalogue. An unchanged second
Build has no installation actions. Failed updates or readback checks leave the
selected model uncertified. Semantic-only deployment uses REST and catalogue
TDS; it does not use Spark.

This slice accepts native overlays and calculated `.dax` content. Unsupported
TMDL statements and addon keys produce source-located errors. `.source`, rules,
relationship shorthand, standalone DAX tests, and installed semantic
Load/Test/Health execution are not implemented in this slice.

## Documentation

- [Website](https://weaverstack.dev)
- [Documentation](https://docs.weaverstack.dev)
- [Source](https://github.com/matthias-wong-dev/weaverstack)

## Licence

Mozilla Public License 2.0. See [LICENSE](LICENSE).
