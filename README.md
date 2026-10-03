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

A semantic item lives under `SemanticModel/<logical-name>/`. A normal PBIP
works without an extension. Build deploys its TMDL definition parts and preserves
untouched source bytes, including constructs Weaver does not edit.

`extension.tmdl` contains optional partial native TMDL declarations.
`SemanticModel/extension.tmdl` applies first, followed by
`SemanticModel/<logical-name>/extension.tmdl`. More local values win. Object
identity is its parent path, type and name. Supplied properties replace their
previous values; omitted properties and unrelated source bytes remain unchanged.
Named children merge recursively. A supplied expression replaces its full body.
A `ref` declaration requires an existing object and reports its source line if
that object is missing.

An extension-only model starts from a minimal TMDL package and uses the same
compiler. Its source folder needs only `extension.tmdl`.

For a new extension-only model in an existing workspace:

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

Edit `reporting/SemanticModel/Reporting/extension.tmdl` to define calculated content:

```tmdl
table Calendar
    partition Calendar = calculated
        source = CALENDAR(DATE(2026, 1, 1), DATE(2026, 12, 31))

    measure Days = COUNTROWS(Calendar)
```

The same Build is available from Python:

```python
import weaver

result = weaver.build("./reporting", items="SemanticModel/Reporting")
assert result.succeeded, result.errors
```

Build sends the effective TMDL package to Fabric. After deployment it reads
TMSL back from Fabric, verifies requested edits and publishes the observed model
to the five semantic catalogue tables. Failed updates or readback checks leave
the selected model uncertified. An unchanged effective package plans zero actions.

### Shared data sources

A PBIP can use one shared M expression per logical data source. Table queries
navigate from that expression. Build changes the shared expression's physical
source while preserving the table queries:

```bash
weaver build ./reporting --item SemanticModel/Reporting \
  --data-source DataSource1=Warehouse/Serving_Dev
```

Expressions named `Warehouse/Serving` or `Lakehouse/Curated` resolve through the
workspace's logical targets automatically. Generic names use explicit mappings.
`data_sources` in workspace config supplies defaults; repeatable `--data-source`
options override them. SQL sources use the resolved SQL endpoint. Native
`Lakehouse.Contents` expressions use workspace and Lakehouse IDs. Ordinary
expressions with no mapping remain unchanged.

Observable shared-expression navigation publishes exact managed Table/View
and consuming-table dependencies for installed Load ordering. Unknown M
navigation remains unknown. Source mapping preserves authored columns,
descriptions, partitions and storage modes.

### Load and connections

```bash
weaver load SemanticModel/Reporting --workspace Analytics --catalogue Warehouse/Catalogue
```

Load uses the installed catalogue, so it needs no source checkout. It refreshes
the model and records request ID, outcome and timing in LoadStatus and Log.
Changed Build permits required clearing of processed semantic data and leaves
LoadStatus Pending. It does not delete Warehouse or Lakehouse source data.
Unchanged Build preserves LoadStatus. Semantic-only Build and Load use REST/TDS
and start no Spark session.

Source mapping changes the definition. Build and Load leave runtime connection
bindings and credentials to Fabric settings. Refresh errors retain the service
error and identify connection-owner action where applicable. A standalone
`semantic-model bind` command is planned for existing approved connections;
it is not implemented here. Shared connections and gateways remain outside
semantic-item ownership.

Native PBIP deployment supports more TMDL than extension editing. Complete new,
non-colliding native objects can be added without a Python schema for their type.
An extension targeting an existing object type that Weaver cannot safely patch
fails with a source-located diagnostic. Fabric acceptance and TMSL readback
certify opaque new objects; known requested changes receive value checks.

The former `addon.yml`, `.dax` and `.source` authoring syntax is removed.
`Weaver.*` annotation transformations, semantic wipe and DAX Test/TestStatus/Health
are separate follow-ups. Native TMDL expresses relationships and calculated tables.
Delegated-user Azure CLI/browser access remains a separate user validation.

## Documentation

- [Website](https://weaverstack.dev)
- [Documentation](https://docs.weaverstack.dev)
- [Source](https://github.com/matthias-wong-dev/weaverstack)

## Licence

Mozilla Public License 2.0. See [LICENSE](LICENSE).
