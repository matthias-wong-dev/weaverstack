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
`data_sources` in workspace config supplies defaults, naming logical items that
resolve through `targets`; repeatable `--data-source` options override them. SQL sources use the resolved SQL endpoint. Native
`Lakehouse.Contents` expressions use workspace and Lakehouse IDs. Ordinary
expressions with no mapping remain unchanged.

Observable shared-expression navigation publishes exact managed Table/View
and consuming-table dependencies for installed Load ordering. Unknown M
navigation remains unknown. Source mapping preserves authored columns,
descriptions, partitions and storage modes.

### Without a catalogue

A workspace with no catalogue still builds and refreshes semantic models:

```bash
weaver build ./reporting --item SemanticModel/Reporting --workspace Analytics
weaver load SemanticModel/Reporting --workspace Analytics
```

Build deploys each selected model and verifies its readback. With no catalogue
there is no record of what is installed, so every Build redeploys, and Load
records nothing. Lakehouse and Warehouse items, `Weaver.Source`, lineage and
`load --stale`, `--name` or `--reload` need a catalogue.

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

Source mapping changes the definition, naming each source by its Warehouse or
Lakehouse name. Before refreshing, Load binds every unbound SQL data source to
the one cloud or gateway connection whose path is that source's
`server;database`; `weaver build --bind-data-sources` does the same after
deploying. A source no connection reaches keeps the connection Fabric gave it,
such as single sign-on. A source several connections reach fails, naming them.
Weaver creates no connections or credentials.

`extension.tmdl` merges by native structure. An object is identified by its
kind and name within its parent, and a property by its name. A new object is
added whole, an existing one merges recursively, and a supplied property,
expression or description replaces the base value. This holds for any TMDL
object or property, including ones Weaver does not interpret. Fabric acceptance
and TMSL readback certify such content; known requested changes receive value
checks.

The same edits are available from Python, using native TMDL names:

```python
from weaver.semantic_models import TmdlDefinition

definition = TmdlDefinition(parts)
for table in definition.model.tables:
    for column in table.columns:
        if column.dataType == "int64":
            column.isHidden = True
```

Each edit changes only the TMDL lines it addresses.

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
