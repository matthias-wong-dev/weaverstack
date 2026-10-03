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
works without an addon. Build deploys its TMDL definition parts and preserves
untouched source bytes, including constructs Weaver does not edit.

`addon.yml` applies surgical edits to a staged TMDL package. An addon-only model
starts from an empty TMDL package and uses the same compiler.
`SemanticModel/addon.yml` applies first; the item addon overrides it. Named native
collections merge by name, and ordinary lists replace the previous list.

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

For addon-authored tables, `.source: Warehouse/Serving/Cake.Sales` owns the table's
source fragment. Observable managed sources publish table-level dependencies
for installed Load ordering.

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

Live qualification uses a service principal. Delegated-user Azure CLI/browser
access remains a separate user check. Rules, relationship shorthand, semantic
DAX Test/TestStatus/Health and broader addon editing remain follow-up work.
Unsupported requested addon keys fail with source diagnostics; untouched native
TMDL passes through to Fabric.

## Documentation

- [Website](https://weaverstack.dev)
- [Documentation](https://docs.weaverstack.dev)
- [Source](https://github.com/matthias-wong-dev/weaverstack)

## Licence

Mozilla Public License 2.0. See [LICENSE](LICENSE).
