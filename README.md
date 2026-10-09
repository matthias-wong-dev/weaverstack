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

A Power BI source project lives under `PowerBI/<project>/`. Each native
`<model-name>.SemanticModel` directory or standalone `<model-name>.tmdl` declares
one logical model. A native directory and TMDL file with the same name form one
model; different names form separate models. A normal PBIP needs no Weaver
declaration. Build deploys its TMDL definition parts and preserves untouched
source bytes, including constructs Weaver does not edit.

`PowerBI/policy.tmdl` contains organisation-wide partial native TMDL declarations.
It applies first, followed by `PowerBI/<project>/<model-name>.tmdl`.
More local values win. Object identity is its parent path, type and name.
Supplied properties replace their previous values; omitted properties and unrelated source bytes remain unchanged.
Named children merge recursively. A supplied expression replaces its full body.
A `ref` declaration requires an existing object and reports its source line if
that object is missing.

An extension-only model starts from a minimal TMDL package and uses the same
compiler. Its source folder needs only `<model-name>.tmdl`.

A Report under the project, including a nested `<report-name>.Report` directory,
links to the first of:

1. the same-name logical model, including a variant composed with
   `Weaver.BaseSemanticModels`;
2. the model its `definition.pbir` `byPath` reference names in the project: a
   `<model-name>.SemanticModel` directory, or `<model-name>.SemanticModel` beside
   a model declared only by `<model-name>.tmdl`.

A Report with a `byConnection` reference and no same-name model deploys as
authored: it stays bound to the same model in every environment and gains no
model dependency. A `byPath` reference that names no model in the project is
refused. For a linked Report, Build rebinds only the deployment payload to the
resolved service model. Authored files and native resources remain unchanged.
Reverse-mapping thin Report connections is not supported.

Select a model and its Reports together:

```bash
weaver build ./reporting \
  --item SemanticModel/Reporting=SemanticModel/Reporting_Dev \
  --item Report/Executive=Report/Executive_Dev
```

Build verifies the deployed model before updating its selected Reports, then
verifies each Report's definition and model binding before catalogue publication.
A model deployment rebuilds its selected consuming Reports. A Report-only edit
rebuilds that Report; JSON parts compare by value, so whitespace and line endings
alone are not edits. Unchanged catalogue-backed Report-only builds perform no work;
every selected model deploys even with an unchanged signature.
Catalogue-free model and Report builds deploy and verify on every invocation.
A linked Report-only selection needs its model's certified
catalogue installation. An as-authored Report is independently certified.

Targets must already exist. Python `weaver.initialise(..., reports={name: definition})`
creates or reuses typed Reports from complete service-bound native definitions.
Report updates use `updateMetadata=false`. Report wipe and destructive mutations
are unsupported. Reports are excluded from Load, Test and refresh execution.

Build selectors expand before target binding: `SemanticModel`, `Warehouse`,
`Lakehouse` and `Report` select their real logical items; `PowerBI` selects all
Power BI projects, and `PowerBI/<project>` selects one. Exact selections retain
their physical target overrides. Build source items first, then Power BI items.

To adopt an existing `PowerBI` source tree, run ordinary `initialise` against its
project folder without `--semantic-model`. It creates or reuses every discovered
logical model and Report at its configured typed target, preserves authored
files, and writes default target entries when there is no workspace config.
An explicit `--semantic-model Name` still provisions just that named model.

Rename organisation `extension.tmdl` to `PowerBI/policy.tmdl` and project
`extension.tmdl` to `<model-name>.tmdl` before building.

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

Power BI items are built as a separate step from Lakehouse and Warehouse items.
Build the sources first, then the semantic models and Reports. A SemanticModel
and its Reports can share one Build.

Edit `reporting/PowerBI/Reporting/Reporting.tmdl` to define calculated content:

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
the selected model uncertified. Every selected model compiles and deploys, even
when its effective signature matches the installed model.

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

### Weaver annotations

Start with ordinary PBIP modelling, add `Weaver.Source` lineage and metadata,
apply organisation policy, and test the model. Use a same-name TMDL overlay for
project edits. Shared edits can form named local definitions that several
outputs compose. Custom annotations remain an optional last transformation over
the fully composed input.

`Weaver.*` annotations opt a PBIP into native transformations. They can also live
in either `policy.tmdl` or `<model-name>.tmdl`. Weaver merges the layers before
executing the annotations. Ordinary annotations pass through; an unknown
`Weaver.*` name is an error. Executed annotations remain on surviving objects. Native dotted
annotation identifiers are quoted, such as `annotation 'Weaver.Source' = ...`.
Weaver quotes supported bare identifiers in the effective package while retaining
their full names and values.

Annotations execute in two phases over the composed model. `schema` annotations
run before source metadata and generated columns are determined. `post_schema`
annotations run over the completed schema. Each occurrence executes once, in
declaration order within its phase. `Weaver.Source` and `Weaver.MeasureTable` use
`schema`; the other built-ins use `post_schema`.

Weaver's own annotations are
[`semantic_models/builtin_annotations.py`](src/weaver/semantic_models/builtin_annotations.py):

- `Weaver.Source` on a table names `Warehouse/<item>/<schema>.<object>` or
  `Lakehouse/<item>/Tables/<schema>.<object>`. It declares logical lineage and
  associates available catalogue metadata. Authored partitions, source expressions,
  modes, columns, types and mappings remain intact. Only missing descriptions are
  enriched, matching columns by `sourceColumn`, or by name when absent.
  A table without a partition generates its source and takes every source column;
  authored columns refine the source columns they name. Generated tables share one
  M expression per logical source. Their workspace and CLI source overrides must
  resolve to the selected or installed managed target; build the source into its
  new target before changing that binding.
- `Weaver.BaseSemanticModels` on a model lists local semantic-definition names,
  one per line. A name includes its native directory and same-name overlay.
  Bases compose recursively in declared order; later properties win. Organisation
  policy applies once to the resulting model, followed by that model's own
  overlay. Self-references, cycles, missing local names and repeated ancestors
  (including diamonds) fail with a scoped diagnostic. Composition adds no
  dependency on a base's physical Fabric item.
- `Weaver.MeasureTable = true` on a table generates a calculated partition over
  `INFO.VIEW.MEASURES()` with the columns Measure name, Expression, Format
  string definition, Description, Display folder, Table and Data category. Native `isHidden` controls visibility.
  Use a bare table or the existing generated recipe; authored partitions are
  reported as a conflict.
- `Weaver.Switch` on a measure lists `Table[Measure]` references, one per line.
  A unique unqualified reference is accepted. The generated value and dynamic
  format expressions select through the measure table's `[Measure name]` column. Static
  formats and context-independent dynamic formats are supported. Ambiguous names,
  duplicate selector labels, switch-to-switch references and measure-context
  dependent format expressions fail with diagnostics.
- `Weaver.AutoHideColumns` on a model or table sets `isHidden` for matching
  columns. Its value is one or more glob patterns, one per line; `*` and `?`
  have normal case-sensitive glob semantics.
- `Weaver.AutoHideForeignKeys` on a model takes `true` or `false`. It hides
  actual many-side relationship columns. Column names do not determine keys.
- `Weaver.Exclude` on a table or column takes `true` or `false`. It removes the
  object and verifies its absence in TMSL readback. References elsewhere must
  remain valid; Weaver does not rewrite DAX or remove unrelated model objects.

For example, an extension can generate a source table and metadata selector while
keeping measures in native DAX:

```tmdl
model Model
    annotation 'Weaver.AutoHideColumns' = "*SK"

table Sales
    annotation 'Weaver.Source' = Warehouse/Serving/Cake.Sales

    measure Revenue = SUM(Sales[Amount])
        formatString: #,##0.00

    measure Units = SUM(Sales[Quantity])
        formatString: #,##0

table Metric
    annotation 'Weaver.MeasureTable' = true

    measure Value
        annotation 'Weaver.Switch' = ```
            Sales[Revenue]
            Sales[Units]
            ```
```

### Project annotations

A project adds annotations of its own, one class per file, in
`SemanticModel/annotations/`. They apply to every semantic model in the
project and use the same live TMDL objects as Weaver's annotations:

```python
# SemanticModel/annotations/DWG__HideIntegerColumns.py
from weaver.semantic_models import Annotation


class DWG__HideIntegerColumns(Annotation):
    scopes = {"model", "table"}

    def apply(self, target):
        tables = target.tables if target.parent is None else [target]
        for table in tables:
            for column in table.columns:
                if column.dataType == "int64":
                    column.isHidden = True
```

```tmdl
model Model
    annotation DWG.HideIntegerColumns = true
```

The class name is the annotation name with `__` for `.`, and the file is named
after the class. `scopes` lists the TMDL object kinds it may annotate.
`self.value`, `self.lines()` and `self.boolean()` read the declared value, and
`self.error()` fails Build at the declaration. Once a project defines a
namespace, an undefined annotation in it is an error. The `Weaver` namespace is
reserved.

Custom annotations default to `phase = "post_schema"`. Set `phase = "schema"`
on the class to establish structure or introduce a source before metadata
resolution. Default handlers see source-generated columns and the known
MeasureTable columns. A new source dependency in `post_schema` fails with a
diagnostic; source resolution occurs once.

Annotation files are trusted code: Build executes them while it compiles each
semantic definition. A file may import installed packages such as `weaver`, but
not other project files. After native, policy and target layers merge, each
selected model runs schema annotations, materialises source metadata and generated
columns, then runs post-schema annotations. Each occurrence executes once in its phase.
Its final effective package determines the persisted signature. Every selected
model deploys; selected consuming Reports redeploy with it. Report-only Build
retains independent change detection.

Generated source tables follow the model's `defaultMode`: `import` reads the
shared source through M navigation, and otherwise they use Direct Lake through
their typed SQL endpoint.
Authored data definitions remain intact, including transformed M and calculated
partitions. Source-generated columns receive declared hiding policies after
inference; exclusions remain removed. This compilation uses the ordinary Build,
catalogue and installed dependency graph. Ordinary PBIP adoption can start with
logical source annotations and add generation only for tables without partitions.

### Tests and Assumptions

`PowerBI/<project>/tests/<Model>/` and `assumptions/<Model>/` hold DAX
validations for the named local definition, including standalone and composed
models. Each file is named `<Schema>.<Object>.dax`. Base composition reuses TMDL;
each model owns its validations. A Test compares its
Expected SQL, run in its Expected source, with its DAX, run against the model:

```text
/*
Test ID: Sales.RevenueReconciles

Description: Revenue in the model agrees with the serving Warehouse.

Primary key: Month

Expected source: Warehouse/Serving

Expected SQL: |
  SELECT Month, SUM(Revenue) AS Revenue
  FROM Cake.Sales
  GROUP BY Month
*/

EVALUATE
SUMMARIZECOLUMNS('Date'[Month], "Revenue", [Revenue])
```

The comparison is the ordinary Test contract: both sides return the same
columns in the same order, and the Primary key only pairs discrepancies. DAX
column labels such as `'Date'[Month]` compare as `Month`. Expected SQL is T-SQL
for a Warehouse and Spark SQL for a Lakehouse, naming objects as
`Schema.Object` in the Expected source. An Assumption's DAX returns the rows
that violate it.

Build publishes each validation's definition to the catalogue. A validation edit
leaves the model's effective signature unchanged and resets only that validation's
TestStatus. Every selected model still deploys and resets its LoadStatus.
`weaver test SemanticModel/Reporting`
runs them, records TestStatus and Log, and Health treats them like any other
validation: a refresh after a pass makes them stale until they run again.

### Without a catalogue

A workspace with no catalogue still builds and refreshes semantic models:

```bash
weaver build ./reporting --item SemanticModel/Reporting --workspace Analytics
weaver load SemanticModel/Reporting --workspace Analytics
```

Build deploys each selected model and verifies its readback. With no catalogue
Load records nothing. Authored tables can retain logical `Weaver.Source` annotations
without catalogue metadata. Source generation, Lakehouse and Warehouse operations,
installed lineage, Tests and Assumptions, and `load --stale`, `--name` or `--reload`
need a catalogue.

### Load and connections

```bash
weaver load SemanticModel/Reporting --workspace Analytics --catalogue Warehouse/Catalogue
```

Load uses the installed catalogue, so it needs no source checkout. It refreshes
the model and records request ID, outcome and timing in LoadStatus and Log.
Every model Build permits required clearing of processed semantic data and leaves
LoadStatus Pending. It does not delete Warehouse or Lakehouse source data.
Report-only Build preserves model LoadStatus. Semantic-only Build and Load use REST/TDS
and start no Spark session.

Source mapping changes the definition, naming each source by its Warehouse or
Lakehouse name. Before refreshing, Load binds every unbound SQL data source to
the one cloud or gateway connection whose path is that source's
`server;database`; `weaver build --bind-data-sources` does the same after
deploying. A source no connection reaches keeps the connection Fabric gave it,
such as single sign-on. A source several connections reach fails, naming them.
Weaver creates no connections or credentials.

`policy.tmdl` and `<model-name>.tmdl` merge by native structure. An object is
identified by its kind and name within its parent, and a property by its name.
A new object is added whole, an existing one merges recursively, and a supplied property,
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

The former `addon.yml` and `.source` authoring syntax is removed, and a `.dax`
file is a Test or Assumption. Native TMDL expresses relationships and
calculated tables.
Delegated-user Azure CLI/browser access remains a separate user validation.

## Documentation

- [Website](https://weaverstack.dev)
- [Documentation](https://docs.weaverstack.dev)
- [Source](https://github.com/matthias-wong-dev/weaverstack)

## Licence

Mozilla Public License 2.0. See [LICENSE](LICENSE).
