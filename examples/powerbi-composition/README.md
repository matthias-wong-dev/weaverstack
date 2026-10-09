# Named Power BI outputs

Save an ordinary PBIP under `PowerBI/Sales/`, with its native
`Normal.SemanticModel` and `Normal.Report` directories. Keep native data modelling
in Power BI: columns, relationships, partitions, DAX and storage modes stay there.

## Lineage, policy and an overlay

A same-name `PowerBI/Sales/Normal.tmdl` adds logical lineage without copying the
native model. For an existing Sales table, use a reference:

```tmdl
ref table Sales
    annotation 'Weaver.Source' = Warehouse/Serving/Cake.Sales
```

An authored partition makes this metadata-only: it keeps its data definition and
can receive missing descriptions from the source catalogue. A table without a
partition generates its source and columns. Build the managed Warehouse source
before building its consuming model.

Put organisation rules in `PowerBI/policy.tmdl`:

```tmdl
model Model
    annotation 'Weaver.AutoHideColumns' = *SK
    annotation 'Weaver.AutoHideForeignKeys' = true
```

Use native DAX for tests and measures. Test the authored model in Power BI; Build
verifies requested changes and observed definitions, not data-value correctness.
The normal Weaver Load/Test/catalogue lifecycle for managed sources still applies.

## Compose variants without duplicating the model

Create `PowerBI/Sales/Executive.tmdl`:

```tmdl
model Model
    annotation 'Weaver.BaseSemanticModels' = Normal

ref table Sales
    measure Revenue
        formatString: #,##0.00
```

Create `PowerBI/Sales/Public.tmdl`:

```tmdl
model Model
    annotation 'Weaver.BaseSemanticModels' = Normal

ref table Sales
    description: Published sales facts
```

`Normal` includes its native directory and same-name overlay. Each variant gets
those raw definitions, organisation policy once, then its own overlay. A second
base can be listed on the next line in a fenced annotation; later base values
win. Names containing spaces or commas remain whole names, not CSV entries.
Bases must be local; cycles and repeated ancestors, including diamonds, fail.

Save variant Reports as `Executive.Report` and `Public.Report` within this project.
A variant has no native directory, so its name links the Report, whatever its
`byPath` reference names. A differently named Report links to the model its
`byPath` reference names, such as `../Normal.SemanticModel`; with a
`byConnection` reference it deploys as authored.

## Provision and build

Configure each logical item in the existing `workspace-config.yml` `targets`
section when physical names differ. Ordinary initialise adopts all discovered
models and Reports; it leaves the authored PBIP and overlays untouched:

```bash
weaver initialise --workspace Analytics --project-folder ./reporting --non-interactive
weaver build ./reporting --item PowerBI/Sales --non-interactive
```

A variant-only Build reads its base definitions but deploys only selected items:

```bash
weaver build ./reporting --item SemanticModel/Executive --item Report/Executive --non-interactive
```

Custom annotation classes live in `SemanticModel/annotations/` and subclass
`weaver.semantic_models.Annotation`. They run once on each selected model's
fully composed, target-bound input, before source metadata/generation. They do
not execute on unselected bases. Final signatures and catalogue table ordinals
include their additions and removals. Every selected model deploys; an unchanged,
catalogue-backed Report-only Build can skip deployment.
