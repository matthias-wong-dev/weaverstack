# Preservative build

`Prohibit rebuild` protects the data an object holds. The physical object is not
dropped, not recreated, and not altered. What a declaration may still change is
anything that leaves the stored rows alone: a description, and the load code that
future loads will run.

A declared schema change is therefore not something the current build can honour.
The column would have to appear on the physical table, and putting it there is an
alteration.

## The gap this leaves today

A schema change to a protected object is accepted without complaint, and the
result is a load that cannot run.

```text
CUR__Customer.py gains "CustomerLabel: string"
    ↓
select_build puts CUR.Customer in `prohibited`, so it is neither dropped nor built
    ↓
the load artefact has its own identity, so it is not in `prohibited`
    ↓
the deployed load module is regenerated against the new declared schema
    ↓
the merge names CustomerLabel, and the physical table does not have it
    ↓
UNRESOLVED_COLUMN at load
```

`select_build` in `weaver.build_bundle.incremental` reads `prohibited` from
`repository.source_documents`. A load artefact's identity comes from
`runtime_artefacts(repository)` through `_selectable` in
`weaver.build_bundle.planner`, which is a different namespace, so a load artefact
is never prohibited.

This was found by the Fabric acceptance journey, whose scenario F now makes only
the changes `Prohibit rebuild` permits. The journey therefore no longer covers a
schema change to a protected object.

## What the work needs

Both candidate behaviours need the same thing the build does not have: the
physical column shape at planning time.

- Refuse the change, naming the object, the column and the next action.
- Alter the table preservatively, adding the column and leaving the rows.

`TargetInventory` in `weaver.build_bundle.prune` carries object names and no
column shapes, so it would gain them, and `to_mapping` would carry a new
`format_version` for the desktop handover. That is one extra read per bound item
on every build.

Until then, a protected object whose declared schema changes installs a load that
cannot run.
