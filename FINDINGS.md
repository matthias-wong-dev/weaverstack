# Findings while building the acceptance estate

## F1. A Lakehouse physical shortcut refuses a Warehouse source (product gap)

`weaver/build_bundle/shortcut_sources.py::_resolve` refuses any physical shortcut
whose target item is not a Lakehouse:

    A OneLake shortcut reads a Lakehouse, and Warehouse items are reached over TDS.

But `read_runtime_sources`, 20 lines above in the same module, does exactly what
the message says is impossible: it resolves the catalogue **Warehouse** and points
a built Lakehouse's shortcuts at `Tables/_/<table>`, because a Fabric Warehouse
publishes each table as a Delta directory in OneLake.

Consequence for the plan: an external Warehouse in `PYTEST_WORKSPACE_EXT` cannot be
read at all today.
  - Lakehouse OneLake shortcut: refuses a Warehouse source.
  - Warehouse view shortcut: `view_statement` renders three-part `[item].[schema].[object]`,
    which reaches only the same workspace.

Fix taken: allow a table or schema physical shortcut to name a Warehouse, resolved
by its declared type. A Folder shortcut still refuses one, because a Warehouse has
no Files area. Own commit, revertible.

## F2. T-SQL has no bookmark-windowed incremental

`tsql_load.py::_bookmark_key` reads `@weaver_bookmark` only for a `Static` object:
"every other load writes its bookmark at the end and never asks what it was."
There is no token an authored T-SQL query can use to window on its own bookmark.

So the plan's "SERVE.Customer: T-SQL bookmark-driven incremental load" is not a
current capability. What T-SQL `Incremental: true` means is: a staging query plus a
second query returning the keys to retire, reconciled by primary key. Bookmark
windowing is a Python/Folder capability (`self.bookmark()`, `files_since`).

Estate built to the real contract. Not a defect, but the plan's wording should change.

## F3. A Lakehouse cannot logically shortcut a Warehouse-bound item

`build_bundle/shortcuts.py::_unsupported` refuses a logical Lakehouse shortcut
whose source item is bound to a Warehouse: "there is no shortcut form for the
Warehouse source". Same underlying capability as F1, which is now allowed for a
physical target.

Consequence: the Published Lakehouse names the Serving Warehouse *physically*, so
the acceptance repository carries a `{{SERVING_WAREHOUSE}}` placeholder rather
than following Serving's own binding.

Not fixed. One product change per PR, and this one is the user's call.

## F4. T-SQL incremental cannot claim retires from its own target

A T-SQL incremental object's second statement returns the keys to retire. Reading
its own target there is refused as a self-dependency:

    Warehouse/Serving/SERVE.Customer depends on itself

Python has no such limit: `CUR.Customer` computes its retire claim from
`self.dataframe()` against the source. So "retire what the source no longer
offers" is expressible in Python and not in T-SQL.

Estate models a retire feed instead: `CUR.RetiredCustomer` materialises the
withdrawn ids from the event stream, and `SERVE.Customer` claims from it. That is
realistic, and it exercises the T-SQL retire path properly. Worth documenting as
the T-SQL incremental contract.

## F5. Lakehouse → Warehouse → Lakehouse cannot be built in one build

The two ways a Lakehouse could read a Warehouse are each blocked, and together
they block the return crossing entirely:

- a **logical** Lakehouse shortcut refuses a Warehouse source (F3);
- a **physical** one resolves against the estate as it already stands, so it
  cannot name an object the same build is about to create:

      shortcut WH__Reporting in Lakehouse/Published points at
      Warehouse/PYTEST_WH_1/SERVE.Reporting, and 'SERVE' is not in
      .../Tables

Every foreign shortcut resolves correctly, including both Warehouse ones, so this
is specific to a source the same build creates.

The fix is F3's: allow a logical Lakehouse shortcut whose source item is bound to
a Warehouse. Ordering then comes from the build's own item graph. Three places:
`_unsupported` in `build_bundle/shortcuts.py` (allow it), `shortcut_payload` (mark
the frozen source as a Warehouse), and `executors/shortcut.py` (resolve a
Warehouse source item and address `Tables/<schema>/<object>`).

Not done: a second product change touching the installer is more risk than value
in one overnight PR. The Published item stays in the repository, unbound, so the
intended architecture is visible and binding it is a one-line change once the
product allows it.

## F6. A schema shortcut crashed the second build (fixed)

`determine_impact` expands every changed root across the dependency graph. A
schema shortcut's destination is a `WeaverSchemaId`, which is not a node in that
graph, so classifying one as changed raised:

    GraphError: unknown node: 'Lakehouse/Landing/Reference'

The first build succeeds because the shortcut has no Registry row yet and is
classified *new*. The second build classifies it *changed* and crashes. So any
repository holding a schema shortcut could be built once and never again.

The graph carries *table* shortcut destinations as nodes, which is why this was
specific to schema shortcuts: a schema establishes a namespace, so nothing can
name it as a dependency. Fixed the same way a runtime artefact is: it ends the
walk rather than starting one.

Regression: `tests/targeted/test_shortcut_planning_install.py`
`test_a_changed_schema_shortcut_does_not_walk_the_graph`.

Found by the acceptance journey's scenario B, which is the reason the scenario
exists. The remaining question the journey will answer is *why* an unchanged
schema shortcut classifies as changed at all.

## F7. A schema shortcut was rebuilt on every build (fixed)

`projection._identity` writes a schema shortcut's Registry row with the schema in
both the schema and object columns, because the Registry keys on both. Reading it
back, `_row_identity` reconstructed a two-part `WeaverDocumentId`
(`Lakehouse/Landing/Reference.Reference`) instead of the `WeaverSchemaId`
(`Lakehouse/Landing/Reference`) the declaration is keyed by.

So classification never found the shortcut's installed signature. It was in the
inventory, so not *new*; its signature read as absent, so *changed*. Every build
therefore deleted and recreated it.

That is real churn, and it caused a load failure: the acceptance journey's second
build recreated the schema shortcut, and the load immediately after read through
it inside Fabric's shortcut-discovery window:

    [PATH_NOT_FOUND] .../Tables/Reference/Product

Fixed by reading the row back as the identity that wrote it, with one shared
reader (`catalogue.claims.catalogue_columns`) so the two sides cannot drift.
Four consumers of `Catalogue.registered` assumed a document identity and now
state what a namespace means:

- `catalogue_columns` — the stored column pair;
- `state.reconcile_catalogue_state` — the inventory lookup;
- `load_plan.InstalledObject.physical` — no shape on a namespace;
- `load_plan.primitive_candidates` — a namespace has no load primitive;
- `load_plan._resolve_reference` — a `shortcuts.Reference` import resolves to it.

Regression: `tests/test_catalogue_read_primitive.py`
`test_a_schema_shortcuts_row_reads_back_as_the_schema_identity_it_wrote`.

Found by the acceptance journey. F6 was the crash this hid behind.

## F8. The shortcut discovery wait probes the wrong surface (NOT fixed)

`executors/shortcut.py` waits until every table shortcut it created is readable,
and probes one surface:

    SELECT * FROM <four-part name> LIMIT 0

An authored load reads the other: `_TableReader.dataframe()` reads the Delta
files by path. The two settle independently, so a load in the same run reaches a
path Fabric has not finished discovering:

    [PATH_NOT_FOUND] .../Tables/Source/Product

Intermittent, and F7 makes it more likely rather than less: with the schema
shortcut no longer rebuilt every time, an unchanged build is fast, so less time
passes between creating a shortcut and reading through it. Fixing one defect
exposed the other.

**Attempted and reverted.** Adding a second probe by path fails, because the
address the executor has is the wrong spelling:

    pyspark UnsupportedOperationException on SELECT * FROM delta.`<path>`

`_location(...)` returns `resolver.delta_table(...)`, which on a desktop build is
the OneLake DFS **https://** URL. Spark needs the **abfss://** form, and building
one needs the target's workspace and item ids, which the executor does not carry.
So the fix is real but not a two-liner, and it needs a Fabric run to confirm the
second probe actually closes the window. Left for a decision rather than guessed
at overnight.

Consequence for the journey: scenario C onward is blocked behind this, and fails
intermittently. Scenarios A and B pass.

The estate reads its foreign tables through table shortcuts rather than the
schema shortcut, because a table shortcut is at least waited on. The schema
shortcut stays declared and is asserted structurally in scenario A, so build,
prune and Registry behaviour over one is still covered. That is where F6 and F7
came from.
