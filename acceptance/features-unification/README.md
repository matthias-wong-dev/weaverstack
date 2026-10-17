# Unified Features — implementation and local acceptance

Recorded 2026-10-15T09:39:57+11:00 (Australia/Canberra).

## Revision and scope

- Product baseline: `bc19acbb3bc4286b12e559b25ffc3faf8e5f1f10`, existing draft PR #222, `docs/product-and-scale-refresh`.
- Technical-documentation evidence: `df5e266ffe5c6b893ef7a6291ba7b23ab0ed2b45`, existing draft PR #3. Its only companion change is migration of `tools/check_fabric_first_story.py`; docs, examples, configuration and item-first navigation are untouched.
- Final tested/pushed heads and full file manifest are reported in the delivery record. No merge, deployment, release, Fabric execution, capacity change or tenant action is authorised or claimed.

## Content and source evidence

All **34** capabilities remain visible, grouped **5 / 8 / 7 / 7 / 7** across five stages. Four spotlights occur in order: managed cross-engine DAG, generated Warehouse engineering, Power BI foundation/variants/bindings, Mirror.

`claims.json` maps each capability to the approved brief, final copy, deeper destinations and individually hashed technical owners. The **31** distinct source pages were reviewed in four source batches; every recorded hash was rechecked before this note. `feature-contract.json` owns stable IDs, grouping and owner links; checker assertions deliberately do not lock public sentences.

Important scope corrections:

- Incremental Build's unchanged-work claim concerns source-data Build; selected models still redeploy even unchanged.
- Built-in annotation copy names supported measure-selector generation rather than unqualified measure organisation.
- Custom annotations are trusted executable Python, not a sandbox.
- Bookmarks belong to loadable Tables/Folders and represent clean processing boundaries.
- Protected ownership is a Build replacement guard, not a promise against Load, Wipe or re-Mirror.
- Spark SQL dependencies are explicit; supported Python/T-SQL inference is not universal.
- Catalogue/Health concern registered managed work; no automatic workspace-wide inventory is implied.

## Single cross-engine graph

`dag-source-results.json` records a production-parser/compiler/Catalogue-projection/LoadDag probe against a coherent neutral fixture assembled from the existing Parcel Lakehouse and Power BI examples:

1. `Lakehouse/Landing/Files/Parcel.Events` (Python Folder)
2. `Lakehouse/Landing/Tables/Parcel.Event` (Python Delta Table)
3. `Lakehouse/Landing/Tables/Parcel.CurrentStatus` (Spark SQL Delta Table, explicit dependency)
4. Generated `endpoint_refresh` barrier
5. `Warehouse/Dispatch/Parcel.Delivery` (T-SQL Table reading the logical Shortcut)
6. `SemanticModel/Parcel` refresh (authored `Weaver.Source` lineage)

This is **offline source-shape evidence**. Installed source rows and physical IDs are explicit test boundaries. Model certification is projected from actual compiled native table names; it is not a fabricated API response or service certification/readback. Network connects are refused. Authored native files were hashed before and after compilation and remained unchanged. The production planner, not a rewritten graph algorithm, ordered the work and inserted the endpoint barrier. Live publication, SQL endpoint visibility and model refresh remain separate gates.

## Retired routes

`retired-routes.json` records the four canonical docs destinations. The repository is public, and the URLs have been present in its publicly accessible draft branch/PR. Current `main` contains none of the four route trees and its path history has no commits for them; Pages is configured as legacy static hosting from `main:/docs`. There is no positive evidence they were publicly served. That is **not proof that no external links exist**, so inbound compatibility is retained conservatively.

Each old route is now a minimal, accessible **noindex,follow pointer**, with a canonical docs URL and visible direct link, no shared product navigation, no script dependency and no automatic refresh. The former pages had only the `main` fragment, retained by the pointers. All four are excluded from the product sitemap. There are **8 canonical product pages**, including the separately approved Story, plus **4 non-indexable pointer routes**; no full item-feature marketing pages remain.

Browser tests reached every pointer and clicked through to the corresponding freshly built documentation overview with JavaScript both enabled and disabled. The normal homepage cards/footer item links bypass the pointers and go directly to developer docs.

## Completed checks

- Product route/metadata/fragment/social/analytics/benchmark guard: **8 canonical pages, 4 pointers, 34 visible features, 4 spotlights**.
- Strict companion MkDocs build: pass.
- Paired rendered crawler: **162 HTML pages; 23,163 local links/assets; 276 cross-site links; 0 broken links/fragments; 7 benchmark rows**.
- Migrated Fabric-first guard: pass; **94 authored commands** parsed against the real CLI.
- Estate example guard: **2 coherent project checks, 79 commands, 3 production selection plans, 6 configurations**; no service calls.
- Product-checker regression suite: **8 tests**. It rejects the old dropdown, missing/duplicate capability, disconnected graph, stale product link and indexable pointer while permitting a copy-only sentence change.
- Focused DAG/semantic-source suite: **199 passed**; separately, Mirror/protection/lifecycle suite: **111 passed**. Counts are kept as separate runs, not added to historical/overlapping totals.
- Features browser sweep: **10 states**, widths **320, 390, 768, 1100, 1440**, JS on/off; 34 features and all four spotlights visible; no body overflow; anchors, skip link, keyboard outline, reduced-motion preference and direct docs destinations pass.
- Shared navigation sweep: **32 states**, all 8 canonical pages at **390 and 1241**, JS on/off; ordinary Features link, tight desktop fit, unscripted fallback and mobile Escape/focus restoration pass.
- Measured feature prose/link/diagram contrast: minimum **11.654:1**; diagram text at least **14px**.
- `node --check`, Ruff checks/format, and Git whitespace check: pass.

`preservation.json` proves approved Home, Problem, Get Started, How It Works, Scale, comparison and Story body bytes are unchanged except the approved item-link replacements. Runtime source, existing source tests, configuration, workflows and the measured benchmark JSON remain unchanged. Analytics loader/initialiser and copy/tab behaviour are preserved.

## Rendered owner read-through (not independent review)

1. The opening names familiar Python/SQL/native Power BI and Fabric before technical machinery.
2. The serious-engineering stage opens with one explicit Lakehouse → Warehouse → model-refresh graph, followed by generated SELECT engineering.
3. Seven native Power BI capabilities remain visible; separate variants lead to separate correctly bound Reports.
4. Seven operational rows give Catalogue, Health, history, failures, selection, concurrency and workflows their own labels.
5. The advanced toolkit retains Mirror, incremental Build, previews, frozen bundles, readiness and protected ownership with exact reset/protection boundaries.
6. Capability headings go directly to their technical owners; every stage also offers a task/reference route.
7. Spacious authoring rows evolve into engineering visuals, a denser two-column operations cluster and a compact three-column advanced toolkit—not 34 boxed cards.

Selected opening, all four visual treatments, operations and advanced layouts were inspected at desktop and phone widths. Screenshots and executable probes are retained in the task workspace, outside the repository publication assets.

## Remaining gates

- **Independent seven-point editorial review remains pending**: the explicit delegation permission prompt was unanswered; no reviewer was spawned. Owner findings above are not a substitute and are not represented as independent acceptance.
- Matching intended PyPI wheel installation and actual published-package capability acceptance were not run. Public copy retains the explicitly approved normal-pip, prospective matching-release path.
- No live Fabric/Spark/T-SQL deployment, source access/publication, SQL endpoint readback, model refresh/DAX query, Report rendering or service binding verification was conducted.
- Public deployment and remote-client tailnet access are not claimed. Existing private Serve mappings and local HTTP assets/destinations were verified; no hosting configuration was changed.
