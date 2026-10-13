# Fabric-first feedback revision

Verified 2026-10-14 01:50 AEDT. Behaviour remains pinned to `74b4d15ee65ed2740593af1f4d459771b801b132`. This is an editorial and private-preview revision, not runtime work or release permission.

## Feedback addressed

1. Fabric is the product proposition. Power BI independence is a secondary setup option, not a competing entry route. Home, Get started, Features, descriptions/manifest and installation use that hierarchy.
2. Authored discovery lists and examples use Lakehouse → Warehouse → semantic model → Report. The mixed real fixture configurations and their published twin use the same order. Literal generated interfaces retain their exact source syntax.
3. Product copy uses the existing senior-engineer prose guide. Both repositories’ PROSE.md now record the hierarchy, reader viewpoint and practical motivation. New headings state the work rather than sell it; substantive capability examples remain.
4. Scale copy is short and rounded: about an hour for the first 3,000-object F64 load, under 15 minutes for the later measured incremental loads. Exact seven-operation evidence remains in the product dataset and `scale-measurements.md`, outside the public prose. The earlier first incremental day was slower; the new wording deliberately says later loads, not every incremental run. Matthias also reported a separate 59-minute first load; that observation is not silently substituted into the original dataset.
5. Features opens with Matthias’s requested Fabric-first sentence and the four item cards. No catalogue-free exception appears in that headline or introduction. Reader-facing examples no longer lead with evaluator-only hash procedures or a separately requested export.
6. The dbt comparison includes native PBIP/TMDL model and Report deployment, modelling rules/variants, DAX-to-SQL validation and managed refresh order. It distinguishes native lifecycle support from dbt’s documented Power BI query connector: https://docs.getdbt.com/docs/platform-integrations/semantic-layer/power-bi.
7. Basics puts native model/Report deployment after Lakehouse and Warehouse authoring, without a separate Power BI navigation group. Advanced uses Semantic models. Existing URLs remain reachable for compatibility; the former Basics platform overview points to the ordinary flow.
8. Models are integrated into project structure, core lifecycle/dependencies/state, validation authoring, development, recovery and the four shared CLI leaves. The managed-source guide runs Load, Test and Health without item selectors after both Build stages, showing the common estate operations. Health is recorded operational evidence, not live Report rendering; exact operation limits remain in Reference.

## Real verification

- Strict MkDocs build passed. Canonical paired crawl: 126 HTML pages, 20,419 page/asset/fragment references, 166 cross-site links, zero missing targets/fragments.
- Product route/header/footer/metadata checker passed all eleven routes, retained seven dataset operations and both rounded public scale results.
- New `tools/check_fabric_first_story.py` passed Fabric-first discovery, installation order, native dbt comparison, ordinary navigation, whole-estate Health and 72 shared-flow command parses.
- Six complete fixtures passed real local Weaver Checks; 53 README commands parsed. Fixture checks used explicit offline boundary inputs, not service responses. No authored bytes changed during the checks.
- Thirteen journey pages passed 71 command parses, five configurations, three Python call signatures, twelve byte-identical fixture blocks and four exact assembled-project Checks/compilations.
- Four generators passed strict checks: 18 CLI leaves, 55 root Python exports, four extension classes and 23 catalogue tables. Generated markers/interfaces were not hand-edited.
- Reference example checker passed 103 command parses, six configurations, four operation call signatures, two DAX files and one native editing example.
- Focused pinned-source suite: 157 passed across model catalogue/build/load, installed validation, lineage and CLI Health/Load/Test/Power BI boundaries. No live Fabric resource declared or exercised.
- Browser: 55 product route/viewport checks, 190 developer route/viewport/theme checks, plus 60 new shared-flow checks. Total 305, with no page errors or document overflow. Keyboard/touch/focus/Escape, no-JavaScript access and real documentation search passed.
- Parent read the revised product copy continuously and inspected home, phone Features, environment code cards, documentation Home/Basics and phone dark-mode Health screenshots. No new independent reviewer was spawned; the earlier independent reviews remain evidence for their earlier iteration only.
- Ruff lint/format, JavaScript syntax and Git whitespace checks passed. Runtime/test/configuration/workflow trees remain identical to the pinned product revision.

## Private delivery

The existing preview roots were rebuilt. Local HTTP serves the revised content and the live Tailscale mappings remain:

- product HTTPS 9445 → loopback 8847;
- developer HTTPS 9446 → loopback 8846.

No Serve route was reset, no Funnel enabled, and no separate tailnet-peer browser observation is claimed. Raw local outputs and screenshots are under the private task scratch workspace. Draft PR heads/readback and CI are verified separately after committing this revision.

## Held gates

No merge/public deployment/release, live data/model deployment, refresh, DAX/SQL execution, Report rendering or live licence/permission acceptance. No capacity, tenant, credential or Environment mutation; no compiled-TMDL export. Publication remains held for a matching package and explicit approval.
