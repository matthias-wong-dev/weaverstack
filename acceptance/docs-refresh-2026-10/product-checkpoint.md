# Product-site implementation evidence

Behaviour target: `74b4d15ee65ed2740593af1f4d459771b801b132`. Documentation base: `f35c943ed0ca423b92ab53aa571c43ae2817b74e`.

Publication remains held for a matching package and explicit approval. No merge, release, capacity operation or live resource mutation occurred.

## Reader outcomes

- Homepage and Get started expose existing Power BI and small Fabric paths with distinct prerequisites.
- One top-level Features overview and native disclosure, four children in the agreed order, separate Built for scale. Every existing header/footer is reconciled.
- `/features/`, `/lakehouse/`, `/warehouse/`, `/semantic-models/` and `/reports/` lead with the work each item makes easier and link to specific developer task owners.
- `/scale/` follows 3 / 30 / 300 / 3,000 objects without making performance tuning a starting requirement.
- All seven historical benchmark operations are static HTML and backed by a provenance-labelled JSON file. Unknown raw run ID/timestamp remain null.
- Existing Problem, How it works, comparison and README are consistent with the native Power BI route and runtime/managed-state boundaries. Sitemap, manifest and existing publishing assets remain intact.
- Product runtime/tests/configuration are unchanged from the behaviour target.

## Executed checks

From the product worktree:

```bash
python tools/check_product_site.py --developer-site <integrated-built-docs>
node --check docs/assets/site.js
/opt/data/repos/weaverstack/.venv/bin/python -m ruff check tools/check_product_site.py
/opt/data/repos/weaverstack/.venv/bin/python -m ruff format --check tools/check_product_site.py
git diff --check
```

HTML checker passes eleven canonical routes, unique titles/descriptions/H1s, exact Features children, independent Scale link, shared footers, local fragments/assets and sitemap. All seven benchmark rows match raw seconds, displays and context in the JSON. All 21 distinct developer destinations exist in the integrated build.

The documentation repository's `tools/check_paired_sites.py` checked both builds: **126 HTML pages, 20,500 local page/asset/fragment references, 166 cross-site links, zero broken targets/fragments**, and seven matching developer benchmark rows.

Parent browser harness passed **55 route/viewport checks** at 375, 768, 1024, 1250 and 1440px. Native disclosure works by keyboard and touch; links stay inside the viewport, focus is visible in keyboard modality, Escape closes/returns focus, mobile toggle closes, and no-JavaScript navigation reaches the overview and all four children at 375/1440px. No console/page errors. Seven benchmark rows remain accessible without scripting.

Selected opening/code/table states were inspected visually at phone/desktop widths. Screenshots are private review artifacts, not published site assets or stitched full-page captures. GitHub CTA final measured contrast is **18.36:1**. The real product copy button copied the exact full displayed configuration through the browser clipboard API.

The complete Warehouse Table shown on its product page was checked through pinned `weaver_cli` and passed. The Lakehouse Python body is explicitly an illustrative excerpt; complete projects belong to its linked tutorial. No SQL, Python extraction or service execution is inferred from local parsing.

## Benchmark provenance

Fresh `gh pr view 193 --repo matthias-wong-dev/weaverstack --json title,body,headRefOid,mergedAt,url` established head `39dd32dc4bff4998f3e4a5a98d96d3079477e029`, merged at `2026-10-06T20:27:24Z`. The PR substantiates the seven operations, object/node/action distinction, F64 capacity, lane settings, convergence Tests and two tuning probes. It is historical reported evidence, not a new documentation-release run. No stress harness was rerun.

## Corrected local failures

- Storage exhaustion during setup/browser work: reused the existing runtime and removed only identified task-created duplicates/regenerable cache.
- An HTML formatting pass briefly reconstructed pages from a truncated tool display; the checker rejected the missing H1. Complete authored buffers restored the pages before any publication. Subsequent eleven-route checks pass.
- Reports cards retained intrinsic code width on phones: measured offending boxes, set `min-width: 0`, kept local code scrolling; all route widths pass.
- Header button inherited the navigation's dark foreground: explicit button foreground restores measured contrast.
- Browser harness initially asserted keyboard focus styling immediately after touch; entering keyboard modality before the assertion makes the test reflect the actual interaction.
- Full-page screenshot capture failed; viewport JPEG capture succeeds. No full-page visual coverage is claimed.
- The standard-library checker initially passed lint but not repository formatting; it was formatted and rerun before delivery.

## Remaining publication gates

The final independent reader batch remains a separate gate until its findings are accepted or corrected. Service deployment/refresh, Report viewing/binding, DAX/SQL execution, deployed rule/variant inspection and licensing acceptance are not run. Matching package availability and explicit publication permission remain required.

Private paired static previews are prepared separately from canonical files. Their links remain within the pair; local targets return HTTP 200, HTTPS proxy mappings were read back, existing Serve routes were preserved and no public Funnel was enabled. A separate tailnet peer has not been observed loading them; local userspace self-hairpin timed out. Preview addresses are handed over privately, not stored in the public site.
