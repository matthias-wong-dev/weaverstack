# Product-site implementation checkpoint

Source target: `74b4d15ee65ed2740593af1f4d459771b801b132`.
Developer target: `f35c943ed0ca423b92ab53aa571c43ae2817b74e`, with reference/fixture work in progress.
Publication remains held for a matching package and explicit approval. No merge, release, capacity or live resource mutation occurred.

## Implemented locally

- One top-level Features route and native disclosure, four child links in the approved order; Built for scale remains separate. Every existing product header/footer is reconciled.
- `/features/` and four flat item pages, with distinct outcomes and owning developer URLs.
- Power BI and Fabric homepage/entry routes; catalogue-free language and native-output limitations.
- 3 / 30 / 300 / 3,000 scale narrative and all seven historical benchmark operations.
- Provenance-labelled static benchmark JSON, sitemap and manifest changes.
- Small CSS/JS extensions; standard-library product-site checker. No platform change.
- Targeted consistency changes to Problem, How it works, comparison and README.

## Commands actually exercised

From product worktree:

- `python tools/check_product_site.py`: **pass**. Eleven canonical routes, unique titles/descriptions/H1s, exact Features child order, independent Scale link, shared footers, local links/fragments/assets and sitemap checked. All seven static benchmark rows match raw seconds, display labels and contexts in the JSON. Developer destination existence deliberately **not checked yet**; requires the integrated built docs site.
- `node --check docs/assets/site.js`: **pass**, syntax only.
- `git diff --check`: **pass**.
- `PYTHONPATH=<pinned-source>/src /opt/data/repos/weaverstack/.venv/bin/python -m weaver_cli check <editor-cache>/weaver-product-example`: **Project valid.** This checked the complete Warehouse Table example shown on the Warehouse page. It did not execute SQL or a service deployment.

The Lakehouse Python body is explicitly an illustrative body excerpt. Complete files belong to the linked tutorial; no remote extraction/CSV execution is claimed.

## Benchmark provenance

GitHub PR #193 freshly read through `gh pr view 193 --repo matthias-wong-dev/weaverstack --json title,body,headRefOid,mergedAt,url`. Reported head: `39dd32dc4bff4998f3e4a5a98d96d3079477e029`, merged at `2026-10-06T20:27:24Z`. This is the PR head, not an invented benchmark run identifier or timestamp. JSON keeps unknown run fields null. The PR body substantiates all seven operations, declared-object composition, capacity, lane configuration, convergence Tests and the two tuning probes. No stress harness rerun.

## Failures corrected locally

- Locked MkDocs installation initially failed with no space left on the 12GB persistent volume. Removed only the duplicate product environment created for this task and regenerable package cache; reused the existing source interpreter with explicit pinned PYTHONPATH. Locked docs install then succeeded.
- A formatting pass reconstructed new HTML from a display-window response and briefly truncated new pages. The HTML checker rejected the missing H1. Restored from complete authored buffers, formatted with real file bytes, searched for truncation markers and reran the eleven-route check successfully. No truncated files were published.
- Removed only stale editor web-cache entries older than the task to recover working space. Other agents' repositories, drafts and evidence were not deleted.

## Remaining A5/integration gates

- Source-backed claims reviewed against A1/A2 technical findings and final fixture definitions.
- Browser keyboard/touch/no-JavaScript, responsive, focus and visual inspection; syntax and HTML checks do not establish these.
- Developer benchmark table checked against the same facts.
- Staged developer destination links; live destinations checked only after approved publication.
- Independent reader walkthroughs and actual service acceptance remain unrun.
- Recheck open Installation identity and Dashboard work before updating the target revision or merging.

This is a working-copy checkpoint, not completion of A5 or the documentation plan.
