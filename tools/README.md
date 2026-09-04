# Website asset generators

`docs/` is the weaverstack.dev site: static HTML, no build step at deploy time,
served by GitHub Pages from this directory. Two things in it are generated, and
both are committed so the published pages stay static.

## The Sales example code

The site shows the project `weaver initialise --example` writes. Retyping it
would let the two drift, so each code block carries a marker naming the file it
shows, and the generator fills it from `weaver.onboarding`:

```html
<!-- example:Lakehouse/Landing/Tables/Sales__Customer.py -->
<pre>...</pre>
<!-- /example -->
```

Run it from the repository root after changing the onboarding example:

```bash
.venv/bin/python tools/build_site_examples.py
```

`--check` reports drift and writes nothing, which suits CI.

The item names the site uses throughout are `Landing` and `Curated`, set at the
top of the generator. Changing them there changes every filled block, and the
surrounding prose names them too, so change both.

## The scale page graph

`docs/scale/index.html` shows a real installed estate: the graph
`Catalogue.dag()` derives, which is the same installed graph load planning,
validation planning and health read. It is exported from a running project with
that project's own tooling, and laid out here:

```bash
.venv/bin/python tools/build_scale_dag.py path/to/catalogue-dag.json
```

That writes three files: `docs/assets/scale-dag.svg`, the whole graph as one
static SVG; `docs/assets/scale-dag.json`, the adjacency and node detail the
viewer reads when someone selects a node; and `docs/scale/index.html`, which is
`scale-page/head.html` and `scale-page/tail.html` joined around the inlined
graph. Inlining is what lets the graph render with no JavaScript, so
`docs/assets/scale-dag.js` only adds lineage highlighting and search on top.

The page's prose lives in those two fragments, not in `docs/scale/index.html`.
Edit the fragments and re-run the generator, or the next run overwrites you.

The export currently on the site came from
[ilovegov.datawithoutguessing.com](https://ilovegov.datawithoutguessing.com):
181 objects and 365 dependencies across a Lakehouse and a Warehouse. Weaver's
own catalogue item and its `_` schema are filtered out, because they describe
Weaver rather than the estate the page is about.

To refresh it, re-export from that project and re-run the generator. The stated
counts on the page are prose in `scale-page/head.html`, so check them against
what the generator prints.

## Previewing the site

```bash
python3 -m http.server 4173 --directory docs
```

The pages use absolute paths (`/assets/site.css`), so they need a server rather
than opening the files directly. `.claude/launch.json` has the same server as a
`site` configuration.
