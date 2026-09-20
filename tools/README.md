# Website asset generators

`docs/` is the weaverstack.dev site: static HTML, no build step at deploy time,
served by GitHub Pages from this directory. One part of it is generated, and it
is committed so the published pages stay static.

Page links use root-relative clean routes. Assets use paths relative to each page.
Preview the site through a server.

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

## Previewing the site

```bash
python3 -m http.server 4173 --directory docs
```

The clean routes resolve through the preview server. `.claude/launch.json` has
the same server as a `site` configuration.
