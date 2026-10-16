# Weaver brand assets

The four SVGs under `docs/assets/brand/` are unmodified originals from the supplied `weaver-logo-svg-kit.zip`.

- The blue folded W is the compact mark. The full logo stacks that W above an outlined lowercase wordmark. Preserve the source paths, gradients, proportions and spacing; do not reconstruct the wordmark with a font.
- Product headers and footers use the light mark on the site's near-white ground. The comparison-table heading uses the dark mark. Developer docs select the light/dark mark from the actual Material theme, including after a manual theme switch.
- The square 128 × 128 favicon centres the unchanged wide mark within a near-white rounded tile; it does not stretch the W to a square.
- The default 1200 × 630 social cards retain the full stacked lockup, the established “Making Fabric easier to use” proposition, and the corresponding site's domain. The existing incremental-load card retains its own headline and supporting wording.

## Regenerate

Run with Playwright and Chromium available; neither is a product runtime dependency:

```sh
python tools/render_brand_assets.py \
  --browser /path/to/chromium \
  --developer-site ../weaverstack-docs/docs \
  --review-dir /path/to/review
```

`--developer-site` is optional. When given, it copies the exact supplied SVG bytes and favicon to the documentation repository and exports its default and topic-specific cards. `--review-dir` retains full-size and 600 × 315 review images.

Inspect full-size and thumbnail renders before committing. The PNGs are checked-in publication assets, not deployment-time renders. Open Graph and Twitter both reference the same PNG for each page, with explicit dimensions and corresponding alt text. Retain absolute production image URLs in metadata even when reviewing on a private preview. Publishing either site still needs approval.
