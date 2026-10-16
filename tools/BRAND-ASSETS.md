# Approved Weaver PNG identity

Use the PNG-derived assets approved in review, not the rejected SVG kit. The source reference SHA-256 is `bc3403b7f86b17d3c5ed6f28af1afa1cd9ed0518c0ca5f36285c3767d6473062`.

`docs/assets/brand/` retains the native transparent full logos and compact W marks for light and dark backgrounds. Transparency was extracted from the raster reference; it is not native vector artwork. The `*-134.png` marks are 134 × 77 web derivatives of the approved 670 × 385 crops, downsampled with Pillow LANCZOS at unchanged aspect ratio.

Headers and footers display the compact mark at **30 CSS pixels wide**, with proportional height, a 6-pixel gap to the name, and an explicit maximum width. The dark comparison heading uses a 28-pixel dark variant. Do not place the large stacked logo in the navigation or let intrinsic image width determine header size. Material's header keeps the mark and title together in one link; its mobile drawer uses the same selected-theme artwork.

The default `og.png` and comparison `og-dbt.png` are exact copies of the approved light 1200 × 630 card, without rerendering, new wording or changed spacing. The approved dark card is retained under `assets/brand/`. Developer docs use the same approved light card, including the retained topic-specific OG URL. Each page's existing social title/description is preserved; image alt text describes the actual logo card.

`favicon.png`, `favicon.ico`, `apple-touch-icon.png`, and the 192/512 manifest icons come from the approved asset bundle. Keep manifest dimensions aligned with the files.

Both repositories contain checked-in publication image bytes. No image-processing library, browser renderer or runtime visualisation library is added to either site. Publication still requires approval; review-branch pushes and private previews are not deployment.
