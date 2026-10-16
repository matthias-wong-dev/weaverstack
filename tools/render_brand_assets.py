"""Export the supplied Weaver artwork as website icons and social cards.

Requires Playwright and Chromium. Example:
    python tools/render_brand_assets.py --browser /path/to/chromium \
        --developer-site ../weaverstack-docs/docs --review-dir /path/to/review

The supplied SVG paths, gradients, proportions and wordmark are not redrawn.
"""

from __future__ import annotations

import argparse
import base64
import html
import shutil
import struct
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
BRAND = ROOT / "docs/assets/brand"
FONT = 'Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif'


def svg_url(name: str) -> str:
    return "data:image/svg+xml;base64," + base64.b64encode(
        (BRAND / name).read_bytes()
    ).decode("ascii")


def card(domain: str, *, incremental: bool = False) -> str:
    logo = svg_url("weaver-logo-light.svg")
    if incremental:
        composition = f"""
        <img class="topic-logo" src="{logo}" alt="Weaver">
        <section><h1>Incremental loads<br>in Microsoft Fabric</h1>
        <p>Tables, Folders, bookmarks and explicit deletes.</p></section>
        """
        domain += "/advanced/incremental-data-processing/"
    else:
        composition = f"""
        <img class="logo" src="{logo}" alt="Weaver">
        <h1 class="tagline">Making Fabric easier to use</h1>
        """
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
    <title>Weaver social card</title><style>
    * {{ box-sizing: border-box; }}
    html, body {{ margin: 0; width: 1200px; height: 630px; overflow: hidden; }}
    body {{ background: #f7f7f6; color: #0c1520; font-family: {FONT}; }}
    .logo {{ position: absolute; left: 300px; top: 20px; width: 600px; height: 480px; }}
    .tagline {{ position: absolute; top: 515px; left: 60px; width: 1080px;
        margin: 0; font-size: 32px; font-weight: 500; text-align: center; }}
    footer {{ position: absolute; left: 60px; bottom: 24px; width: 1080px;
        color: #4b5661; font-size: 18px; text-align: center; }}
    .topic-logo {{ position: absolute; left: 65px; top: 125px; width: 390px; height: 312px; }}
    section {{ position: absolute; left: 535px; top: 175px; width: 600px; }}
    section h1 {{ margin: 0; font-size: 54px; line-height: 1.13; letter-spacing: -.025em; }}
    section p {{ margin: 28px 0 0; color: #4b5661; font-size: 25px; line-height: 1.4; }}
    </style></head><body>{composition}<footer>{html.escape(domain)}</footer></body></html>"""


def check_png(path: Path, dimensions: tuple[int, int]) -> None:
    data = path.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", path
    assert struct.unpack(">II", data[16:24]) == dimensions, path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--browser", help="Chromium executable; otherwise use Playwright's"
    )
    parser.add_argument(
        "--developer-site", type=Path, help="Developer site's docs directory"
    )
    parser.add_argument(
        "--review-dir", type=Path, help="Optional full-size and thumbnail copies"
    )
    args = parser.parse_args()
    if args.review_dir:
        args.review_dir.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=args.browser, args=["--no-sandbox"]
        )
        context = browser.new_context(
            viewport={"width": 1200, "height": 630}, device_scale_factor=1
        )
        context.route("**/*", lambda route: route.abort())
        page = context.new_page()
        targets = [("product", ROOT / "docs/og.png", "weaverstack.dev", False)]
        if args.developer_site:
            images = args.developer_site / "assets/images"
            images.mkdir(parents=True, exist_ok=True)
            targets.extend(
                [
                    ("developer", images / "og.png", "docs.weaverstack.dev", False),
                    (
                        "incremental",
                        images / "og-incremental-loads.png",
                        "docs.weaverstack.dev",
                        True,
                    ),
                ]
            )
            shutil.copytree(BRAND, images / "brand", dirs_exist_ok=True)
        for name, output, domain, incremental in targets:
            page.set_viewport_size({"width": 1200, "height": 630})
            page.set_content(card(domain, incremental=incremental))
            page.wait_for_function(
                "[...document.images].every(i => i.complete && i.naturalWidth > 0)"
            )
            page.evaluate("document.fonts.ready")
            for selector in ("h1", "footer", "section p"):
                assert page.locator(selector).evaluate_all(
                    "es => es.every(e => {const r=e.getBoundingClientRect();return r.left>=0 && r.top>=0 && r.right<=1200 && r.bottom<=630 && e.scrollWidth<=e.clientWidth+1})"
                ), (name, selector)
            page.screenshot(path=str(output))
            check_png(output, (1200, 630))
            if args.review_dir:
                shutil.copy2(output, args.review_dir / f"{name}-og.png")
                page.evaluate(
                    "document.body.style.transform='scale(.5)';document.body.style.transformOrigin='top left'"
                )
                page.set_viewport_size({"width": 600, "height": 315})
                thumbnail = args.review_dir / f"{name}-og-thumbnail.png"
                page.screenshot(path=str(thumbnail))
                check_png(thumbnail, (600, 315))
            print(f"PASS: {output} (1200 x 630)")
        page.set_viewport_size({"width": 128, "height": 128})
        page.set_content(f"""<style>
        html,body {{ margin:0; width:128px; height:128px; background:transparent; }}
        div {{ width:128px; height:128px; display:grid; place-items:center;
            background:#f7f7f6; border-radius:24px; }}
        img {{ width:120px; height:auto; }}
        </style><div><img src="{svg_url("weaver-icon-light.svg")}" alt=""></div>""")
        page.wait_for_function(
            "document.images[0].complete && document.images[0].naturalWidth > 0"
        )
        favicon = ROOT / "docs/favicon.png"
        page.screenshot(path=str(favicon), omit_background=True)
        check_png(favicon, (128, 128))
        if args.developer_site:
            shutil.copy2(favicon, args.developer_site / "assets/images/favicon.png")
        print("PASS: 128 x 128 favicon; SVG proportions preserved")
        browser.close()


if __name__ == "__main__":
    main()
