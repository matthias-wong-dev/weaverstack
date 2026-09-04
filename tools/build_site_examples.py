#!/usr/bin/env python3
"""Write the canonical Sales example into the website's code blocks.

The site shows the project `weaver initialise --example` generates. Retyping it
would let the two drift, so the pages carry markers and this fills them from
`weaver.onboarding` itself:

    <!-- example:Lakehouse/Landing/Tables/Sales__Customer.py -->
    <pre>...</pre>
    <!-- /example -->

Run it from the repository root after changing the onboarding example, and
commit what it produces. The published pages stay static HTML.

    .venv/bin/python tools/build_site_examples.py [--check]

`--check` reports drift and writes nothing, which suits CI.
"""

from __future__ import annotations

import argparse
import html
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

#: The item names the website's walkthrough uses throughout.
LAKEHOUSE = "Landing"
WAREHOUSE = "Curated"
WORKSPACE = "My Fabric Workspace"

MARKER = re.compile(
    r"(<!-- example:(?P<path>[^\s]+) -->\s*)<pre>.*?</pre>(\s*<!-- /example -->)",
    re.DOTALL,
)


def generated() -> dict[str, str]:
    """Every file one `weaver initialise --example` run writes."""

    sys.path.insert(0, str(ROOT / "src"))
    from weaver.onboarding import example_files, project_files
    from weaver.onboarding.project import ProjectRequest

    request = ProjectRequest(
        workspace=WORKSPACE,
        catalogue="Catalogue",
        environment="Weaver",
        lakehouse=LAKEHOUSE,
        warehouse=WAREHOUSE,
        example=True,
    )
    return {**project_files(request), **example_files(request)}


def fill(page: str, files: dict[str, str]) -> tuple[str, list[str]]:
    """One page with every marked block replaced, and the paths it asked for."""

    wanted: list[str] = []

    def replace(match: re.Match[str]) -> str:
        path = match.group("path")
        wanted.append(path)
        if path not in files:
            raise SystemExit(f"{path} is not a file the example generates")
        body = html.escape(files[path].rstrip("\n"))
        return f"{match.group(1)}<pre>{body}</pre>{match.group(3)}"

    return MARKER.sub(replace, page), wanted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="report drift and write nothing"
    )
    args = parser.parse_args()

    files = generated()
    drifted: list[str] = []
    filled = 0

    for page in sorted(DOCS.rglob("index.html")):
        before = page.read_text(encoding="utf-8")
        after, wanted = fill(before, files)
        filled += len(wanted)
        if after == before:
            continue
        if args.check:
            drifted.append(str(page.relative_to(ROOT)))
        else:
            page.write_text(after, encoding="utf-8")
            print(f"updated {page.relative_to(ROOT)}")

    if args.check and drifted:
        print("example blocks are out of date in:", ", ".join(drifted))
        return 1
    print(f"{filled} example blocks across the site")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
