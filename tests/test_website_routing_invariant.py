"""The static website navigates through GitHub Pages clean routes."""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

from support.weaver_test import weaver_test

ROOT = Path(__file__).parents[1]
SITE = ROOT / "docs"


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value is not None:
                self.hrefs.append(value)


def _links(page: Path) -> tuple[str, ...]:
    parser = _Links()
    parser.feed(page.read_text(encoding="utf-8"))
    return tuple(parser.hrefs)


@weaver_test()
def test_internal_navigation_uses_clean_routes():
    offenders = [
        (str(page.relative_to(SITE)), href)
        for page in sorted(SITE.rglob("index.html"))
        for href in _links(page)
        if not urlparse(href).scheme and urlparse(href).path.endswith("index.html")
    ]

    assert offenders == []
