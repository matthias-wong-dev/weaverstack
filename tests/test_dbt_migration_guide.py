"""The dbt migration guide is generated from its copyable Markdown source."""

from __future__ import annotations

import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path

from support.weaver_test import weaver_test

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "docs/vs-dbt/index.html"
SOURCE = ROOT / "docs/vs-dbt/migration-agent-brief.md"
GENERATOR = ROOT / "tools/build_dbt_migration_guide.py"


class GuideHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.guide_depth = 0
        self.headings: list[str] = []
        self.links: list[tuple[str, str]] = []
        self.code: list[str] = []
        self._heading: list[str] | None = None
        self._link: tuple[str, list[str]] | None = None
        self._code: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "article" and "migration-guide" in (attributes.get("class") or "").split():
            self.guide_depth = 1
            return
        if not self.guide_depth:
            return
        self.guide_depth += 1
        if tag in {"h3", "h4"}:
            self._heading = []
        elif tag == "a":
            self._link = (attributes.get("href") or "", [])
        elif tag == "code":
            self._code = []

    def handle_endtag(self, tag):
        if not self.guide_depth:
            return
        if tag in {"h3", "h4"} and self._heading is not None:
            self.headings.append("".join(self._heading))
            self._heading = None
        elif tag == "a" and self._link is not None:
            href, text = self._link
            self.links.append(("".join(text), href))
            self._link = None
        elif tag == "code" and self._code is not None:
            self.code.append("".join(self._code))
            self._code = None
        self.guide_depth -= 1

    def handle_data(self, data):
        if self._heading is not None:
            self._heading.append(data)
        if self._link is not None:
            self._link[1].append(data)
        if self._code is not None:
            self._code.append(data)


class PageControls(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.copy_button: dict[str, str | None] | None = None
        self.status: dict[str, str | None] | None = None
        self.scripts: list[str] = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "button" and "data-copy-markdown" in attributes:
            self.copy_button = attributes
        elif tag == "p" and attributes.get("id") == "migration-copy-status":
            self.status = attributes
        elif tag == "script" and attributes.get("src"):
            self.scripts.append(attributes["src"] or "")


@weaver_test()
def test_generated_guide_matches_markdown_source():
    result = subprocess.run(
        [sys.executable, str(GENERATOR), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    markdown = SOURCE.read_text()
    assert "dbt parse\n```" in markdown
    assert "--no-partial-parse" not in markdown
    assert "target/manifest.json" in markdown
    assert "weaver check" in markdown

    parser = GuideHTML()
    parser.feed(PAGE.read_text())
    assert "Migration contract" in parser.headings
    assert "2. Confirm migration decisions with the user" in parser.headings
    assert "A. Source environment and scope" in parser.headings
    assert "G. Validation and deployment" in parser.headings
    assert "dbt parse" in parser.code
    assert (
        "Build, Load and Test",
        "https://docs.weaverstack.dev/core-concepts/build-load-and-test/",
    ) in parser.links


@weaver_test()
def test_copy_button_reads_the_original_markdown_source():
    parser = PageControls()
    parser.feed(PAGE.read_text())

    assert parser.copy_button is not None
    assert parser.copy_button["data-copy-markdown"] == "migration-agent-brief.md"
    assert parser.copy_button["aria-describedby"] == "migration-copy-status"
    assert "disabled" in parser.copy_button
    assert parser.status is not None
    assert parser.status["role"] == "status"
    assert parser.status["aria-live"] == "polite"
    assert "migration-brief.js" in parser.scripts

    script = (ROOT / "docs/vs-dbt/migration-brief.js").read_text()
    assert "response.text()" in script
    assert "clipboard.writeText(markdown)" in script
    assert "innerText" not in script
