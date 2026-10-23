#!/usr/bin/env python3
"""Render the dbt migration Markdown into the checked-in product page."""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs/vs-dbt/migration-agent-brief.md"
PAGE = ROOT / "docs/vs-dbt/index.html"
START = "<!-- generated:dbt-migration-guide:start -->"
END = "<!-- generated:dbt-migration-guide:end -->"
BLOCK_START = re.compile(r"^(#{1,3}) (.+)$")
INLINE = re.compile(
    r"(`[^`]+`|\*\*[^*]+\*\*|\[[^\]]+\]\(https://[^)]+\))"
)
LINK = re.compile(r"^\[([^\]]+)\]\((https://[^)]+)\)$")


def inline(text: str) -> str:
    """Render the small inline Markdown surface used by the migration brief."""

    rendered: list[str] = []
    position = 0
    for token in INLINE.finditer(text):
        rendered.append(html.escape(text[position : token.start()]))
        value = token.group(0)
        link = LINK.match(value)
        if link:
            rendered.append(
                f'<a href="{html.escape(link.group(2), quote=True)}">'
                f"{html.escape(link.group(1))}</a>"
            )
        elif value.startswith("`"):
            rendered.append(f"<code>{html.escape(value[1:-1])}</code>")
        else:
            rendered.append(f"<strong>{html.escape(value[2:-2])}</strong>")
        position = token.end()
    rendered.append(html.escape(text[position:]))
    return "".join(rendered)


def render(markdown: str) -> str:
    """Render the constrained, dependency-free Markdown document."""

    lines = markdown.splitlines()
    output = ['<article class="migration-guide">']
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line:
            index += 1
            continue

        if line.startswith("```"):
            language = line[3:].strip()
            index += 1
            code: list[str] = []
            while index < len(lines) and not lines[index].startswith("```"):
                code.append(lines[index])
                index += 1
            if index == len(lines):
                raise ValueError("Unclosed fenced code block")
            index += 1
            label = "terminal" if language == "console" else language or "code"
            output.extend(
                [
                    '<div class="code plain">',
                    f'  <div class="code-bar"><span>{html.escape(label)}</span></div>',
                    f"  <pre><code>{html.escape(chr(10).join(code))}</code></pre>",
                    "</div>",
                ]
            )
            continue

        heading = BLOCK_START.match(line)
        if heading:
            level = "h3" if len(heading.group(1)) <= 2 else "h4"
            output.append(f"<{level}>{inline(heading.group(2))}</{level}>")
            index += 1
            continue

        if line.startswith("- "):
            output.append("<ul>")
            while index < len(lines) and lines[index].startswith("- "):
                output.append(f"  <li>{inline(lines[index][2:])}</li>")
                index += 1
            output.append("</ul>")
            continue

        paragraph = [line]
        index += 1
        while index < len(lines):
            next_line = lines[index]
            if (
                not next_line
                or next_line.startswith("```")
                or next_line.startswith("- ")
                or BLOCK_START.match(next_line)
            ):
                break
            paragraph.append(next_line)
            index += 1
        output.append(f"<p>{inline(' '.join(paragraph))}</p>")

    output.append("</article>")
    return "\n".join(output)


def generated_region() -> str:
    body = render(SOURCE.read_text())
    indented = "\n".join(f"          {line}" for line in body.splitlines())
    return f"{START}\n{indented}\n          {END}"


def update_page(page: str) -> str:
    if page.count(START) != 1 or page.count(END) != 1:
        raise ValueError("Expected one dbt migration guide marker pair")
    before, rest = page.split(START, 1)
    _, after = rest.split(END, 1)
    return before + generated_region() + after


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail when the checked-in HTML differs from the Markdown source.",
    )
    args = parser.parse_args()

    current = PAGE.read_text()
    expected = update_page(current)
    if args.check:
        if current != expected:
            print("dbt migration guide is stale; run tools/build_dbt_migration_guide.py")
            return 1
        print("dbt migration guide matches its Markdown source")
        return 0

    PAGE.write_text(expected)
    print(f"rendered {SOURCE.relative_to(ROOT)} into {PAGE.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
