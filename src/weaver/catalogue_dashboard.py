"""The built-in Catalogue Dashboard: a semantic model and Report over the catalogue.

``catalogue_dashboard:`` in workspace configuration names the Fabric item both
deploy to. Build composes them from the ``dashboard`` fragment as one Power BI
project, so they build, read back and publish like an authored project.
"""

from __future__ import annotations

from .declaration.model import REPORT, SEMANTIC_MODEL, WeaverItemId

#: The logical name of the Dashboard's model, Report and Power BI project.
DASHBOARD = "Catalogue Dashboard"
DASHBOARD_MODEL = WeaverItemId(SEMANTIC_MODEL, DASHBOARD)
DASHBOARD_REPORT = WeaverItemId(REPORT, DASHBOARD)
DASHBOARD_ITEMS = (DASHBOARD_MODEL, DASHBOARD_REPORT)
#: The Dashboard's Power BI project folder, as an authored one would be named.
DASHBOARD_PROJECT = f"PowerBI/{DASHBOARD}"

#: The renderer's sources in the fragment, inlined into the model's Renderer measure.
RENDERER_SOURCES = ("dashboard.css", "dashboard.js")
#: The Renderer measure's expression in the fragment, replaced by the renderer.
RENDERER_PLACEHOLDER = '"{renderer}"'


def dashboard_files() -> dict[str, bytes]:
    """The Dashboard's Power BI project, by project-relative path.

    The renderer is checked in as a stylesheet and a script and reaches HTML
    Content as one DAX string literal, so the model carries it to every page.
    """

    from .fragments import DASHBOARD as FRAGMENT
    from .fragments import fragment_files

    files = fragment_files(FRAGMENT)
    css, script = (files.pop(name).decode("utf-8") for name in RENDERER_SOURCES)
    model = f"{DASHBOARD}.tmdl"
    text = files[model].decode("utf-8")
    if text.count(RENDERER_PLACEHOLDER) != 1:
        raise AssertionError(f"{model} must hold one Renderer placeholder")
    # The placeholder opens a fenced expression line; every line of the
    # literal keeps that line's indentation so TMDL reads it as one block.
    start = text.index(RENDERER_PLACEHOLDER)
    indent = text[text.rindex("\n", 0, start) + 1 : start]
    literal = renderer_literal(css, script).replace("\n", "\n" + indent)
    files[model] = text.replace(RENDERER_PLACEHOLDER, literal).encode("utf-8")
    return files


def _compact(text: str) -> str:
    # Neither source has a string literal spanning lines, so indentation and
    # blank lines are layout only.
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def renderer_literal(css: str, script: str) -> str:
    """A DAX string literal holding the style and script elements."""

    # A closing tag inside either source would end its element early.
    for name, text, tag in (("CSS", css, "</style"), ("script", script, "</script")):
        if tag in text.lower():
            raise AssertionError(f"the Dashboard {name} must not contain {tag}")
    html = f"<style>{_compact(css)}</style><script>{_compact(script)}</script>"
    return '"' + html.replace('"', '""') + '"'


__all__ = [
    "DASHBOARD",
    "DASHBOARD_ITEMS",
    "DASHBOARD_MODEL",
    "DASHBOARD_PROJECT",
    "DASHBOARD_REPORT",
    "dashboard_files",
    "renderer_literal",
]
