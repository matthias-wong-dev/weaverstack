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

#: The renderer's folder in the fragment: a stylesheet and scripts run in name order.
RENDERER = "renderer/"
#: The Renderer measure's expression in the fragment, replaced by the renderer.
RENDERER_PLACEHOLDER = '"{renderer}"'


def dashboard_files() -> dict[str, bytes]:
    """The Dashboard's Power BI project, by project-relative path.

    The renderer is checked in as a stylesheet and scripts and reaches HTML
    Content as one DAX string literal, so the model carries it to every page.
    """

    from .fragments import DASHBOARD as FRAGMENT
    from .fragments import fragment_files

    files = fragment_files(FRAGMENT)
    sources = {
        path[len(RENDERER) :]: files.pop(path).decode("utf-8")
        for path in sorted(files)
        if path.startswith(RENDERER)
    }
    model = f"{DASHBOARD}.tmdl"
    text = files[model].decode("utf-8")
    if text.count(RENDERER_PLACEHOLDER) != 1:
        raise AssertionError(f"{model} must hold one Renderer placeholder")
    # The placeholder opens a fenced expression line; every line of the
    # literal keeps that line's indentation so TMDL reads it as one block.
    start = text.index(RENDERER_PLACEHOLDER)
    indent = text[text.rindex("\n", 0, start) + 1 : start]
    literal = renderer_literal(*_renderer(sources)).replace("\n", "\n" + indent)
    files[model] = text.replace(RENDERER_PLACEHOLDER, literal).encode("utf-8")
    return files


def renderer_html() -> str:
    """The style and script elements every page measure ends with."""

    from .fragments import DASHBOARD as FRAGMENT
    from .fragments import fragment_files

    sources = {
        path[len(RENDERER) :]: content.decode("utf-8")
        for path, content in sorted(fragment_files(FRAGMENT).items())
        if path.startswith(RENDERER)
    }
    css, script = _renderer(sources)
    _refuse_closing_tags(css, script)
    return f"<style>{_compact(css)}</style><script>{_compact(script)}</script>"


def _renderer(sources: dict[str, str]) -> tuple[str, str]:
    css = "".join(text for name, text in sources.items() if name.endswith(".css"))
    scripts = [text for name, text in sorted(sources.items()) if name.endswith(".js")]
    return css, renderer_script(scripts)


def renderer_script(scripts: list[str]) -> str:
    """The scripts, in order, inside one function so they share its scope.

    ``tests/js/renderer.js`` composes them the same way for Node.
    """

    body = "\n".join(scripts)
    return f'(function (root, module) {{\n"use strict";\n{body}\n}})(window);'


def _compact(text: str) -> str:
    # No source has a string literal spanning lines, and a line comment is
    # never written after code, so indentation, blank lines and whole-line
    # comments are layout only.
    lines = (line.strip() for line in text.splitlines())
    return "\n".join(line for line in lines if line and not line.startswith("//"))


def _refuse_closing_tags(css: str, script: str) -> None:
    # A closing tag inside either source would end its element early.
    for name, text, tag in (("CSS", css, "</style"), ("script", script, "</script")):
        if tag in text.lower():
            raise AssertionError(f"the Dashboard {name} must not contain {tag}")


def renderer_literal(css: str, script: str) -> str:
    """A DAX string literal holding the style and script elements."""

    _refuse_closing_tags(css, script)
    html = f"<style>{_compact(css)}</style><script>{_compact(script)}</script>"
    return '"' + html.replace('"', '""') + '"'


__all__ = [
    "DASHBOARD",
    "DASHBOARD_ITEMS",
    "DASHBOARD_MODEL",
    "DASHBOARD_PROJECT",
    "DASHBOARD_REPORT",
    "dashboard_files",
    "renderer_html",
    "renderer_literal",
    "renderer_script",
]
