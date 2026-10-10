"""The built-in Catalogue Browser: a semantic model and Report over the catalogue.

``catalogue_browser:`` in workspace configuration names the Fabric item both
deploy to. Build composes them from the ``browser`` fragment as one Power BI
project, so they build, read back and publish like an authored project.
"""

from __future__ import annotations

from .declaration.model import REPORT, SEMANTIC_MODEL, WeaverItemId

#: The logical name of the Browser's model, Report and Power BI project.
BROWSER = "Catalogue Browser"
BROWSER_MODEL = WeaverItemId(SEMANTIC_MODEL, BROWSER)
BROWSER_REPORT = WeaverItemId(REPORT, BROWSER)
BROWSER_ITEMS = (BROWSER_MODEL, BROWSER_REPORT)
#: The Browser's Power BI project folder, as an authored one would be named.
BROWSER_PROJECT = f"PowerBI/{BROWSER}"


__all__ = [
    "BROWSER",
    "BROWSER_ITEMS",
    "BROWSER_MODEL",
    "BROWSER_PROJECT",
    "BROWSER_REPORT",
]
