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


__all__ = [
    "DASHBOARD",
    "DASHBOARD_ITEMS",
    "DASHBOARD_MODEL",
    "DASHBOARD_PROJECT",
    "DASHBOARD_REPORT",
]
