"""Generated built-in ``Lakehouse/_weaver`` catalogue item."""

from __future__ import annotations

from dataclasses import replace

from .builtin import render_schema_file, render_source
from .item_tables import CATALOGUE_TABLES

ITEM_ROOT = "Lakehouse/_weaver"
SCHEMA_PATH = f"{ITEM_ROOT}/schemas/_.yml"


def render_item_sources() -> dict[str, str]:
    sources = {SCHEMA_PATH: render_schema_file()}
    for table in CATALOGUE_TABLES:
        documented = replace(
            table,
            columns=tuple(
                replace(
                    column,
                    description=column.description
                    or f"The catalogue value for {column.name.replace('_', ' ')}.",
                )
                for column in table.columns
            ),
        )
        sources[f"{ITEM_ROOT}/{table.qualified}.spark.sql"] = render_source(documented)
    return sources


def item_repository_files() -> dict[str, bytes]:
    return {
        path: text.encode("utf-8") for path, text in render_item_sources().items()
    }
