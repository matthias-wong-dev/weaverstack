"""Render the complete SQL statements carried by build actions."""

from __future__ import annotations

from pathlib import Path
from string import Template

_TEMPLATES = Path(__file__).resolve().parent / "templates"


def render_sql_statement(dialect: str, name: str, **values: object) -> str:
    template = (_TEMPLATES / dialect / f"{name}.sql").read_text(encoding="utf-8")
    return Template(template).substitute(
        {key: str(value) for key, value in values.items()}
    )


def tsql_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"
