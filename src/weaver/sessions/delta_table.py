"""Delta table creation shared by Session hosts."""

from __future__ import annotations

import json
from importlib import import_module
from typing import Any, Sequence

from ..errors import CommandError

_CASE_SENSITIVE = "spark.sql.caseSensitive"


def _identity_error(qualified_name: str) -> str:
    return (
        f"{qualified_name}: Lakehouse identity requires "
        "delta.tables.IdentityGenerator on this Fabric runtime"
    )


def create_delta_table_in_session(
    spark,
    qualified_name: str,
    columns: Sequence[Sequence[Any]],
    *,
    identity_column: str | None,
    column_mapping: bool,
    validate_only: bool,
):
    """Create one strict Delta table against an active Spark session."""

    delta_tables = import_module("delta.tables")
    DeltaTable = delta_tables.DeltaTable

    identity_generator = None
    if identity_column is not None:
        try:
            identity_generator = delta_tables.IdentityGenerator
        except AttributeError as exc:
            raise CommandError(_identity_error(qualified_name)) from exc

    if validate_only:
        return {"object": qualified_name, "identity_supported": True}

    previous = spark.conf.get(_CASE_SENSITIVE)
    restore = str(previous).lower() != "true"
    if restore:
        spark.conf.set(_CASE_SENSITIVE, "true")
    try:
        builder = DeltaTable.create(spark).tableName(qualified_name)
        for name, type_, not_null in columns:
            options = {"nullable": not bool(not_null)}
            if name == identity_column:
                assert identity_generator is not None
                options["generatedAlwaysAs"] = identity_generator()
            builder = builder.addColumn(name, type_, **options)
        if column_mapping:
            builder = builder.property("delta.columnMapping.mode", "name")
        return builder.execute()
    finally:
        if restore:
            spark.conf.set(_CASE_SENSITIVE, previous)


def remote_delta_table_program(
    qualified_name: str,
    columns: Sequence[Sequence[Any]],
    *,
    identity_column: str | None,
    column_mapping: bool,
    validate_only: bool,
) -> str:
    """Return a self-contained program for the desktop Session's Livy crossing."""

    specification = json.dumps(
        {
            "object": qualified_name,
            "columns": [list(column) for column in columns],
            "identity_column": identity_column,
            "column_mapping": bool(column_mapping),
            "validate_only": bool(validate_only),
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return (
        "import importlib as _importlib\n"
        "import json as _json\n"
        "_delta_tables = _importlib.import_module('delta.tables')\n"
        "_DeltaTable = _delta_tables.DeltaTable\n"
        f"_spec = _json.loads({specification!r})\n"
        "_identity_factory = None\n"
        "if _spec['identity_column'] is not None:\n"
        "    try:\n"
        "        _IdentityGenerator = _delta_tables.IdentityGenerator\n"
        "    except AttributeError as _exc:\n"
        "        raise RuntimeError(\n"
        "            f\"{_spec['object']}: Lakehouse identity requires \"\n"
        '            "delta.tables.IdentityGenerator on this Fabric runtime"\n'
        "        ) from _exc\n"
        "    _identity_factory = _IdentityGenerator\n"
        "if _spec['validate_only']:\n"
        "    emit({'object': _spec['object'], 'identity_supported': True})\n"
        "else:\n"
        f"    _case_key = {_CASE_SENSITIVE!r}\n"
        "    _previous = spark.conf.get(_case_key)\n"
        "    _restore = str(_previous).lower() != 'true'\n"
        "    if _restore:\n"
        "        spark.conf.set(_case_key, 'true')\n"
        "    try:\n"
        "        _builder = _DeltaTable.create(spark).tableName(_spec['object'])\n"
        "        for _name, _type, _not_null in _spec['columns']:\n"
        "            _options = {'nullable': not bool(_not_null)}\n"
        "            if _name == _spec['identity_column']:\n"
        "                _options['generatedAlwaysAs'] = _identity_factory()\n"
        "            _builder = _builder.addColumn(_name, _type, **_options)\n"
        "        if _spec['column_mapping']:\n"
        "            _builder = _builder.property('delta.columnMapping.mode', 'name')\n"
        "        _builder.execute()\n"
        "    finally:\n"
        "        if _restore:\n"
        "            spark.conf.set(_case_key, _previous)\n"
        "    emit({'object': _spec['object'], 'created': True})\n"
    )


__all__ = ["create_delta_table_in_session", "remote_delta_table_program"]
