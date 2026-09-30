"""Delta table creation shared by Session hosts."""

from __future__ import annotations

import json
from importlib import import_module
from typing import Any, Sequence

from ..delta_protocol import ProtocolMinima, SparkDeltaAction, resolve_protocol_minima
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
    protocol_minima: ProtocolMinima | None = None,
):
    """Create one strict Delta table against an active Spark session."""

    minima = resolve_protocol_minima(protocol_minima)
    delta_tables = import_module("delta.tables")
    DeltaTable = delta_tables.DeltaTable

    identity_generator = None
    if identity_column is not None:
        try:
            identity_generator = delta_tables.IdentityGenerator
        except AttributeError as exc:
            raise CommandError(_identity_error(qualified_name)) from exc

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
        builder = builder.property(
            "delta.minReaderVersion", str(minima["minReaderVersion"])
        ).property("delta.minWriterVersion", str(minima["minWriterVersion"]))
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
    protocol_minima: ProtocolMinima | None = None,
) -> str:
    """Return a self-contained program for the desktop Session's Livy crossing."""

    specification = json.dumps(
        {
            "object": qualified_name,
            "columns": [list(column) for column in columns],
            "identity_column": identity_column,
            "column_mapping": bool(column_mapping),
            "protocol_minima": resolve_protocol_minima(protocol_minima),
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
        f"_case_key = {_CASE_SENSITIVE!r}\n"
        "_previous = spark.conf.get(_case_key)\n"
        "_restore = str(_previous).lower() != 'true'\n"
        "if _restore:\n"
        "    spark.conf.set(_case_key, 'true')\n"
        "try:\n"
        "    _builder = _DeltaTable.create(spark).tableName(_spec['object'])\n"
        "    for _name, _type, _not_null in _spec['columns']:\n"
        "        _options = {'nullable': not bool(_not_null)}\n"
        "        if _name == _spec['identity_column']:\n"
        "            _options['generatedAlwaysAs'] = _identity_factory()\n"
        "        _builder = _builder.addColumn(_name, _type, **_options)\n"
        "    for _key, _value in _spec['protocol_minima'].items():\n"
        "        _builder = _builder.property('delta.' + _key, str(_value))\n"
        "    if _spec['column_mapping']:\n"
        "        _builder = _builder.property('delta.columnMapping.mode', 'name')\n"
        "    _builder.execute()\n"
        "finally:\n"
        "    if _restore:\n"
        "        spark.conf.set(_case_key, _previous)\n"
        "emit({'object': _spec['object'], 'created': True})\n"
    )


def remote_delta_table_actions_program(actions: Sequence[SparkDeltaAction]) -> str:
    """Run labelled TableBuilder creates in order with one outcome per Table."""
    specification = json.dumps(
        [
            {
                "label": label,
                "object": qualified,
                "columns": [list(column) for column in columns],
                "identity_column": identity,
                "column_mapping": bool(mapping),
                "protocol_minima": resolve_protocol_minima(
                    policy[0] if policy else None
                ),
            }
            for label, qualified, columns, identity, mapping, *policy in actions
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return (
        "import importlib as _importlib\n"
        "import json as _json\n"
        "import time as _time\n"
        "_delta_tables = _importlib.import_module('delta.tables')\n"
        "_DeltaTable = _delta_tables.DeltaTable\n"
        f"_specs = _json.loads({specification!r})\n"
        f"_case_key = {_CASE_SENSITIVE!r}\n"
        "_previous = spark.conf.get(_case_key)\n"
        "_restore = str(_previous).lower() != 'true'\n"
        "_results = []\n"
        "_origin = _time.monotonic()\n"
        "if _restore:\n"
        "    spark.conf.set(_case_key, 'true')\n"
        "try:\n"
        "    for _spec in _specs:\n"
        "        _started = _time.monotonic()\n"
        "        try:\n"
        "            _identity_factory = None\n"
        "            if _spec['identity_column'] is not None:\n"
        "                try:\n"
        "                    _identity_factory = _delta_tables.IdentityGenerator\n"
        "                except AttributeError as _exc:\n"
        "                    raise RuntimeError(\n"
        "                        f\"{_spec['object']}: Lakehouse identity requires \"\n"
        '                        "delta.tables.IdentityGenerator on this Fabric runtime"\n'
        "                    ) from _exc\n"
        "            _builder = _DeltaTable.create(spark).tableName(_spec['object'])\n"
        "            for _name, _type, _not_null in _spec['columns']:\n"
        "                _options = {'nullable': not bool(_not_null)}\n"
        "                if _name == _spec['identity_column']:\n"
        "                    _options['generatedAlwaysAs'] = _identity_factory()\n"
        "                _builder = _builder.addColumn(_name, _type, **_options)\n"
        "            for _key, _value in _spec['protocol_minima'].items():\n"
        "                _builder = _builder.property('delta.' + _key, str(_value))\n"
        "            if _spec['column_mapping']:\n"
        "                _builder = _builder.property('delta.columnMapping.mode', 'name')\n"
        "            _builder.execute()\n"
        "        except Exception as _error:\n"
        "            _outcome = {\n"
        "                'label': _spec['label'], 'succeeded': False,\n"
        "                'error_type': type(_error).__name__,\n"
        "                'error_message': str(_error),\n"
        "            }\n"
        "        else:\n"
        "            _outcome = {'label': _spec['label'], 'succeeded': True}\n"
        "        _outcome['started_after_seconds'] = _started - _origin\n"
        "        _outcome['duration_seconds'] = _time.monotonic() - _started\n"
        "        _results.append(_outcome)\n"
        "finally:\n"
        "    if _restore:\n"
        "        spark.conf.set(_case_key, _previous)\n"
        "emit(_results)\n"
    )


__all__ = [
    "create_delta_table_in_session",
    "remote_delta_table_program",
    "remote_delta_table_actions_program",
]
