"""Wire encoding for a complete mutation invocation report."""

import json
from dataclasses import fields

from ..errors import BuildError
from ..mutation.executor import (
    LedgerEvent,
    MutationReport,
    MutationResult,
    Operation,
    Pending,
    TypedValue,
)
from ..mutation.models import DriverContract
from ..mutation.serialization import freeze_value, thaw_value

_TYPES = {
    c.__name__: c
    for c in (
        LedgerEvent,
        MutationReport,
        MutationResult,
        Operation,
        Pending,
        TypedValue,
        DriverContract,
    )
}
_TUPLES = {
    "ledger",
    "results",
    "operations",
    "journal_errors",
    "exclusions",
    "retained_exclusions",
}


def dumps(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def loads(data):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate receipt field")
            result[key] = value
        return result

    return json.loads(
        data,
        object_pairs_hook=pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError("nonfinite receipt value")
        ),
    )


def encode(value):
    if type(value).__name__ in _TYPES and type(value) is _TYPES[type(value).__name__]:
        return {
            "type": type(value).__name__,
            "fields": {f.name: encode(getattr(value, f.name)) for f in fields(value)},
        }
    if isinstance(value, (list, tuple)):
        return {"type": "tuple", "value": [encode(v) for v in value]}
    return {"type": "data", "value": thaw_value(freeze_value(value))}


def decode(value):
    if not isinstance(value, dict):
        raise BuildError("invalid typed receipt value")
    kind = value.get("type")
    if kind in {"tuple", "data"} and set(value) == {"type", "value"}:
        if kind == "tuple":
            if not isinstance(value["value"], list):
                raise BuildError("invalid receipt tuple")
            return tuple(decode(v) for v in value["value"])
        return freeze_value(value["value"])
    if kind not in _TYPES or set(value) != {"type", "fields"}:
        raise BuildError("invalid receipt type")
    cls = _TYPES[kind]
    content = value["fields"]
    if not isinstance(content, dict) or set(content) != {f.name for f in fields(cls)}:
        raise BuildError("invalid receipt fields")
    values = {key: decode(v) for key, v in content.items()}
    if any(key in _TUPLES and not isinstance(v, tuple) for key, v in values.items()):
        raise BuildError("invalid receipt collection")
    return cls(**values)


def encode_report(report):
    return encode(report)


def decode_report(plan, mapping, *, invocation_id=None):
    report = decode(mapping)
    if not isinstance(report, MutationReport):
        raise BuildError("invalid mutation report")
    if report.plan_id != plan.bundle_id or (
        invocation_id is not None and report.invocation_id != invocation_id
    ):
        raise BuildError("mutation report invocation differs")
    expected = [a.id for _, _, a in plan.actions()]
    if [r.action_id for r in report.results] != expected:
        raise BuildError("mutation report action inventory differs")
    statuses = {"succeeded", "failed", "uncertain", "blocked", "not_dispatched"}
    if any(r.status not in statuses for r in report.results):
        raise BuildError("invalid mutation result status")
    return report
