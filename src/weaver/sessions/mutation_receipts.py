"""Strict wire values and bounded, hash-linked durable checkpoints."""

import hashlib
import json
from dataclasses import fields, replace

from ..errors import BuildError
from ..mutation.executor import (
    LedgerEvent,
    MutationReport,
    MutationResult,
    Operation,
    Pending,
    TypedValue,
)
from ..mutation.fragments import PrerequisiteReceipt, validate_fragment
from ..mutation.models import DriverContract
from ..mutation.recovery import recover
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
        PrerequisiteReceipt,
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
    "selected",
    "prerequisites",
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


def checked_request(plan, request):
    from datetime import datetime
    from math import isfinite

    from ..mutation.models import PhysicalScope
    from .install_archive import ArchiveStaging, select_staging

    if not isinstance(request, dict) or set(request) != {
        "plan_id",
        "invocation_id",
        "selected",
        "prerequisites",
        "staging",
        "build_datetime",
        "timeout",
    }:
        raise BuildError("invalid mutation fragment request")
    if (
        request["plan_id"] != plan.bundle_id
        or not isinstance(request["invocation_id"], str)
        or not request["invocation_id"]
    ):
        raise BuildError("fragment invocation identity differs")
    if not isinstance(request["prerequisites"], list):
        raise BuildError("invalid fragment prerequisites")
    prerequisites = tuple(decode(r) for r in request["prerequisites"])
    validate_fragment(plan, request["selected"], prerequisites)
    staging = request["staging"]
    stage = (
        ArchiveStaging.from_mapping(staging)
        if isinstance(staging, dict) and "target" in staging
        else PhysicalScope.from_mapping(staging)
    )
    if select_staging(plan, (stage,)) is None:
        raise BuildError("fragment staging overlaps mutation or protected source")
    timeout = request["timeout"]
    if type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0:
        raise BuildError("invalid fragment timeout")
    instant = request["build_datetime"]
    if instant is not None:
        if not isinstance(instant, str):
            raise BuildError("invalid publication instant")
        try:
            datetime.strptime(instant, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError as error:
            raise BuildError("invalid publication instant") from error
    return prerequisites


def encode_report(report):
    return encode(report)


def decode_report(plan, mapping, *, invocation_id, selected=None, prerequisites=()):
    report = decode(mapping)
    chosen = (
        tuple(a.id for _, _, a in plan.actions())
        if selected is None
        else tuple(selected)
    )

    if (
        not isinstance(report, MutationReport)
        or report.plan_id != plan.bundle_id
        or report.invocation_id != invocation_id
    ):
        raise BuildError("mutation report identity differs")
    if any(not isinstance(e, str) or not e for e in report.journal_errors):
        raise BuildError("invalid journal diagnostic")
    if any(
        isinstance(e, LedgerEvent)
        and e.kind == "admission_refused"
        and e.value not in report.journal_errors
        for e in report.ledger
    ):
        raise BuildError("refused admission lacks journal diagnostic")
    checked = recover(
        plan,
        report.ledger,
        invocation_id=invocation_id,
        selected=chosen,
        prerequisites=prerequisites,
    )
    if report.results != checked.results or report.operations != checked.operations:
        raise BuildError("mutation report differs from durable evidence")
    actions = {a.id: a for _, _, a in plan.actions()}
    for exclusion, owner in report.retained_exclusions:
        if (
            owner not in chosen
            or exclusion not in actions[owner].exclusions
            or report.by_id[owner].status not in {"succeeded", "uncertain"}
        ):
            raise BuildError("invalid retained exclusion")
    return replace(report, results=checked.results)


class DurableJournal:
    def __init__(self, write, output, plan_id, invocation_id, *, context=None):
        self.write, self.output = write, output
        self.plan_id, self.invocation_id = plan_id, invocation_id
        self.context = dict(context or {})
        self.previous = None
        self.number = 0
        self.publish()

    def publish(self):
        self.write(
            self.output,
            dumps(
                self.context
                | {
                    "status": "running",
                    "plan_id": self.plan_id,
                    "invocation_id": self.invocation_id,
                    "journal": self.previous,
                }
            ),
        )

    def __call__(self, events):
        chunk = dumps(
            {
                "plan_id": self.plan_id,
                "invocation_id": self.invocation_id,
                "previous": self.previous,
                "events": [encode(e) for e in events],
            }
        )
        path = self.output + f".checkpoint-{self.number:08d}.json"
        reference = {
            "path": path,
            "bytes": len(chunk),
            "sha256": hashlib.sha256(chunk).hexdigest(),
        }
        self.write(path, chunk)
        self.previous = reference
        self.number += 1
        self.publish()


def read_journal(head, read, output):
    from .install_archive import read_receipt

    reference = head.get("journal")
    chunks, seen = [], set()
    while reference is not None:
        if not isinstance(reference, dict) or set(reference) != {
            "path",
            "bytes",
            "sha256",
        }:
            raise BuildError("invalid checkpoint reference")
        path = reference["path"]
        if (
            not isinstance(path, str)
            or not path.startswith(output + ".checkpoint-")
            or not path.endswith(".json")
            or not path[len(output + ".checkpoint-") : -5].isdigit()
            or path in seen
            or len(seen) >= 100000
        ):
            raise BuildError("unsafe or cyclic checkpoint reference")
        seen.add(path)
        chunk = read_receipt(reference, read(path))
        if (
            set(chunk) != {"plan_id", "invocation_id", "previous", "events"}
            or chunk["plan_id"] != head["plan_id"]
            or chunk["invocation_id"] != head["invocation_id"]
            or not isinstance(chunk["events"], list)
            or not 1 <= len(chunk["events"]) <= 64
        ):
            raise BuildError("checkpoint invocation differs")
        chunks.append(tuple(decode(e) for e in chunk["events"]))
        reference = chunk["previous"]
    return tuple(e for chunk in reversed(chunks) for e in chunk)
