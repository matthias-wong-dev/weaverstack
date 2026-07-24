"""Writing, loading and validating a build bundle on a store.

A bundle is a directory:

.. code-block:: text

    <bundle>/
        plan.yml                     the canonical manifest
        repository/                  the certified repository snapshot
            ...
        payload/                     one generated definition per action
            010-create-schemas/
                create-DWG.spark.sql
            ...

The manifest is written **last**, so a half-written directory never looks
installable. Loading validates the whole bundle — structure, target bindings,
payload presence, and payload hashes — before any action can run, because the
installer must be able to trust what it is handed without re-reading the source.

``bundle_id`` is derived from stable inputs only: the format version, the
repository signature, the target descriptors and the canonical manifest with
its payload hashes. No timestamp participates, so the same inputs always yield
the same identity.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

import yaml

from ..errors import BuildError
from ..locations import Location
from ..store import Store
from .models import OMISSION_REASONS, BuildPlan

#: The only bundle format this code writes and accepts.
SUPPORTED_FORMAT_VERSION = 1

PLAN_FILENAME = "plan.yml"
REPOSITORY_DIR = "repository"
PAYLOAD_DIR = "payload"

PYTHON_EXECUTOR = "python"
SPARK_SQL_EXECUTOR = "spark_sql"
#: Executors a bundle may carry in v1. T-SQL is deliberately not here: a bundle
#: that reached generation with Warehouse work would already have raised.
VALID_EXECUTORS = frozenset({PYTHON_EXECUTOR, SPARK_SQL_EXECUTOR})
_EXECUTOR_EXTENSION = {PYTHON_EXECUTOR: ".py", SPARK_SQL_EXECUTOR: ".spark.sql"}


@dataclass(frozen=True)
class BuildBundle:
    """A validated bundle on a store: its root location and loaded plan."""

    location: Location
    plan: BuildPlan

    @property
    def bundle_id(self) -> str:
        return self.plan.bundle_id


# --- canonical form and identity --------------------------------------------


def _canonical_bytes(mapping) -> bytes:
    """A byte form that is identical for equal manifests on any platform."""

    return json.dumps(
        mapping, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def compute_bundle_id(plan: BuildPlan) -> str:
    """The identity of a plan, independent of its stored ``bundle_id`` field.

    The field is blanked before hashing so a plan's id never depends on itself,
    and everything else — signature, targets, sequences, payload hashes — feeds
    in through the canonical mapping.
    """

    mapping = plan.to_mapping()
    mapping["bundle_id"] = ""
    return hashlib.sha256(_canonical_bytes(mapping)).hexdigest()


def plan_to_yaml(plan: BuildPlan) -> str:
    """The human-readable canonical manifest."""

    return yaml.safe_dump(
        plan.to_mapping(), sort_keys=False, default_flow_style=False, allow_unicode=True
    )


def plan_from_yaml(text: str) -> BuildPlan:
    loaded = yaml.safe_load(text)
    if not isinstance(loaded, dict):
        raise BuildError("plan.yml must be a mapping")
    try:
        return BuildPlan.from_mapping(loaded)
    except KeyError as exc:
        raise BuildError(f"plan.yml is missing a required field: {exc}") from exc


# --- writing -----------------------------------------------------------------


def write_bundle(
    location: Location,
    *,
    plan: BuildPlan,
    payloads: Mapping[str, bytes],
    snapshot: Mapping[str, bytes],
    store: Store,
) -> BuildBundle:
    """Write a bundle, manifest last, then reload and validate it.

    ``payloads`` is keyed by each action's bundle-relative payload path;
    ``snapshot`` by repository-relative path (written beneath ``repository/``).
    """

    for _, _, action in plan.actions():
        if action.payload is None:
            continue
        _check_payload_path(action.payload)
        if action.payload not in payloads:
            raise BuildError(
                f"action {action.id!r} references payload {action.payload!r} "
                "but no payload was supplied for it"
            )
        digest = hashlib.sha256(payloads[action.payload]).hexdigest()
        if action.payload_sha256 != digest:
            raise BuildError(
                f"action {action.id!r} payload hash does not match its content "
                f"({action.payload_sha256} vs {digest})"
            )

    for relative, data in snapshot.items():
        _check_relative(relative, what="snapshot path")
        store.write(location.join(REPOSITORY_DIR, *relative.split("/")), data)

    for relative, data in payloads.items():
        store.write(location.join(*relative.split("/")), data)

    # The manifest goes last: until it exists, the directory is not a bundle.
    store.write(location.join(PLAN_FILENAME), plan_to_yaml(plan).encode("utf-8"))

    return load_bundle(location, store=store)


# --- loading and validation --------------------------------------------------


def load_bundle(location: Location, *, store: Store) -> BuildBundle:
    """Read a bundle and fully validate it before returning."""

    plan_location = location.join(PLAN_FILENAME)
    if not store.exists(plan_location):
        raise BuildError(f"no bundle manifest at {plan_location.value}")

    plan = plan_from_yaml(store.read(plan_location).decode("utf-8"))
    validate_bundle(location, plan, store=store)
    return BuildBundle(location=location, plan=plan)


def validate_bundle(location: Location, plan: BuildPlan, *, store: Store) -> None:
    """Reject any structural or integrity fault before an action runs.

    Structure is checked first and needs no store, so a malformed manifest is
    caught the same way whether or not its payloads happen to exist; payload
    presence and hashes are checked second, against the store.
    """

    validate_plan_structure(plan)
    _validate_payload_integrity(location, plan, store)


def validate_plan_structure(plan: BuildPlan) -> None:
    """Everything provable from the manifest alone, without reading payloads."""

    if plan.format_version != SUPPORTED_FORMAT_VERSION:
        raise BuildError(
            f"unsupported bundle format version {plan.format_version} "
            f"(this build supports {SUPPORTED_FORMAT_VERSION})"
        )

    for node in plan.omitted_nodes:
        if node.reason not in OMISSION_REASONS:
            raise BuildError(f"omitted node {node.node_id!r} has unknown reason {node.reason!r}")
    omitted_ids = {node.node_id for node in plan.omitted_nodes}

    target_ids = plan.target_ids
    if len(target_ids) != len(plan.targets):
        raise BuildError("duplicate target id in plan")

    seen_numbers: list[int] = []
    batch_ids: set[str] = set()
    action_ids: set[str] = set()

    for sequence in plan.sequences:
        seen_numbers.append(sequence.number)
        for batch in sequence.batches:
            if not batch.target_id:
                raise BuildError(f"batch {batch.id!r} has no target")
            if batch.target_id not in target_ids:
                raise BuildError(
                    f"batch {batch.id!r} names unknown target {batch.target_id!r}"
                )
            if batch.id in batch_ids:
                raise BuildError(f"duplicate batch id {batch.id!r}")
            batch_ids.add(batch.id)
            for action in batch.actions:
                if action.id in action_ids:
                    raise BuildError(f"duplicate action id {action.id!r}")
                action_ids.add(action.id)
                _validate_action_shape(action, omitted_ids)

    if seen_numbers != sorted(set(seen_numbers)) or len(seen_numbers) != len(set(seen_numbers)):
        raise BuildError(
            f"sequence numbers must be unique and ascending, got {seen_numbers}"
        )


def _validate_action_shape(action, omitted_ids) -> None:
    if action.executor not in VALID_EXECUTORS:
        raise BuildError(
            f"action {action.id!r} uses unsupported executor {action.executor!r}"
        )
    if action.resource_node_id is not None and action.resource_node_id in omitted_ids:
        raise BuildError(
            f"action {action.id!r} targets omitted node {action.resource_node_id!r}"
        )

    if action.payload is None:
        if action.payload_sha256 is not None:
            raise BuildError(
                f"action {action.id!r} has no payload but carries a payload hash"
            )
        return

    _check_payload_path(action.payload)
    extension = _EXECUTOR_EXTENSION[action.executor]
    if not action.payload.endswith(extension):
        raise BuildError(
            f"action {action.id!r} payload {action.payload!r} does not match "
            f"executor {action.executor!r} extension {extension!r}"
        )


def _validate_payload_integrity(location, plan: BuildPlan, store: Store) -> None:
    for _, _, action in plan.actions():
        if action.payload is None:
            continue
        payload_location = location.join(*action.payload.split("/"))
        if not store.exists(payload_location):
            raise BuildError(f"action {action.id!r} payload is missing: {action.payload!r}")
        digest = hashlib.sha256(store.read(payload_location)).hexdigest()
        if digest != action.payload_sha256:
            raise BuildError(
                f"action {action.id!r} payload hash mismatch for {action.payload!r} "
                f"(manifest {action.payload_sha256}, file {digest})"
            )


def _check_payload_path(payload: str) -> None:
    _check_relative(payload, what="payload path")
    if not payload.startswith(PAYLOAD_DIR + "/"):
        raise BuildError(
            f"payload {payload!r} must live under {PAYLOAD_DIR!r}/"
        )


def _check_relative(path: str, *, what: str) -> None:
    if path.startswith("/") or ":" in path:
        raise BuildError(f"{what} must be relative and stay in the bundle: {path!r}")
    parts = path.split("/")
    if any(part in ("", "..", ".") for part in parts):
        raise BuildError(f"{what} must not be empty or traverse: {path!r}")
