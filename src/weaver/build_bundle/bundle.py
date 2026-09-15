"""Writing, loading and validating a build bundle on a store.

A bundle is a directory:

.. code-block:: text

    <bundle>/
        plan.yml                     the canonical manifest
        payload/                     one generated definition per action
            010-create-schemas/
                create-DWG.spark.sql
            ...

The manifest is written **last**, so a half-written directory never looks
installable. Loading validates the whole bundle before any action can run:
structure, target bindings, payload presence, and payload hashes. The
installer must be able to trust what it is handed without re-reading the source.

``bundle_id`` is derived from stable inputs only: the format version, the
repository signature, the target descriptors and the canonical manifest with
its payload hashes. No timestamp participates, so the same inputs always yield
the same identity.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Mapping

import yaml

from ..errors import BuildError
from ..locations import Location
from ..store import Store
from .models import DELETE_FILE, OMISSION_REASONS, BuildPlan

#: The only bundle format this code writes and accepts.
SUPPORTED_FORMAT_VERSION = 3

PLAN_FILENAME = "plan.yml"
PAYLOAD_DIR = "payload"

SPARK_SQL_EXECUTOR = "spark_sql"
SPARK_SQL_BATCH_EXECUTOR = "spark_sql_batch"
SPARK_TABLE_EXECUTOR = "spark_table"
TSQL_EXECUTOR = "tsql"
TSQL_BATCH_EXECUTOR = "tsql_batch"
FOLDER_EXECUTOR = "folder"
SHORTCUT_EXECUTOR = "shortcut"
SQL_ENDPOINT_REFRESH_EXECUTOR = "sql_endpoint_refresh"
LOAD_FILE_EXECUTOR = "load_file"
RUNTIME_STATE_EXECUTOR = "runtime_state"
#: Executors accepted in a bundle manifest. Batch executors preserve statement
#: order; target-only executors carry no payload.
VALID_EXECUTORS = frozenset(
    {
        SPARK_SQL_EXECUTOR,
        SPARK_SQL_BATCH_EXECUTOR,
        SPARK_TABLE_EXECUTOR,
        TSQL_EXECUTOR,
        TSQL_BATCH_EXECUTOR,
        FOLDER_EXECUTOR,
        SHORTCUT_EXECUTOR,
        SQL_ENDPOINT_REFRESH_EXECUTOR,
        LOAD_FILE_EXECUTOR,
        RUNTIME_STATE_EXECUTOR,
    }
)
#: Required payload extension by executor.
_EXECUTOR_EXTENSION = {
    SPARK_SQL_EXECUTOR: ".spark.sql",
    SPARK_SQL_BATCH_EXECUTOR: ".spark-sql-batch.json",
    SPARK_TABLE_EXECUTOR: ".spark-table.json",
    TSQL_EXECUTOR: ".sql",
    TSQL_BATCH_EXECUTOR: ".tsql-batch.json",
    SHORTCUT_EXECUTOR: ".shortcut.json",
    # Load payloads contain exact bytes of several content types. The extension
    # therefore identifies the load role.
    LOAD_FILE_EXECUTOR: ".payload",
    RUNTIME_STATE_EXECUTOR: ".runtime-state.json",
}
_PAYLOADLESS_EXECUTORS = frozenset({FOLDER_EXECUTOR, SQL_ENDPOINT_REFRESH_EXECUTOR})
#: Payloadless exceptions for executors that otherwise require one.
_PAYLOADLESS_KINDS = frozenset({DELETE_FILE})


@dataclass(frozen=True)
class BuildBundle:
    """A validated bundle and the store holding its files.

    The bundle store is independent of the target store. Inside
    Fabric, payloads live on the session driver's temporary filesystem while
    target Files mutations still use ``FabricStore``.
    ``None`` remains accepted for compatibility with callers that reconstruct a
    lightweight handle and let the installer use its environment store.
    """

    location: Location
    plan: BuildPlan
    store: Store | None = field(default=None, compare=False, repr=False)

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
    and everything else feeds in through the canonical mapping: signature,
    targets, sequences, payload hashes.
    """

    mapping = plan.to_mapping()
    mapping["bundle_id"] = ""
    return hashlib.sha256(_canonical_bytes(mapping)).hexdigest()


def plan_to_yaml(plan: BuildPlan) -> str:
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
    store: Store,
) -> BuildBundle:
    """Write a bundle, manifest last, then reload and validate it.

    ``payloads`` is keyed by each action's bundle-relative payload path.
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
    return BuildBundle(location=location, plan=plan, store=store)


def validate_bundle(location: Location, plan: BuildPlan, *, store: Store) -> None:
    """Reject any structural or integrity fault before an action runs.

    Structure is checked first and needs no store, so a malformed manifest is
    caught the same way whether or not its payloads happen to exist; payload
    presence and hashes are checked second, against the store.
    """

    validate_plan_structure(plan)
    _validate_payload_integrity(location, plan, store)


def validate_plan_structure(plan: BuildPlan) -> None:
    if plan.format_version != SUPPORTED_FORMAT_VERSION:
        raise BuildError(
            f"Bundle format version {plan.format_version} is not supported; this "
            f"Weaver version supports {SUPPORTED_FORMAT_VERSION}. Regenerate the "
            "bundle with this Weaver version."
        )

    for node in plan.omitted_nodes:
        if node.reason not in OMISSION_REASONS:
            raise BuildError(
                f"The build bundle is invalid: omitted item {node.node_id!r} has "
                f"unsupported reason {node.reason!r}. Regenerate the bundle with "
                "this Weaver version."
            )
    omitted_ids = {node.node_id for node in plan.omitted_nodes}

    target_ids = plan.target_ids
    if len(target_ids) != len(plan.targets):
        raise BuildError(
            "The build bundle contains duplicate targets. Regenerate it with this "
            "Weaver version."
        )
    for target in plan.targets:
        if (target.logical_item_type is None) != (target.logical_item_name is None):
            raise BuildError(
                f"The build bundle does not completely identify the Weaver item for "
                f"target {target.id!r}. Regenerate it with this Weaver version."
            )
        if target.logical_item_type is not None:
            expected = {
                "Lakehouse": "lakehouse",
                "Warehouse": "warehouse",
            }.get(target.logical_item_type)
            if expected != target.kind:
                raise BuildError(
                    f"The build bundle sends a {target.logical_item_type} item to "
                    f"incompatible target {target.id!r} of type {target.kind!r}. "
                    "Regenerate it with this Weaver version."
                )

    seen_numbers: list[int] = []
    batch_ids: set[str] = set()
    action_ids: set[str] = set()

    for sequence in plan.sequences:
        seen_numbers.append(sequence.number)
        for batch in sequence.batches:
            if not batch.target_id:
                raise BuildError(
                    f"The build bundle has an installation stage {batch.id!r} with "
                    "no target. Regenerate it with this Weaver version."
                )
            if batch.target_id not in target_ids:
                raise BuildError(
                    f"The build bundle names unknown target {batch.target_id!r} in "
                    f"installation stage {batch.id!r}. Regenerate it with this "
                    "Weaver version."
                )
            if batch.id in batch_ids:
                raise BuildError(
                    f"The build bundle repeats installation stage {batch.id!r}. "
                    "Regenerate it with this Weaver version."
                )
            batch_ids.add(batch.id)
            for action in batch.actions:
                if action.id in action_ids:
                    raise BuildError(
                        f"The build bundle repeats installation action {action.id!r}. "
                        "Regenerate it with this Weaver version."
                    )
                action_ids.add(action.id)
                _validate_action_shape(action, omitted_ids)

    if seen_numbers != sorted(set(seen_numbers)) or len(seen_numbers) != len(
        set(seen_numbers)
    ):
        raise BuildError(
            f"The build bundle's installation stages are not uniquely ordered: "
            f"{seen_numbers}. Regenerate it with this Weaver version."
        )


def _validate_action_shape(action, omitted_ids) -> None:
    if action.executor not in VALID_EXECUTORS:
        raise BuildError(
            f"The build bundle uses unsupported installation method "
            f"{action.executor!r} for action {action.id!r}. Regenerate it with this "
            "Weaver version."
        )
    if action.resource_node_id is not None and action.resource_node_id in omitted_ids:
        raise BuildError(
            f"The build bundle tries to install omitted object "
            f"{action.resource_node_id!r}. Regenerate it with this Weaver version."
        )

    if action.payload is None:
        if action.payload_sha256 is not None:
            raise BuildError(
                f"The build bundle has a checksum but no installation file for "
                f"action {action.id!r}. Regenerate it with this Weaver version."
            )
        if (
            action.executor not in _PAYLOADLESS_EXECUTORS
            and action.kind not in _PAYLOADLESS_KINDS
        ):
            raise BuildError(
                f"The build bundle has no installation file for action {action.id!r}. "
                "Regenerate it with this Weaver version."
            )
        return

    if action.executor in _PAYLOADLESS_EXECUTORS or action.kind in _PAYLOADLESS_KINDS:
        raise BuildError(
            f"The build bundle gives installation action {action.id!r} an unexpected "
            "file. Regenerate it with this Weaver version."
        )
    _check_payload_path(action.payload)
    extension = _EXECUTOR_EXTENSION[action.executor]
    if not action.payload.endswith(extension):
        raise BuildError(
            f"The build bundle gives action {action.id!r} installation file "
            f"{action.payload!r}; it must end in {extension!r}. Regenerate the "
            "bundle with this Weaver version."
        )


def _validate_payload_integrity(location, plan: BuildPlan, store: Store) -> None:
    for _, _, action in plan.actions():
        if action.payload is None:
            continue
        payload_location = location.join(*action.payload.split("/"))
        if not store.exists(payload_location):
            raise BuildError(
                f"The build bundle is missing installation file {action.payload!r} "
                f"for action {action.id!r}. Regenerate it with this Weaver version."
            )
        digest = hashlib.sha256(store.read(payload_location)).hexdigest()
        if digest != action.payload_sha256:
            raise BuildError(
                f"Installation file {action.payload!r} in the build bundle does not "
                f"match its checksum for action {action.id!r}. Regenerate the bundle "
                "with this Weaver version."
            )


def _check_payload_path(payload: str) -> None:
    _check_relative(payload, what="payload path")
    if not payload.startswith(PAYLOAD_DIR + "/"):
        raise BuildError(
            f"Build bundle file {payload!r} is outside {PAYLOAD_DIR!r}/. Regenerate "
            "the bundle with this Weaver version."
        )


def _check_relative(path: str, *, what: str) -> None:
    if path.startswith("/") or ":" in path:
        raise BuildError(
            f"Build bundle {what} {path!r} is not relative. Regenerate the bundle "
            "with this Weaver version."
        )
    parts = path.split("/")
    if any(part in ("", "..", ".") for part in parts):
        raise BuildError(
            f"Build bundle {what} {path!r} is invalid. Regenerate the bundle with "
            "this Weaver version."
        )
