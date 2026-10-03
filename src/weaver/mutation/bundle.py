"""Format-5 MutationPlan artifacts with checksummed payloads."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
from typing import Mapping

import yaml

from ..errors import BuildError
from ..locations import Location
from ..store import Store
from .models import MutationPlan
from .validation import validate_mutation_plan

DELETE_FILE = "delete_file"

#: Persisted MutationPlan format.
SUPPORTED_FORMAT_VERSION = 5

PLAN_FILENAME = "plan.yml"
PAYLOAD_DIR = "payload"

SPARK_SQL_EXECUTOR = "spark_sql"
SPARK_SQL_BATCH_EXECUTOR = "spark_sql_batch"
SPARK_TABLE_EXECUTOR = "spark_table"
TSQL_EXECUTOR = "tsql"
TSQL_BATCH_EXECUTOR = "tsql_batch"
FOLDER_EXECUTOR = "folder"
SHORTCUT_EXECUTOR = "shortcut"
SHORTCUT_READINESS_EXECUTOR = "shortcut_readiness"
LOAD_FILE_EXECUTOR = "load_file"
RUNTIME_STATE_EXECUTOR = "runtime_state"
LAKEHOUSE_WIPE_EXECUTOR = "lakehouse_wipe"
COPY_FILES_EXECUTOR = "copy_files"
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
        SHORTCUT_READINESS_EXECUTOR,
        LOAD_FILE_EXECUTOR,
        RUNTIME_STATE_EXECUTOR,
        LAKEHOUSE_WIPE_EXECUTOR,
        COPY_FILES_EXECUTOR,
        "semantic_model",
        "semantic_catalogue",
        "semantic_wipe",
    }
)
#: Required payload extension by executor.
_EXECUTOR_EXTENSION = {
    "semantic_model": ".semantic_model.json",
    "semantic_catalogue": ".semantic_catalogue.json",
    "semantic_wipe": ".semantic-wipe.json",
    SPARK_SQL_EXECUTOR: ".spark.sql",
    SPARK_SQL_BATCH_EXECUTOR: ".spark-sql-batch.json",
    SPARK_TABLE_EXECUTOR: ".spark-table.json",
    TSQL_EXECUTOR: ".sql",
    TSQL_BATCH_EXECUTOR: ".tsql-batch.json",
    SHORTCUT_EXECUTOR: ".shortcut.json",
    SHORTCUT_READINESS_EXECUTOR: ".shortcut-readiness.json",
    # Load payloads contain exact bytes of several content types. The extension
    # therefore identifies the load role.
    LOAD_FILE_EXECUTOR: ".payload",
    RUNTIME_STATE_EXECUTOR: ".runtime-state.json",
    COPY_FILES_EXECUTOR: ".copy-files.json",
}
_PAYLOADLESS_EXECUTORS = frozenset({FOLDER_EXECUTOR, LAKEHOUSE_WIPE_EXECUTOR})
#: Payloadless exceptions for executors that otherwise require one.
_PAYLOADLESS_KINDS = frozenset({DELETE_FILE})


@dataclass(frozen=True)
class BuildBundle:
    """A validated bundle and the store holding its files.

    The bundle store is independent of the target store. Inside
    Fabric, payloads live on the session driver's temporary filesystem while
    target Files mutations still use ``FabricStore``.
    A metadata-only handle may omit its store; execution supplies payload bytes.
    """

    location: Location
    plan: MutationPlan
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


def compute_bundle_id(plan: MutationPlan) -> str:
    """The identity of a plan, independent of its stored ``bundle_id`` field.

    The field is blanked before hashing so a plan's id never depends on itself,
    and everything else feeds in through the canonical mapping: signature,
    targets, sequences, payload hashes.
    """

    mapping = plan.to_mapping()
    mapping["bundle_id"] = ""
    return hashlib.sha256(_canonical_bytes(mapping)).hexdigest()


def plan_to_yaml(plan: MutationPlan) -> str:
    return yaml.safe_dump(
        plan.to_mapping(), sort_keys=False, default_flow_style=False, allow_unicode=True
    )


class _PlanLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        self.flatten_mapping(node)
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise BuildError(f"duplicate YAML field {key!r}")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def plan_from_yaml(text: str) -> MutationPlan:
    try:
        loaded = yaml.load(text, Loader=_PlanLoader)
    except (yaml.YAMLError, TypeError, ValueError) as exc:
        raise BuildError(f"invalid plan.yml: {exc}") from exc
    if not isinstance(loaded, dict):
        raise BuildError("plan.yml must be a mapping")
    check_format_version(loaded.get("format_version"))
    return MutationPlan.from_mapping(loaded)


def check_format_version(version) -> None:
    if type(version) is not int or version != SUPPORTED_FORMAT_VERSION:
        raise BuildError(
            f"Bundle format version {version} is not supported; this Weaver "
            f"version supports {SUPPORTED_FORMAT_VERSION}. Regenerate the bundle "
            "with this Weaver version."
        )


# --- writing -----------------------------------------------------------------


def write_bundle(
    location: Location,
    *,
    plan: MutationPlan,
    payloads: Mapping[str, bytes],
    store: Store,
) -> BuildBundle:
    """Write a bundle, manifest last, then reload and validate it.

    ``payloads`` is keyed by each action's bundle-relative payload path.
    """

    check_format_version(plan.format_version)
    validate_plan_structure(plan)
    if isinstance(plan, MutationPlan):
        plan = replace(plan, bundle_id=compute_bundle_id(plan))
    payloads = dict(payloads)
    for relative, data in payloads.items():
        _check_payload_path(relative)
        if not isinstance(data, bytes):
            raise BuildError(f"payload {relative!r} must contain immutable bytes")
    if isinstance(plan, MutationPlan):
        referenced = {a.payload for _, _, a in plan.actions() if a.payload is not None}
        if set(payloads) != referenced:
            raise BuildError("mutation payload inventory does not match the plan")
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


def validate_bundle(location: Location, plan: MutationPlan, *, store: Store) -> None:
    """Reject any structural or integrity fault before an action runs.

    Structure is checked first and needs no store, so a malformed manifest is
    caught the same way whether or not its payloads happen to exist; payload
    presence and hashes are checked second, against the store.
    """

    validate_plan_structure(plan)
    if isinstance(plan, MutationPlan) and not plan.bundle_id:
        raise BuildError("mutation bundle requires a sealed identity")
    _validate_payload_integrity(location, plan, store)


def validate_plan_structure(plan: MutationPlan) -> None:
    if not isinstance(plan, MutationPlan):
        raise BuildError(
            "format-5 bundles require a MutationPlan. Regenerate the bundle."
        )
    check_format_version(plan.format_version)
    validate_mutation_plan(plan)


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


def _validate_payload_integrity(location, plan: MutationPlan, store: Store) -> None:
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
    if not isinstance(path, str) or "\\" in path or "\x00" in path:
        raise BuildError(f"invalid {what}: {path!r}")
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
