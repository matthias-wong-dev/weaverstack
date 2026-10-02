"""Build action vocabulary over the shared mutation types.

Sequences group actions for presentation, batches bind actions to one target,
and every type serialises to the canonical manifest.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..mutation.models import BoundTarget as BoundTarget
from ..mutation.models import BuildBatch as BuildBatch
from ..mutation.models import BuildSequence as BuildSequence
from ..mutation.models import InstallAction as InstallAction

#: Action kinds. Create kinds build structure; prune kinds reconcile the target.
CREATE_SCHEMA = "create_schema"
CREATE_SHORTCUT = "create_shortcut"
BUILD_FOLDER = "build_folder"
BUILD_TABLE = "build_table"
BUILD_VIEW = "build_view"

#: Created shortcuts become readable to their consumers.
AWAIT_TABLE_SHORTCUTS = "await_table_shortcuts"
AWAIT_FILE_SHORTCUTS = "await_file_shortcuts"

#: A SQL analytics endpoint refresh, started and then awaited.
START_ENDPOINT_REFRESH = "start_sql_endpoint_refresh"
AWAIT_ENDPOINT_REFRESH = "await_sql_endpoint_refresh"

#: Runtime artefacts installed in an item's final layer.
WRITE_FILE = "write_file"
BUILD_PROCEDURE = "build_procedure"

#: Rebuild drops remove desired objects before their selected definition is recreated.
DROP_FOLDER = "drop_folder"
DROP_TABLE = "drop_table"
DROP_VIEW = "drop_view"
#: The drop for an identity the previous build installed as a Lakehouse pointer.
#: A OneLake shortcut is a read-write window into the item it points at, so it
#: comes off through the shortcut API; a ``DROP TABLE`` or a directory removal
#: would reach that item's data.
DROP_SHORTCUT = "drop_shortcut"

#: Prior Registry claims determine runtime artefact removals.
DELETE_FILE = "delete_file"
DROP_PROCEDURE = "drop_procedure"

#: Prune kinds. Each names one frozen drop the build computed against the target:
#: a Spark SQL DROP for a table/view/schema, a directory removal for a folder.
PRUNE_TABLE = "prune_table"
PRUNE_VIEW = "prune_view"
PRUNE_SCHEMA = "prune_schema"
PRUNE_FOLDER = "prune_folder"

#: Catalogue actions target the central Warehouse. Claim deletion precedes
#: physical work; publication follows it with Registry last.
DELETE_CATALOGUE_CLAIMS = "delete_catalogue_claims"
PUBLISH_CATALOGUE = "publish_catalogue"
PUBLISH_REGISTRY = "publish_registry"
#: Remove current-state rows for retired and replaced object incarnations before
#: physical work.
RECONCILE_RUNTIME_STATE = "reconcile_runtime_state"
CATALOGUE_KINDS = frozenset(
    {
        DELETE_CATALOGUE_CLAIMS,
        PUBLISH_CATALOGUE,
        PUBLISH_REGISTRY,
        RECONCILE_RUNTIME_STATE,
    }
)

#: Reasons a repository node is not in the plan.
OMIT_TARGET_UNBOUND = "target_unbound"
OMIT_DEPENDS_ON_OMITTED = "depends_on_omitted_node"
OMIT_UNSUPPORTED_EXECUTOR = "unsupported_executor"
#: A shortcut for which current bindings provide no physical form.
OMIT_SHORTCUT_UNSUPPORTED = "shortcut_unsupported"
OMISSION_REASONS = frozenset(
    {
        OMIT_TARGET_UNBOUND,
        OMIT_DEPENDS_ON_OMITTED,
        OMIT_UNSUPPORTED_EXECUTOR,
        OMIT_SHORTCUT_UNSUPPORTED,
    }
)


@dataclass(frozen=True)
class OmittedNode:
    """A repository node the projection left out, and why."""

    node_id: str
    reason: str
    detail: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        mapping: dict[str, Any] = {"node_id": self.node_id, "reason": self.reason}
        if self.detail is not None:
            mapping["detail"] = self.detail
        return mapping

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "OmittedNode":
        return cls(
            node_id=mapping["node_id"],
            reason=mapping["reason"],
            detail=mapping.get("detail"),
        )
