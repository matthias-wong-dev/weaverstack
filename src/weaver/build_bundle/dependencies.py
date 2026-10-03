"""Name the physical facts Build actions establish and need.

A planner declares, per action, the keys it provides, the keys whose providers
must succeed first (``requires``) and the keys whose providers must only reach a
known outcome first (``follows``). A key no action in the plan provides is
already satisfied by the target, so it adds no edge.
"""

from __future__ import annotations

#: The catalogue supports the identity columns this Build publishes.
UPGRADED = "catalogue:upgraded"
#: Every claim deletion has run; the selected objects are no longer certified.
DECERTIFIED = "catalogue:decertified"
#: Decertification and runtime-state reset are complete. Physical work needs it.
PREPARED = "catalogue:prepared"
#: Every physical action has succeeded. Catalogue publication needs it.
PHYSICAL_COMPLETE = "physical:complete"


def action_key(action_id: str) -> str:
    """Provided by the action itself."""

    return f"action:{action_id}"


OBJECT = "object:"


def object_key(identity) -> str:
    """The object exists and its consumer surface can read it."""

    return f"{OBJECT}{identity}"


def dropped_key(identity) -> str:
    return f"dropped:{identity}"


def schema_key(target_id: str, schema: str) -> str:
    return f"schema:{target_id}:{schema.casefold()}"


def pruned_objects_key(target_id: str) -> str:
    return f"pruned-objects:{target_id}"


def runtime_removal_key(target_id: str) -> str:
    return f"runtime-removal:{target_id}"


def endpoint_object_key(identity) -> str:
    """The object is current in its Lakehouse's SQL analytics endpoint."""

    return f"endpoint-object:{identity}"


def catalogue_step_key(index: int) -> str:
    return f"catalogue:published:{index}"
