"""dispatch_primitive — the one place a run crosses into a real engine.

.. code-block:: text

    RunNode
       ↓
    primitive kind / runtime reference
       ↓
    Session capability
       ↓
    the installed runtime artefact

A narrow function, deliberately, and not a fifth doer. Everything about *when* a
node runs belongs to the Runner; everything about *how* to reach an engine
belongs to the Session. What is left here is the translation between them, which
is small enough that giving it a lifecycle would be inventing one.

It is also the seam a run-cycle test replaces. Nothing here is aware of that:
the Runner calls whatever callable it was given, and a controlled outcome and a
real one arrive by the same route.
"""

from __future__ import annotations

from ..errors import LoadError


def dispatch_primitive(node, *, session=None, state=None):
    """Run one installed primitive and return its result.

    The delegation is to the existing runtime dispatch while the load
    orchestration stack is being absorbed; what matters architecturally is that
    the Runner reaches an engine through exactly this call, and that the call
    takes a node rather than a plan.
    """

    if session is None:
        raise LoadError(
            f"{node.node_id} needs a Session to reach {node.physical_target}; "
            "a run with no Session must be given a dispatch of its own"
        )
    raise NotImplementedError(
        "real primitive dispatch is wired as the load orchestration stack is "
        "absorbed into the Runner"
    )


__all__ = ["dispatch_primitive"]
