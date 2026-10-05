"""Python work that runs in Fabric, called in place or sent from a client.

In a notebook, the work is a function call. From a client, the equivalent Python
is sent to Fabric through Livy. A :class:`FabricProgram` carries both forms:

.. code-block:: text

    call()   → the payload, computed in this process
    source   → Python that computes the same payload and emits it

Both forms must return the same value. This mechanism is for a
run's deployed Python primitives; build reads and installs use statements.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class FabricProgram:
    """A named unit of Fabric work, as a call and as source a client sends.

    ``name`` identifies the work in timing and reporting. Use
    ``read_build_state``, not ``run_livy_body``.
    """

    name: str
    call: Callable[[], Any]
    source: str
    timeout: float | None = None
    detail: str | None = None


__all__ = ["FabricProgram"]
