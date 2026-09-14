"""Python work that runs locally in Fabric or remotely through Livy.

In a notebook, the work is a function call. On a desktop, the equivalent Python
runs through Livy. A :class:`RemoteProgram` carries both forms:

.. code-block:: text

    call()   → the payload, computed in this process
    source   → Python that computes the same payload and emits it

The local and remote forms must return the same value. This mechanism is for a
run's deployed Python primitives; build reads and installs use statements.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class RemoteProgram:
    """A named unit of work with equivalent local and remote forms.

    ``name`` identifies the work in timing and reporting. Use
    ``read_build_state``, not ``run_livy_body``.
    """

    name: str
    call: Callable[[], Any]
    source: str
    timeout: float | None = None
    detail: str | None = None


__all__ = ["RemoteProgram"]
