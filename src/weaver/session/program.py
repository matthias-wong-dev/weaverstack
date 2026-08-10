"""One coarse unit of work, spelled for both sides of the physical boundary.

Weaver executes some work where the data is. On a desktop reaching into Fabric
that means shipping Python into a Livy session; in a notebook, or against the
local emulator, it means calling a function. Those are two spellings of the same
unit, and the caller above them should choose neither:

.. code-block:: python

    report = session.execute_python(install_program(archive, workspace))

A :class:`RemoteProgram` carries both spellings and the promise that binds them:

.. code-block:: text

    call()   → the payload, computed in this process
    source   → Python that computes the same payload and emits it

Keeping them in one object is what stops the two drifting into different
answers, and what stops every operation growing an ``if we are inside Fabric``
of its own. The unit stays **coarse** — a whole build state read, a whole
install, a whole run. Breaking these into host-driven steps is the later
decomposition work, and this shape is what makes that a change of granularity
rather than a redesign.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class RemoteProgram:
    """A named unit of work executable here or in a Fabric session.

    ``name`` is what timing and reporting call it, so it should read as the work
    rather than the mechanism — ``read_build_state``, not ``run_livy_body``.
    """

    name: str
    #: What runs when this process is already where the data is.
    call: Callable[[], Any]
    #: Python that produces the same payload and hands it back through ``emit``.
    source: str
    #: How long the remote spelling may take, where it differs from the default.
    timeout: float | None = None
    #: Anything the caller wants recorded alongside the timing.
    detail: str | None = None


__all__ = ["RemoteProgram"]
