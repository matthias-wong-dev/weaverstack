"""One unit of Python work, spelled for both sides of the physical boundary.

Some work has to happen where the data is. In a notebook that is a function
call; from a desktop reaching into Fabric it is Python shipped into a Livy
session. A :class:`RemoteProgram` carries both
spellings so the caller chooses neither:

.. code-block:: text

    call()   → the payload, computed in this process
    source   → Python that computes the same payload and emits it

Holding them together is what stops the two drifting into different answers.
What still crosses this way is a run's deployed Python primitives, which are
imported where Spark is; build reads and installs are statements instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class RemoteProgram:
    """A named unit of work executable here or in a Fabric session.

    ``name`` is what timing and reporting call it, so it should read as the work
    rather than the mechanism, ``read_build_state``, not ``run_livy_body``.
    """

    name: str
    #: What runs when this process is already where the data is.
    call: Callable[[], Any]
    #: Python that produces the same payload and hands it back through ``emit``.
    source: str
    #: How long the remote spelling may take, where it differs from the default.
    timeout: float | None = None
    #: Anything the caller needs recorded alongside the timing.
    detail: str | None = None


__all__ = ["RemoteProgram"]
