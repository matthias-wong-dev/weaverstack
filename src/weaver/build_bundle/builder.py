"""Builder — what should be installed, decided before anything is mutated.

One of Weaver's four doers, and the one that makes every decision:

.. code-block:: text

    Repository + BuildState + bindings
                  ↓
               Builder
                  ↓
             BuildBundle
                  ↓
              Installer

**Builder is pure once its inputs are supplied.** It does not query Fabric REST,
read Spark catalogue state, inspect OneLake, connect to a Warehouse, read
physical catalogue tables, or install anything. Everything it needs to know
about the estate arrived as :class:`~weaver.build_bundle.workflow.BuildState`,
which a boundary read once, above.

That is not a stylistic preference. A decision made against state that is still
moving is a decision nobody can reproduce — and a planner that can reach the
estate will eventually be asked to check its own work, at which point the
question of who decided becomes unanswerable.

It also means a Builder can be constructed and run in a test with no Fabric, no
Spark and no estate at all: a Catalogue and some inventories are ordinary Python
objects. What such a test cannot prove is that those objects match what a real
readback produces, which is why the boundary fidelity tests exist and why they
are separate.

Reconciliation happens here, on the decision side. It is the step that turns
"what the catalogue claims" plus "what the target actually holds" into "what is
stale", and it needs no physical access to do it.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..declaration.repository import WeaverRepository
from ..locations import Location
from ..store import Store
from .bundle import BuildBundle
from .targets import ItemBindings, LakehouseBinding


@dataclass(frozen=True)
class Builder:
    """The decision half of a build: repository intent against observed state."""

    repository: WeaverRepository
    state: Any
    bindings: ItemBindings
    control_lakehouse: LakehouseBinding
    source_store: Store

    def build(self, *, output: Location | None = None) -> BuildBundle:
        """The bundle this repository and this state call for.

        ``output`` places the generated tree somewhere durable; without one the
        caller is expected to be inside a temporary directory it owns, because a
        bundle is a directory of generated payloads and something has to hold
        them.
        """

        from ..catalogue.state import reconcile_catalogue_state
        from .planner import generate_item_build_bundle
        from .workflow import validate_build_request

        validate_build_request(
            self.repository, self.bindings, control_lakehouse=self.control_lakehouse
        )
        reconciliation = reconcile_catalogue_state(
            self.state.catalogue, inventories=self.state.target_inventories
        )
        if output is None:
            raise ValueError("Builder.build needs an output location for the bundle")
        return generate_item_build_bundle(
            self.repository,
            bindings=self.bindings,
            output=output,
            store=self.source_store,
            target_inventories=self.state.target_inventories,
            catalogue=reconciliation.catalogue,
            stale_claims=reconciliation.stale_claims,
            control_lakehouse=self.control_lakehouse,
        )

    def build_in_temporary(self, prefix: str = "weaver-build-"):
        """The bundle, in a temporary tree that lives as long as the context.

        A convenience for the common shape — plan, install, discard — and the
        reason it is a context manager rather than a return value: the payloads
        are files, and something has to own them until the Installer has read
        them.
        """

        from contextlib import contextmanager

        @contextmanager
        def _built():
            with tempfile.TemporaryDirectory(prefix=prefix) as temporary:
                yield self.build(
                    output=Location((Path(temporary) / "bundle").as_posix())
                )

        return _built()


__all__ = ["Builder"]
