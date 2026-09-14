"""Pure build planning from repository intent and observed target state."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..declaration.repository import WeaverRepository
from ..locations import Location
from ..store import Store
from .bundle import BuildBundle
from .targets import ItemBindings, WarehouseBinding


@dataclass(frozen=True)
class Builder:
    repository: WeaverRepository
    state: Any
    bindings: ItemBindings
    catalogue_binding: WarehouseBinding
    source_store: Store

    def build(self, *, output: Location | None = None) -> BuildBundle:
        """Write the planned bundle tree to ``output``."""

        from ..catalogue.state import reconcile_catalogue_state
        from .planner import generate_item_build_bundle
        from .workflow import validate_build_request

        validate_build_request(
            self.repository, self.bindings, catalogue_binding=self.catalogue_binding
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
            catalogue_binding=self.catalogue_binding,
            shortcut_sources=self.state.shortcut_sources,
        )

    def build_in_temporary(self, prefix: str = "weaver-build-"):
        """Yield a bundle whose payloads live for the context's lifetime."""

        from contextlib import contextmanager

        @contextmanager
        def _built():
            with tempfile.TemporaryDirectory(prefix=prefix) as temporary:
                yield self.build(
                    output=Location((Path(temporary) / "bundle").as_posix())
                )

        return _built()


__all__ = ["Builder"]
