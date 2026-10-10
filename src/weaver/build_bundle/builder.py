"""Pure build planning from repository intent and observed target state."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ..declaration.repository import WeaverRepository
from ..locations import Location
from ..store import Store
from .bundle import BuildBundle
from .execution import ExecutionIdentity
from .targets import ItemBindings, WarehouseBinding


@dataclass(frozen=True)
class Builder:
    repository: WeaverRepository
    state: Any
    bindings: ItemBindings
    catalogue_binding: WarehouseBinding
    source_store: Store
    #: Resolved before planning starts. The planner completes it with the Spark
    #: attachment its action set requires; it makes no calls of its own.
    execution: ExecutionIdentity
    #: Receives each authoring warning found while compiling the selection.
    warn: Callable[[str], None] | None = field(default=None, compare=False)

    def build(self, *, output: Location | None = None) -> BuildBundle:
        from ..catalogue.state import reconcile_catalogue_state
        from ..semantic_models.binding import bind_semantic_sources
        from ..semantic_models.expressions import bind_expression_sources
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
        repository = bind_expression_sources(
            self.repository, self.state.semantic_expressions
        )
        repository = bind_semantic_sources(
            repository, self.state.semantic_sources, self.bindings.by_item
        )
        # Without a catalogue Load orders nothing, so tracing changes nothing.
        if self.warn is not None and self.catalogue_binding is not None:
            from ..semantic_models.lineage import untraced_warning

            for item, contribution in sorted(
                repository.semantic_models.items(), key=lambda pair: str(pair[0])
            ):
                if item in self.bindings.by_item:
                    message = untraced_warning(item, contribution)
                    if message:
                        self.warn(message)
        return generate_item_build_bundle(
            repository,
            bindings=self.bindings,
            output=output,
            store=self.source_store,
            target_inventories=self.state.target_inventories,
            catalogue=reconciliation.catalogue,
            stale_claims=reconciliation.stale_claims,
            catalogue_binding=self.catalogue_binding,
            execution=self.execution,
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
