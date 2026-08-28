"""The installed validation estate, and the order it runs in.

Selection from the installed managed graph, whose validation nodes come from
``_.TestDictionary`` and whose runnable artefacts come from ``_.Registry``.

A validation dispatches a compiled procedure or module that reports counts. It
is selected by name and by target, and it is never ordered against another
validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, Sequence

from .catalogue.state import Catalogue
from .catalogue.tables import TEST_DICTIONARY
from .declaration.metadata import TEST
from .declaration.model import WeaverDocumentId, WeaverItemId
from .errors import ValidationError
from .installed import KIND_FOR_TEST_TYPE, TEST_TYPE_FOR_KIND, InstalledNode
from .targets import PhysicalTargetRef

__all__ = [
    "KIND_FOR_TEST_TYPE",
    "TEST_TYPE_FOR_KIND",
    "InstalledValidation",
    "ValidationEstate",
    "validation_order",
]


@dataclass(frozen=True)
class InstalledValidation:
    """One logical validation, and the primitive that runs it.

    ``logical`` is what the estate calls it and what a caller names; ``artefact``
    is where the runnable thing is. Both are kept because the two questions have
    different answers: what this estate validates, and what to dispatch.
    """

    logical: WeaverDocumentId
    kind: str
    target: PhysicalTargetRef
    artefact: WeaverDocumentId
    #: What Registry says the primitive is, or ``None`` when it has no row, which
    #: is a missing installation rather than an absence of interest.
    object_type: str | None = None
    primary_key: tuple[str, ...] = ()
    description: str | None = None

    @classmethod
    def of(cls, node: InstalledNode) -> "InstalledValidation":
        """One validation node of the installed graph, as dispatch reads it."""

        return cls(
            logical=node.identity,
            kind=node.artefact_kind,
            target=node.target,
            artefact=node.artefact,
            object_type=node.artefact_type,
            primary_key=node.primary_key,
            description=node.description,
        )

    @property
    def is_installed(self) -> bool:
        return self.object_type is not None

    @property
    def is_test(self) -> bool:
        return self.kind == TEST

    @property
    def qualified(self) -> str:
        return self.logical.object_id.qualified

    def require_installed(self) -> None:
        """Refuse a declared validation whose primitive is not there.

        Reported rather than skipped, and reported as an execution failure
        rather than as a pass. A Test that could not be run is not a Test that
        found nothing. See :mod:`weaver.runtime.validation_result`.
        """

        if self.is_installed:
            return
        raise ValidationError(
            f"{self.logical} is declared in {TEST_DICTIONARY.name} but its "
            f"installed primitive {self.artefact} is not registered. Build the "
            "item before running its validation"
        )

    def to_mapping(self) -> dict:
        """Everything a dispatcher needs, as plain data.

        A validation crosses the host boundary as the description the estate
        gave of it, which is what it is here: a Registry row saying where the
        primitive lives and what it compares. Nothing is derived on the far side
        that was not derived here.
        """

        return {
            "logical": str(self.logical),
            "kind": self.kind,
            "target": {"kind": self.target.kind, "name": self.target.name},
            "artefact": str(self.artefact),
            "object_type": self.object_type,
            "primary_key": list(self.primary_key),
            "description": self.description,
        }

    @classmethod
    def from_mapping(cls, mapping) -> "InstalledValidation":
        return cls(
            logical=WeaverDocumentId.parse(mapping["logical"]),
            kind=mapping["kind"],
            target=PhysicalTargetRef(
                kind=mapping["target"]["kind"], name=mapping["target"]["name"]
            ),
            artefact=WeaverDocumentId.parse(mapping["artefact"]),
            object_type=mapping.get("object_type"),
            primary_key=tuple(mapping.get("primary_key", ())),
            description=_text(mapping.get("description")),
        )


@dataclass(frozen=True)
class ValidationEstate:
    """What the catalogue says is validatable, reversed for orchestration."""

    installations: Mapping[WeaverItemId, PhysicalTargetRef]
    validations: Mapping[WeaverDocumentId, InstalledValidation] = field(
        default_factory=dict
    )

    @classmethod
    def from_catalogue(cls, catalogue: Catalogue) -> "ValidationEstate":
        return cls.of(catalogue.dag())

    @classmethod
    def of(cls, dag) -> "ValidationEstate":
        """The validation nodes of one installed graph, keyed by logical identity."""

        return cls(
            installations=MappingProxyType(dict(dag.installations)),
            validations=MappingProxyType(
                {
                    node.identity: InstalledValidation.of(node)
                    for node in dag.validations()
                }
            ),
        )

    def for_targets(
        self, targets: Sequence[PhysicalTargetRef]
    ) -> tuple[InstalledValidation, ...]:
        """Everything installed in the requested physical targets, in ID order.

        By target rather than by item, because a request names a target and
        several logical items may be bound to one. The same grammar a load
        request uses, and the same meaning.
        """

        wanted = set(targets)
        return tuple(
            validation
            for _identity, validation in sorted(
                self.validations.items(), key=lambda pair: str(pair[0])
            )
            if validation.target in wanted
        )

    def named(
        self, name: str, targets: Sequence[PhysicalTargetRef]
    ) -> InstalledValidation:
        """One validation by its logical ``Schema.Object``, within the request.

        A miss is an error rather than an empty run: someone who named a
        validation is asking about that validation, and reporting nothing would
        answer a question they did not ask.
        """

        candidates = [
            validation
            for validation in self.for_targets(targets)
            if validation.qualified.casefold() == name.casefold()
        ]
        if not candidates:
            known = ", ".join(
                sorted(validation.qualified for validation in self.for_targets(targets))
            )
            raise ValidationError(
                f"no validation named {name!r} is installed in the requested "
                f"target(s). Installed: {known or 'none'}"
            )
        if len(candidates) > 1:
            found = ", ".join(str(validation.logical) for validation in candidates)
            raise ValidationError(
                f"{name!r} names more than one installed validation ({found}). "
                "qualify the request with a single target"
            )
        return candidates[0]


def validation_order(
    validations: Sequence[InstalledValidation],
) -> tuple[InstalledValidation, ...]:
    """Return a stable order for installed validations.

    Validations do not depend on one another; their own dependencies are
    installed and loaded before validation begins.
    """

    return tuple(sorted(validations, key=lambda each: str(each.logical)))


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
