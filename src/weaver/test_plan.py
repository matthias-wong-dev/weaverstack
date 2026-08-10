"""The installed validation estate, and the order it runs in.

The sibling of :mod:`weaver.load_plan`, and it shares that module's whole
premise: **the installed catalogue is authoritative and the repository is not
reopened.** By the time anything is runnable it has been built, and what was
built is recorded. Reading the declaration instead would orchestrate what
somebody meant to install rather than what is installed, and the two differ
exactly when it matters most.

One thing here is genuinely unlike load planning, and it is the reason this
module exists rather than a few more branches in that one.

**A validation has no Registry row.** Nothing is materialised under a logical
Test ID, so the estate cannot be recovered by walking Registry the way load
planning walks it. It is recovered from ``_.TestDictionary`` — the logical
declarations — and each is then connected to its installed primitive by
*computing* the artefact identity with
:func:`weaver.etl.validation_artefact_id`, the same function the build claimed
it with. A `TestDictionary` row whose computed artefact is absent from Registry
is a missing installation, reported as an invalid node rather than skipped: a
Test that was declared and never installed must not read as an estate with one
fewer Test in it.

Dependency rows are associated the same way, against logical IDs rather than
through Registry. That is the whole of what §14 of the design settles, and a
fake Registry row for the logical validation — which would have let the load
helper be reused unchanged — is exactly what it settles against.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, Sequence

from .catalogue.state import Catalogue
from .catalogue.tables import DEPENDENCY, TEST_DICTIONARY
from .declaration.metadata import ASSUMPTION, ObjectId, TEST
from .declaration.model import WeaverDocumentId, WeaverItemId
from .errors import ValidationError
from .etl import validation_artefact_id
from .load_plan import PhysicalTargetRef, _installations

#: How ``TestDictionary.test_type`` spells each kind, and back. The catalogue's
#: vocabulary is lower case and a declaration's kind is title case, so the
#: translation is pinned in one place rather than guessed at each reader.
KIND_FOR_TEST_TYPE = {"test": TEST, "assumption": ASSUMPTION}
TEST_TYPE_FOR_KIND = {kind: name for name, kind in KIND_FOR_TEST_TYPE.items()}


@dataclass(frozen=True)
class InstalledValidation:
    """One logical validation, and the primitive that runs it.

    ``logical`` is what the estate calls it and what a caller names; ``artefact``
    is where the runnable thing actually is. Keeping both is the point of the
    whole arrangement — a reader asking "what does this estate validate?" wants
    the first, and a dispatcher wants the second.
    """

    logical: WeaverDocumentId
    kind: str
    target: PhysicalTargetRef
    artefact: WeaverDocumentId
    #: What Registry says the primitive is, or ``None`` when it has no row —
    #: which is a missing installation, not an absence of interest.
    object_type: str | None = None
    primary_key: tuple[str, ...] = ()
    description: str | None = None
    dependencies: tuple[str, ...] = ()

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
        found nothing — see :mod:`weaver.runtime.validation_result`.
        """

        if self.is_installed:
            return
        raise ValidationError(
            f"{self.logical} is declared in {TEST_DICTIONARY.name} but its "
            f"installed primitive {self.artefact} is not registered — build the "
            "item before running its validation"
        )

    def to_mapping(self) -> dict:
        """Everything a dispatcher needs, as plain data.

        A validation crosses the host boundary as the description the estate
        gave of it — which is what it *is* here: a Registry row saying where the
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
            "dependencies": list(self.dependencies),
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
            dependencies=tuple(mapping.get("dependencies", ())),
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
        installations = _installations(catalogue)
        dependencies = _validation_dependencies(catalogue)
        found: dict[WeaverDocumentId, InstalledValidation] = {}

        for item, tables in catalogue.rows.items():
            target = installations.get(item)
            for row in tables.get(TEST_DICTIONARY.name, ()):
                logical = WeaverDocumentId(
                    item,
                    ObjectId(
                        schema=str(row.get("schema_name") or ""),
                        object=str(row.get("object_name") or ""),
                    ),
                )
                kind = _kind(row, logical)
                if target is None:
                    raise ValidationError(
                        f"{logical} is declared but {item} has no installation "
                        "row, so its physical target is unknown"
                    )
                artefact = validation_artefact_id(item, kind, logical.object_id)
                registered = catalogue.registered.get(artefact)
                found[logical] = InstalledValidation(
                    logical=logical,
                    kind=kind,
                    target=target,
                    artefact=artefact,
                    object_type=registered.object_type if registered else None,
                    primary_key=_column_set(row.get("primary_key")),
                    description=_text(row.get("description")),
                    dependencies=tuple(dependencies.get(logical, ())),
                )
        return cls(
            installations=MappingProxyType(dict(installations)),
            validations=MappingProxyType(found),
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
                f"{name!r} names more than one installed validation ({found}) — "
                "qualify the request with a single target"
            )
        return candidates[0]


def validation_order(
    validations: Sequence[InstalledValidation],
) -> tuple[InstalledValidation, ...]:
    """The order to run them in, which is the order of what they read.

    Deliberately shallow. Nothing depends on a validation — the repository
    refuses a declaration that names one — so there are no edges *between* these
    nodes to order, and a validation's own dependencies were installed and
    loaded before it. What remains is a stable order, and stability is what
    makes a run's report comparable with the last one's.

    A second dependency language for validation is what the design says not to
    build, and this is where the temptation would have been.
    """

    return tuple(sorted(validations, key=lambda each: str(each.logical)))


def _kind(row: Mapping[str, object], logical: WeaverDocumentId) -> str:
    test_type = str(row.get("test_type") or "").strip().casefold()
    try:
        return KIND_FOR_TEST_TYPE[test_type]
    except KeyError:
        expected = ", ".join(sorted(KIND_FOR_TEST_TYPE))
        raise ValidationError(
            f"{logical} has unsupported test_type {test_type!r}; expected one of "
            f"{expected}"
        ) from None


def _validation_dependencies(
    catalogue: Catalogue,
) -> dict[WeaverDocumentId, list[str]]:
    """Dependency rows keyed by the *logical* validation that owns them.

    Matched against ``TestDictionary`` keys rather than recovered through
    Registry, because a validation has no Registry row to recover it through.
    """

    logical: dict[WeaverDocumentId, list[str]] = {}
    for item, tables in catalogue.rows.items():
        declared = {
            (
                str(row.get("schema_name") or ""),
                str(row.get("object_name") or ""),
            )
            for row in tables.get(TEST_DICTIONARY.name, ())
        }
        for row in tables.get(DEPENDENCY.name, ()):
            key = (
                str(row.get("schema_name") or ""),
                str(row.get("object_name") or ""),
            )
            if key not in declared:
                continue
            identity = WeaverDocumentId(item, ObjectId(schema=key[0], object=key[1]))
            logical.setdefault(identity, []).append(
                str(row.get("dependency_name") or "")
            )
    return {identity: sorted(names) for identity, names in logical.items()}


def _column_set(value: object) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(part.strip() for part in str(value).split(",") if part.strip())


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "KIND_FOR_TEST_TYPE",
    "TEST_TYPE_FOR_KIND",
    "InstalledValidation",
    "ValidationEstate",
    "validation_order",
]
