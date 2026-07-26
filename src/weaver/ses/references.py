"""Following a ``$Schema.Object`` documentation reference to its text.

A ``Description``, ``Lineage`` or column note is either literal prose or exactly
one reference (:class:`~weaver.ses.metadata.MetadataText`). A reference means
*the text over there is the text here* — the same sentence written once and
pointed at from everywhere it applies, so a correction lands in one place.

Resolving one is therefore a copy, and this module performs it:

.. code-block:: text

    Description: $Sales.Order          -> Sales.Order's description
    Description: $Sales.Order[Order id] -> that column's note on Sales.Order

Pointers may chain — B points at A, C points at B — and the chain is followed to
the literal at its end. A **cycle** is an error: it can never produce text, so it
is a broken declaration rather than a missing one.

**A documentation reference is not a dependency.** A dependency binds in its
consumer's execution namespace, because that is what the SQL will bind to. A
reference is a logical pointer to *the object of that name*, and the interesting
case is precisely the cross-target one: the Warehouse ``Sales.Customer`` written
by a Lakehouse table of the same name, saying "I come from that Delta table".
Resolution therefore excludes the referring object itself and prefers the
referrer's own namespace only to break a tie.

Unresolved is **not** an error. A reference may legitimately name an object in
another repository, and refusing it would cost someone a working object for a
documentation nicety. The reference is recorded as written and the resolved text
is simply absent — the catalogue keeps both columns for exactly this reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from ..errors import DiscoveryError
from .metadata import MetadataText, Reference
from .source import SourceDocument

#: The note the catalogue gives Weaver's own surrogate column, which no author
#: writes and every table with an ``Identity`` header has.
IDENTITY_COLUMN_NOTE = (
    "Weaver-managed surrogate key. Created by build as a not-null bigint and "
    "populated by load; not part of the declared business schema."
)


@dataclass(frozen=True)
class ResolvedText:
    """One piece of metadata, split into its text and where the text came from.

    ``literal`` is the prose — written here, or copied from the end of a
    reference chain, or ``None`` when a reference could not be followed.
    ``reference`` is the ``$Schema.Object[Column]`` as written, or ``None`` when
    the prose was written here.
    """

    literal: str | None = None
    reference: str | None = None

    @property
    def is_reference(self) -> bool:
        return self.reference is not None


def resolve_text(
    text: MetadataText | None,
    *,
    owner: SourceDocument,
    documents: Iterable[SourceDocument],
) -> ResolvedText:
    """Follow one piece of metadata to its literal prose.

    ``documents`` is every object in the repository — resolution needs siblings.
    Raises :class:`~weaver.errors.DiscoveryError` when the chain cycles.
    """

    if text is None:
        return ResolvedText()
    if not text.is_reference:
        return ResolvedText(literal=text.literal)

    index = _index(documents)
    written = str(text.reference)
    literal = _follow(text.reference, owner, index, seen=[(owner.node_id, written)])
    return ResolvedText(literal=literal, reference=written)


def _index(documents: Iterable[SourceDocument]) -> Mapping[str, list[SourceDocument]]:
    grouped: dict[str, list[SourceDocument]] = {}
    for document in documents:
        grouped.setdefault(document.qualified.lower(), []).append(document)
    return grouped


def _follow(
    reference: Reference,
    referrer: SourceDocument,
    index: Mapping[str, list[SourceDocument]],
    *,
    seen: list[tuple[str, str]],
) -> str | None:
    """The literal at the end of a chain, or None when it cannot be followed."""

    target = _target(reference, referrer, index)
    if target is None:
        return None

    step = (target.node_id, reference.column or "")
    if step in seen:
        trail = " -> ".join(f"{node}[{column}]" if column else node for node, column in seen)
        raise DiscoveryError(
            f"metadata reference cycle: {trail} -> {target.node_id} — a reference "
            "copies text from its target, so a cycle has no text to copy"
        )

    text = _text_of(target, reference.column)
    if text is None:
        return None
    if not text.is_reference:
        return text.literal
    return _follow(text.reference, target, index, seen=seen + [step])


def _target(
    reference: Reference,
    referrer: SourceDocument,
    index: Mapping[str, list[SourceDocument]],
) -> SourceDocument | None:
    """The object a documentation reference names, excluding the referrer itself.

    Excluding self is what makes the cross-target case work: the Warehouse
    ``Sales.Customer`` naming ``$Sales.Customer`` means the Delta table, because
    it cannot sensibly mean itself. With more than one candidate left, the
    referrer's own namespace wins; failing that the reference is ambiguous and is
    left unresolved rather than guessed.
    """

    candidates = [
        candidate
        for candidate in index.get(reference.object_id.qualified.lower(), [])
        if candidate.node_id != referrer.node_id
    ]
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        return None
    same_namespace = [
        candidate for candidate in candidates if candidate.namespace == referrer.namespace
    ]
    return same_namespace[0] if len(same_namespace) == 1 else None


def _text_of(document: SourceDocument, column: str | None) -> MetadataText | None:
    """The referenced text on a target: its description, or one column's note."""

    if column is None:
        return document.document.description
    return column_note(document, column)


def column_note(document: SourceDocument, column: str) -> MetadataText | None:
    """One column's declared note, however the object declares its shape.

    A declared schema carries notes on its columns. An inferred one has no
    declared columns to carry them, so its notes stay in the raw metadata block
    — the same split :func:`weaver.ses.columns.metadata_column_references` makes.
    """

    ses = document.document
    for declared in ses.schema:
        if declared.name == column and declared.note is not None:
            return declared.note
    if ses.identity == column:
        return MetadataText(literal=IDENTITY_COLUMN_NOTE)
    if not ses.has_declared_schema:
        notes = ses.raw.get("Column notes") or {}
        if isinstance(notes, dict):
            for name, note in notes.items():
                if str(name).strip() == column:
                    from .metadata import _parse_text_value

                    return _parse_text_value(str(note), f"Column notes[{column}]")
    return None


def declared_column_notes(document: SourceDocument) -> tuple[tuple[str, MetadataText], ...]:
    """Every column that carries a note, in declared order, plus the identity.

    This is the whole of what the catalogue's column dictionary describes: the
    columns an author said something about, and Weaver's own surrogate, which is
    given a generic note because no author writes one. Ordinals, types and
    nullability are physical and are recorded elsewhere.
    """

    ses = document.document
    notes: list[tuple[str, MetadataText]] = []
    if ses.identity is not None and ses.identity_column is not None:
        notes.append((ses.identity, MetadataText(literal=IDENTITY_COLUMN_NOTE)))
    if ses.has_declared_schema:
        notes.extend(
            (column.name, column.note) for column in ses.schema if column.note is not None
        )
        return tuple(notes)
    raw = ses.raw.get("Column notes") or {}
    if isinstance(raw, dict):
        from .metadata import _parse_text_value

        notes.extend(
            (str(name).strip(), _parse_text_value(str(note), f"Column notes[{name}]"))
            for name, note in raw.items()
        )
    return tuple(notes)
