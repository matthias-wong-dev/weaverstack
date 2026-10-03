"""Explicit logical Table/View references used by semantic tables."""

from ..declaration.model import WeaverDocumentId
from ..errors import ConfigError, WeaverError


def source_identity(reference: str) -> WeaverDocumentId:
    parts = reference.split("/") if isinstance(reference, str) else []
    if (
        len(parts) not in {3, 4}
        or any(not part or part != part.strip() for part in parts)
        or parts[0] not in {"Warehouse", "Lakehouse"}
        or (len(parts) == 4 and parts[:1] != ["Lakehouse"])
        or (len(parts) == 4 and parts[2] != "Tables")
        or len(parts[-1].split(".")) != 2
        or any(not p or p != p.strip() for p in parts[-1].split("."))
    ):
        raise ConfigError(
            f"Semantic source: expected a logical Table/View path, got {reference!r}"
        )
    if parts[0] == "Lakehouse" and len(parts) == 3:
        parts.insert(2, "Tables")
    try:
        identity = WeaverDocumentId.parse("/".join(parts))
    except WeaverError as exc:
        raise ConfigError(f"Semantic source {reference!r}: {exc}") from exc
    return identity
