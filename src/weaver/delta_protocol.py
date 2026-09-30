"""Weaver defaults for the protocol of newly created Delta tables."""

from typing import Any, Mapping, Sequence, TypedDict

DeltaColumns = Sequence[Sequence[Any]]
ProtocolMinima = Mapping[str, int]
DirectDeltaAction = (
    tuple[str, str, DeltaColumns, str | None]
    | tuple[str, str, DeltaColumns, str | None, ProtocolMinima]
)
SparkDeltaAction = (
    tuple[str, str, DeltaColumns, str | None, bool]
    | tuple[str, str, DeltaColumns, str | None, bool, ProtocolMinima]
)


class ProtocolOptions(TypedDict, total=False):
    protocol_minima: ProtocolMinima


DEFAULT_MIN_READER_VERSION = 3
DEFAULT_MIN_WRITER_VERSION = 7


def resolve_protocol_minima(
    authored: Mapping[str, int] | None = None,
) -> dict[str, int]:
    """Resolve creation policy independently of declaration signatures."""
    values = {
        "minReaderVersion": DEFAULT_MIN_READER_VERSION,
        "minWriterVersion": DEFAULT_MIN_WRITER_VERSION,
    }
    if authored is not None:
        if set(authored) - set(values):
            raise ValueError("unknown Delta protocol minimum")
        values.update(authored)
    for key, maximum in (("minReaderVersion", 3), ("minWriterVersion", 7)):
        if type(values[key]) is not int or not 1 <= values[key] <= maximum:
            raise ValueError(f"Delta {key} must be an integer from 1 to {maximum}")
    if values["minReaderVersion"] >= 3 or values["minWriterVersion"] > 5:
        values = {"minReaderVersion": 3, "minWriterVersion": 7}
    return values
