"""Shared public result representation."""

from __future__ import annotations

from typing import Any


class AsDict:
    """Give mapping-backed public results one canonical display method."""

    def to_mapping(self) -> dict[str, Any]:
        raise NotImplementedError

    def as_dict(self) -> dict[str, Any]:
        return self.to_mapping()
