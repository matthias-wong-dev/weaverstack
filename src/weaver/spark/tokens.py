"""The one value a frozen payload cannot carry literally.

Object and schema names are rendered when the bundle is generated, so the only
substitution left is the instant an installation published its Registry.
"""

from __future__ import annotations

import re

from ..errors import InstallError

#: ``{{build_datetime}}`` — the instant this installation published its Registry.
#:
#: A token rather than a literal frozen at generation time: a rendered clock
#: would make the same repository produce different payload bytes on every run,
#: and a bundle's identity is its bytes. One install writes rows for several
#: items against several targets, and they all carry the same instant, or two
#: rows published by one build would order against each other.
BUILD_DATETIME = re.compile(r"\{\{build_datetime\}\}")

#: The payload spelling of the publication instant.
BUILD_DATETIME_TOKEN = "{{build_datetime}}"


def substitute_build_datetime(text: str, build_datetime: str | None) -> str:
    """Resolve ``{{build_datetime}}`` to one installation's publication instant."""

    if not BUILD_DATETIME.search(text):
        return text
    if build_datetime is None:
        raise InstallError(
            "a statement names {{build_datetime}} but this installation supplied "
            "none, so the row it writes could not be dated"
        )
    return BUILD_DATETIME.sub(build_datetime.replace("\\", "\\\\"), text)


__all__ = ["BUILD_DATETIME", "BUILD_DATETIME_TOKEN", "substitute_build_datetime"]
