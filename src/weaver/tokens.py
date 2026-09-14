"""Build-time tokens shared by catalogue T-SQL and Lakehouse Spark SQL."""

from __future__ import annotations

import re

from .errors import InstallError

#: Kept as a token so bundle bytes remain deterministic and every Registry row
#: published by one installation receives the same instant.
BUILD_DATETIME = re.compile(r"\{\{build_datetime\}\}")

#: The payload spelling of the publication instant.
BUILD_DATETIME_TOKEN = "{{build_datetime}}"


def substitute_build_datetime(text: str, build_datetime: str | None) -> str:
    """Resolve ``{{build_datetime}}`` to one installation's publication instant."""

    if not BUILD_DATETIME.search(text):
        return text
    if build_datetime is None:
        raise InstallError(
            "the installation has no build datetime required by a statement "
            "containing {{build_datetime}}"
        )
    return BUILD_DATETIME.sub(build_datetime.replace("\\", "\\\\"), text)


__all__ = ["BUILD_DATETIME", "BUILD_DATETIME_TOKEN", "substitute_build_datetime"]
