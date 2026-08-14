"""The one value a frozen payload cannot carry literally.

Object and schema names are rendered when the bundle is generated, so the only
substitution left is the instant an installation published its Registry.
"""

from __future__ import annotations

import re

from ..errors import InstallError

#: ``{{epoch}}`` — the instant this installation published its Registry.
#:
#: A token rather than a literal frozen at generation time: a rendered clock
#: would make the same repository produce different payload bytes on every run,
#: and a bundle's identity is its bytes. One install writes rows for several
#: items against several targets, and they all carry the same instant, or two
#: rows published by one build would order against each other.
EPOCH = re.compile(r"\{\{epoch\}\}")

#: The payload spelling of the publication epoch.
EPOCH_TOKEN = "{{epoch}}"


def substitute_epoch(text: str, epoch: str | None) -> str:
    """Resolve ``{{epoch}}`` to one installation's publication instant."""

    if not EPOCH.search(text):
        return text
    if epoch is None:
        raise InstallError(
            "a statement names {{epoch}} but this installation supplied none, so "
            "the row it writes could not be dated"
        )
    return EPOCH.sub(epoch.replace("\\", "\\\\"), text)


__all__ = ["EPOCH", "EPOCH_TOKEN", "substitute_epoch"]
