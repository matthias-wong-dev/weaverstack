"""The repository content Weaver itself contributes, as checked-in files.

.. code-block:: text

    catalogue/            the Warehouse/_weaver catalogue declaration
    standard/<ItemType>/  what every normal item of that type receives
    browser/              the Catalogue Browser's Power BI project

The first two are item-relative and read by
:func:`weaver.declaration.repository.read_repository_fragment`; ``browser`` is
project-relative and read by the Power BI project readers. Each goes through
the same readers as an authored tree, so what a build composes is what is
reviewed here.
"""

from __future__ import annotations

from importlib.resources import files as resource_files

#: The fragment holding Weaver's catalogue declaration.
CATALOGUE = "catalogue"
#: The fragment holding the Catalogue Browser's Power BI project.
BROWSER = "browser"


def fragment_files(name: str) -> dict[str, bytes]:
    """One fragment's files, keyed by their item-relative path."""

    found: dict[str, bytes] = {}

    def walk(traversable, prefix: str) -> None:
        for entry in sorted(traversable.iterdir(), key=lambda each: each.name):
            relative = f"{prefix}/{entry.name}" if prefix else entry.name
            if entry.is_dir():
                if entry.name == "__pycache__":
                    continue
                walk(entry, relative)
            else:
                found[relative] = entry.read_bytes()

    walk(resource_files(__name__).joinpath(*name.split("/")), "")
    return found


def standard_fragment(item_type: str) -> dict[str, bytes]:
    """What every normal item of one type receives, by item-relative path."""

    return fragment_files(f"standard/{item_type}")


__all__ = ["BROWSER", "CATALOGUE", "fragment_files", "standard_fragment"]
