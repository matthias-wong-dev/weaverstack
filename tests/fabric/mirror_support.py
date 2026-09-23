"""Helpers the Lakehouse and Warehouse mirror journeys share."""

from __future__ import annotations


def forked_config(run, directory):
    """A workspace configuration naming the fork and the catalogue it mirrors.

    ``mirror:`` reaches a Workspace from configuration alone, and it is what
    tells health where a mirrored object's load state is recorded.
    """

    path = directory / "workspace-config.yml"
    path.write_text(
        "\n".join(
            (
                f"workspace: {run.workspace.workspace}",
                f"catalogue: Warehouse/{run.catalogue_name}",
                f"mirror: Warehouse/{run.source_catalogue_name}",
            )
        ),
        encoding="utf-8",
    )
    return path
