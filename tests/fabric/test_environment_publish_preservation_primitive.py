"""Environment publication preserves user-owned state and settles to a no-op."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from support.weaver_test import weaver_test


def _user_libraries(libraries: dict) -> list[dict]:
    """Return every published library outside Weaver's filename ownership."""

    from weaver.fabric.environment import is_weaver_wheel

    found = [
        entry
        for entry in libraries.get("libraries", ())
        if not (
            str(entry.get("libraryType") or "").casefold() == "custom"
            and is_weaver_wheel(str(entry.get("name") or ""))
        )
    ]
    return sorted(found, key=lambda entry: json.dumps(entry, sort_keys=True))


def _published_environment_yml(client, environment) -> bytes:
    """Export the published user-owned external-library declaration."""

    path = (
        f"workspaces/{environment.workspace_id}/environments/{environment.id}/"
        "libraries/exportExternalLibraries"
    )
    return client.request("GET", path, expected=(200,)).content


@weaver_test(remote=True)
@pytest.mark.slow
def test_publish_preserves_user_environment_state_and_is_idempotent(
    fabric_workspace, fabric_client
):
    """One live publication keeps user state and the next changes nothing.

    ``dev=True``, because the suite's Environment carries a Weaver wheel built
    from a checkout. Released mode is the other contract: it would remove that
    wheel and install Weaver from PyPI, which is a different Environment.
    """

    from weaver.fabric.environment import (
        ENVIRONMENT,
        find_workspace,
        is_weaver_wheel,
        publish_environment,
        read_published,
        read_published_spark_compute,
    )
    from weaver.fabric.resources import find_item

    environment = find_item(
        find_workspace(fabric_workspace.workspace, client=fabric_client),
        fabric_workspace.environment.name,
        item_type=ENVIRONMENT,
        client=fabric_client,
    )
    before_libraries = read_published(environment, client=fabric_client)
    before_user_libraries = _user_libraries(before_libraries)
    before_compute = read_published_spark_compute(environment, client=fabric_client)
    before_yml = _published_environment_yml(fabric_client, environment)
    assert before_user_libraries, "the acceptance Environment needs a user package"

    root = Path(__file__).resolve().parents[2]
    first = publish_environment(
        fabric_workspace.workspace,
        fabric_workspace.environment,
        dev=True,
        client=fabric_client,
        root=root,
    )

    after_libraries = read_published(environment, client=fabric_client)
    assert _user_libraries(after_libraries) == before_user_libraries
    assert read_published_spark_compute(environment, client=fabric_client) == (
        before_compute
    )
    assert _published_environment_yml(fabric_client, environment) == before_yml
    assert first.mode == "dev"
    assert first.weaver_requirement is None, "a dev publication installs no PyPI Weaver"
    assert first.wheel_filename in {
        str(entry.get("name") or "")
        for entry in after_libraries.get("libraries", ())
        if str(entry.get("libraryType") or "").casefold() == "custom"
        and is_weaver_wheel(str(entry.get("name") or ""))
    }

    second = publish_environment(
        fabric_workspace.workspace,
        fabric_workspace.environment,
        dev=True,
        client=fabric_client,
        root=root,
    )
    assert second.publish_status == "AlreadyInstalled"
    assert second.published is False
    assert second.action == "unchanged"
    assert second.mode == "dev"
    assert second.removed_wheels == ()
