"""Fixtures for opt-in Fabric integration tests.

These touch a real workspace and a running capacity, so they are deselected by
default and skip unless `WEAVER_FABRIC_WORKSPACE` names a workspace to use.

They create their own Lakehouses and delete them afterwards. Nothing
pre-existing in the workspace is touched, and the names are prefixed so a
leftover from an interrupted run is recognisable.
"""

from __future__ import annotations

import os
import uuid

import pytest

WORKSPACE_ENV = "WEAVER_FABRIC_WORKSPACE"

#: Disposable items carry this prefix so an abandoned one is obvious.
TEST_PREFIX = "weavertest"


@pytest.fixture(scope="session")
def fabric_workspace():
    """The workspace named by WEAVER_FABRIC_WORKSPACE."""

    pytest.importorskip("azure.identity", reason="install the [fabric] extra")
    pytest.importorskip("requests", reason="install the [fabric] extra")

    name = os.environ.get(WORKSPACE_ENV)
    if not name:
        pytest.skip(f"set {WORKSPACE_ENV} to run Fabric tests")

    from weaver.errors import WeaverError
    from weaver.fabric import find_workspace

    try:
        return find_workspace(name)
    except WeaverError as exc:
        pytest.skip(f"cannot reach workspace {name!r}: {exc}")


@pytest.fixture(scope="session")
def fabric_client(fabric_workspace):
    from weaver.fabric import FabricClient

    return FabricClient()


def _disposable_name(role: str) -> str:
    """A name no human would have chosen, so cleanup is unambiguous."""

    return f"{TEST_PREFIX}_{role}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def fabric_lakehouses(fabric_workspace, fabric_client):
    """A Weaver Lakehouse and a target Lakehouse, created and then deleted.

    The local equivalent of this fixture is `lakehouses`, and the pair are
    deliberately shaped the same so a test can be written against either.
    """

    from weaver.fabric import create_lakehouse, delete_item

    created = []
    try:
        weaver = create_lakehouse(
            fabric_workspace, _disposable_name("weaver"), client=fabric_client
        )
        created.append(weaver)
        target = create_lakehouse(
            fabric_workspace, _disposable_name("target"), client=fabric_client
        )
        created.append(target)
        yield {"workspace": fabric_workspace, "weaver": weaver, "target": target}
    finally:
        for item in created:
            try:
                delete_item(item, client=fabric_client)
            except Exception as exc:  # cleanup must not mask a test failure
                print(f"warning: could not delete {item}: {exc}")
