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


# --- running Weaver inside Fabric --------------------------------------------
#
# Session-scoped, because a Lakehouse, a runtime sync and a Livy session are all
# expensive to obtain and cheap to reuse. This is the third execution position:
# not Weaver reaching into a workspace, but Weaver running there.


@pytest.fixture(scope="session")
def fabric_weaver_lakehouse(fabric_workspace, fabric_client):
    """One Lakehouse standing in as the Weaver Lakehouse for the whole run."""

    from weaver.fabric import create_lakehouse, delete_item

    item = create_lakehouse(
        fabric_workspace, _disposable_name("home"), client=fabric_client
    )
    try:
        yield item
    finally:
        try:
            delete_item(item, client=fabric_client)
        except Exception as exc:
            print(f"warning: could not delete {item}: {exc}")


@pytest.fixture(scope="session")
def fabric_host(fabric_workspace, fabric_weaver_lakehouse):
    """A host whose Weaver Lakehouse is real, and which knows where Weaver goes."""

    from weaver import FabricHost

    return FabricHost(
        workspace=fabric_workspace.name,
        weaver_lakehouse=fabric_weaver_lakehouse.name,
        weaver_install=f"{fabric_weaver_lakehouse.name}/Files/weaver",
    )


@pytest.fixture(scope="session")
def synced_runtime(fabric_host):
    """This machine's Weaver package, shipped into the workspace.

    Paid once for the run. The upload is 62 KB of pure Python.
    """

    from weaver.fabric import sync_runtime

    return sync_runtime(fabric_host)


@pytest.fixture(scope="session")
def livy_session(fabric_host, synced_runtime):
    """One Spark session in Fabric, held open across the tests that need it."""

    from weaver.fabric import LivyError, LivySession

    session = LivySession.for_host(fabric_host)
    try:
        session.start()
    except LivyError as exc:
        pytest.skip(f"could not start a Livy session: {exc}")
    try:
        yield session
    finally:
        session.close()
