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
#: The Environment the session attaches to — installed once with `weaver install`
#: and consumed by the suite, never uploaded by it.
ENVIRONMENT_ENV = "WEAVER_FABRIC_ENVIRONMENT"
DEFAULT_ENVIRONMENT = "weaver"

#: Disposable items carry this prefix so an abandoned one is obvious.
TEST_PREFIX = "weavertest"


@pytest.fixture(scope="session")
def fabric_workspace():
    """The workspace named by WEAVER_FABRIC_WORKSPACE."""

    pytest.importorskip("azure.identity", reason="install the [fabric] extra")
    pytest.importorskip("requests", reason="install the [fabric] extra")

    # Credential choice is caller policy, not core's; the test infra is a caller.
    from weaver.fabric.auth import prefer_cli_credential

    prefer_cli_credential()

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
# Session-scoped, because a Lakehouse and a Livy session are expensive to obtain
# and cheap to reuse. This is the third execution position: not Weaver reaching
# into a workspace, but Weaver running there.
#
# Weaver is *installed* into a Fabric Environment beforehand with `weaver
# install`; the suite attaches that Environment and imports it. It never uploads
# Weaver source, copies it into /tmp, or edits sys.path.


@pytest.fixture(scope="session")
def fabric_weaver_lakehouse(fabric_workspace, fabric_client):
    """One Lakehouse standing in as the Weaver Lakehouse for the whole run.

    The Livy session is created against it; Weaver itself comes from the
    attached Environment, not from here.
    """

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
def fabric_environment_name():
    return os.environ.get(ENVIRONMENT_ENV, DEFAULT_ENVIRONMENT)


@pytest.fixture(scope="session")
def fabric_host(fabric_workspace, fabric_weaver_lakehouse, fabric_environment_name):
    """A host that names the Environment Weaver was installed into."""

    from weaver import FabricHost

    return FabricHost(
        workspace=fabric_workspace.name,
        weaver_lakehouse=fabric_weaver_lakehouse.name,
        fabric_environment=fabric_environment_name,
    )


@pytest.fixture(scope="session")
def livy_session(fabric_host):
    """One Spark session in Fabric with the Weaver Environment attached.

    Skips — rather than fails — when the Environment is missing or carries no
    usable Weaver, because that means ``weaver install`` has not been run, which
    is a setup step, not a defect in what is under test.
    """

    from weaver.errors import CommandError
    from weaver.fabric import LivyError, LivySession

    try:
        session = LivySession.for_host(fabric_host)
    except CommandError as exc:
        pytest.skip(f"{exc}; run `weaver install` into the Environment first")
    try:
        session.start()
    except LivyError as exc:
        pytest.skip(f"could not start a Livy session (Environment installed?): {exc}")
    try:
        yield session
    finally:
        session.close()
