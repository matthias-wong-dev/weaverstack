"""One Session, one resolver, one answer per name — against a real workspace.

The cache must survive the operation that filled it. A Session owns one resolver
for its lifetime, so the second question for the same typed name is free.

Cheap on purpose: REST resolution only, no Livy session, so this proves the
ownership without competing for a capacity's one Spark slot.
"""

from __future__ import annotations

import pytest
from support.weaver_test import register_session, weaver_test

from weaver.errors import CommandError
from weaver.fabric.resources import LAKEHOUSE
from weaver.sessions import ConsoleSession
from weaver.targets import ItemRef


@pytest.fixture
def console(fabric_workspace):
    with ConsoleSession() as session:
        yield register_session(session)


@weaver_test(remote=True, resources={"rest"})
def test_a_session_resolves_a_real_item_by_workspace_type_and_name(
    console, fabric_workspace, fabric_target_lakehouse
):
    item = console.resolve_item(
        ItemRef(fabric_target_lakehouse.name),
        item_type=LAKEHOUSE,
        workspace=fabric_workspace,
    )

    assert item.id == fabric_target_lakehouse.id


@weaver_test(remote=True, resources={"rest"})
def test_the_same_name_is_not_asked_about_twice(
    console, fabric_workspace, fabric_target_lakehouse
):
    reference = ItemRef(fabric_target_lakehouse.name)

    first = console.resolve_item(
        reference, item_type=LAKEHOUSE, workspace=fabric_workspace
    )
    second = console.resolve_item(
        reference, item_type=LAKEHOUSE, workspace=fabric_workspace
    )

    assert first is second, "the second answer came from the workspace, not the cache"
    assert console.telemetry.counters.get("resolve.item.cache_hits") == 1


@weaver_test(remote=True, resources={"rest"})
def test_two_operations_in_one_session_share_the_cache(
    console, fabric_workspace, fabric_target_lakehouse
):
    # What two commands in one `weaver session` look like from here: whatever
    # each of them asks the Session for, they ask the same resolver.
    first = console.resolver(fabric_workspace)
    console.resolve_item(
        ItemRef(fabric_target_lakehouse.name),
        item_type=LAKEHOUSE,
        workspace=fabric_workspace,
    )
    second = console.resolver(fabric_workspace)

    assert first is second
    assert second.cache_hits == 0
    console.resolve_item(
        ItemRef(fabric_target_lakehouse.name),
        item_type=LAKEHOUSE,
        workspace=fabric_workspace,
    )
    assert second.cache_hits == 1


@weaver_test(remote=True)
def test_one_credential_serves_the_whole_session(console, fabric_workspace):
    scope = console.scope(fabric_workspace)
    scope.token_provider()

    # The Azure CLI is shelled out to by *constructing* a credential, so sharing
    # one is the saving; a provider per call would still be one credential.
    assert scope._credential is console.scope(fabric_workspace)._credential


@weaver_test(remote=True)
def test_a_closed_session_releases_its_workspace_scopes(fabric_workspace):
    session = ConsoleSession()
    session.scope(fabric_workspace)
    session.close()

    with pytest.raises(CommandError, match="closed"):
        session.scope(fabric_workspace)
