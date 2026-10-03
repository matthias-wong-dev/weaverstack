"""A dead Livy fails its Task, and the next Task gets a live one.

A failed resource is reacquired at a Task boundary, never part way through one,
and only a bounded number of times in a row, so a resource that cannot come back
says so rather than making every command pay for the discovery. A session Fabric
ended while idle is not a failed resource: ``LivySession`` replaces it in place
(``tests/test_livy_session_recovery_boundary.py``).
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.sessions.resources import Resource, ResourceError, ResourceState


class _Flaky:
    """A resource that can be acquired a stated number of times."""

    def __init__(self):
        self.acquired = 0

    def __call__(self):
        self.acquired += 1
        return f"session-{self.acquired}"


def _resource(acquire=None, **kwargs) -> Resource:
    from concurrent.futures import ThreadPoolExecutor

    return Resource(
        name="livy",
        acquire=acquire or _Flaky(),
        executor=ThreadPoolExecutor(max_workers=1),
        **kwargs,
    )


class _Session:
    """A Session holding one scope, with the frame machinery under test."""

    def __init__(self, resource):
        from weaver.sessions.console import ConsoleSession

        self.session = ConsoleSession(progress=False)
        self.scope = _Scope(resource)
        self.session._scopes[("test",)] = self.scope


class _Scope:
    def __init__(self, resource):
        self._resources = [resource]

    def recover(self):
        from weaver.sessions.base import WorkspaceScope

        WorkspaceScope.recover(self)


# --- the resource's own contract ----------------------------------------------


@weaver_test()
def test_a_failed_resource_stays_failed_until_something_asks_again():
    """Nothing self-heals. That is the property the run depends on."""

    resource = _resource()
    resource.get()
    resource.fail(RuntimeError("Livy session entered state 'dead'"))

    assert resource.state is ResourceState.FAILED
    with pytest.raises(ResourceError):
        resource.get()


@weaver_test()
def test_reacquiring_gives_a_new_one():
    resource = _resource()
    first = resource.get()
    resource.fail(RuntimeError("dead"))

    resource.reacquire()

    assert resource.get() != first


@weaver_test()
def test_the_allowance_is_bounded():
    """A resource that will not come back says so, rather than making every
    command pay to find out again."""

    resource = _resource(_refuse, max_attempts=2)
    with pytest.raises(RuntimeError):
        resource.get()
    resource.reacquire()
    with pytest.raises(RuntimeError):
        resource.get()

    with pytest.raises(ResourceError, match="cannot be acquired again"):
        resource.reacquire()


def _refuse():
    raise RuntimeError("no capacity")


# --- where recovery happens ---------------------------------------------------


@weaver_test()
def test_a_task_boundary_reacquires_what_died():
    resource = _resource()
    holder = _Session(resource)
    resource.get()
    resource.fail(RuntimeError("Livy session entered state 'dead'"))

    with holder.session.task("Load"):
        assert resource.state is not ResourceState.FAILED
        assert resource.get() == "session-2"


@weaver_test()
def test_nothing_is_reacquired_part_way_through_a_task():

    resource = _resource()
    holder = _Session(resource)

    with holder.session.task("Load"):
        resource.get()
        resource.fail(RuntimeError("Livy session entered state 'dead'"))

        with holder.session.step("Execute"):
            assert resource.state is ResourceState.FAILED
        with holder.session.substep("DWG.Customer"):
            assert resource.state is ResourceState.FAILED

        assert resource.state is ResourceState.FAILED


@weaver_test()
def test_the_next_task_recovers_after_one_has_failed():
    """The whole lifecycle: a Task dies on a dead resource, the next runs."""

    resource = _resource()
    holder = _Session(resource)
    resource.get()

    with pytest.raises(RuntimeError):
        with holder.session.task("Load"):
            resource.fail(RuntimeError("Livy session entered state 'dead'"))
            raise RuntimeError("the load failed on a dead session")

    with holder.session.task("Load"):
        assert resource.get() == "session-2"


@weaver_test()
def test_a_task_still_starts_when_the_allowance_is_spent():
    """Exhausted is the *user's* problem to hear about from the thing that
    needed it, naming what it was for, not a Task that refuses to begin."""

    resource = _resource(_refuse, max_attempts=1)
    holder = _Session(resource)
    with pytest.raises(RuntimeError):
        resource.get()

    with holder.session.task("Load"):
        assert resource.state is ResourceState.FAILED
        with pytest.raises(ResourceError):
            resource.get()


@weaver_test()
def test_a_healthy_resource_is_untouched_by_a_task_boundary():
    resource = _resource()
    holder = _Session(resource)
    first = resource.get()

    with holder.session.task("Load"):
        pass

    assert resource.get() == first
