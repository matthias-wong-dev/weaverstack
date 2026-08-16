"""A dead Livy fails its Task, and the next Task gets a live one.

The persistent Session made a real gap visible. Before it, each command opened
its own Livy session and closed it, so a session that died cost one command and
the next started fresh. With one session shared for a console's lifetime, a
death was permanent: `livy_run` marked the resource failed, `reacquire()` was
never called by anything, and every later command answered

    the livy resource failed and has not been reacquired

**Recovery belongs at a Task boundary, and nowhere else.** A `Resource.get()`
that healed itself would hand a *replacement* Livy interpreter to a run already
in progress — and that interpreter has none of the RuntimeScopes the run opened
in the dead one. The nodes would import into scopes that do not exist, and the
run would carry on succeeding at nothing. So the failure stands, the Task fails,
and the next Task acquires again.

Bounded, because a resource that cannot come back must eventually say so rather
than making every command pay for the discovery.
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


def _resource(**kwargs) -> Resource:
    from concurrent.futures import ThreadPoolExecutor

    return Resource(
        name="livy",
        acquire=_Flaky(),
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

    resource = _resource(max_attempts=2)
    resource.get()
    resource.fail(RuntimeError("dead"))
    resource.reacquire()
    resource.get()
    resource.fail(RuntimeError("dead again"))

    with pytest.raises(ResourceError, match="will not be acquired again"):
        resource.reacquire()


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
    """The claim the whole design rests on.

    A replacement interpreter has none of the RuntimeScopes the run opened in
    the dead one, so a run that continued on it would dispatch into scopes that
    do not exist — succeeding at nothing, silently.
    """

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
    needed it, naming what it was for — not a Task that refuses to begin."""

    resource = _resource(max_attempts=1)
    holder = _Session(resource)
    resource.get()
    resource.fail(RuntimeError("dead"))

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
