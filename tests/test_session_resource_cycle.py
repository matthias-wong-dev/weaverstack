"""A Session resource is acquired once while healthy, and dies only deliberately.

These are the two rules the whole Session rests on. If concurrent callers can
start two acquisitions, a capacity with one Spark slot deadlocks against itself;
if a failed statement destroys the resource that ran it, every reported data
error costs a minute of startup and the console appears to hang after each
mistake.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from weaver.sessions.resources import Resource, ResourceError, ResourceState
from weaver.sessions.telemetry import SessionTelemetry


@pytest.fixture
def executor():
    with ThreadPoolExecutor(max_workers=4) as pool:
        yield pool


def test_a_resource_is_not_acquired_until_it_is_asked_for(executor):
    calls = []
    resource = Resource("livy", lambda: calls.append(1), executor=executor)

    assert resource.state is ResourceState.NOT_STARTED
    assert calls == []

    resource.get()

    assert resource.state is ResourceState.READY
    assert calls == [1]


def test_concurrent_callers_share_one_acquisition(executor):
    started = threading.Event()
    release = threading.Event()
    acquisitions = []

    def acquire():
        acquisitions.append(1)
        started.set()
        release.wait(5)
        return "livy"

    resource = Resource("livy", acquire, executor=executor)

    # A background warm-up, of the kind `weaver session` starts at the prompt.
    resource.start()
    assert started.wait(5)

    # And a command that needs Spark while that warm-up is still running.
    waiting = [executor.submit(resource.get) for _ in range(3)]
    release.set()

    assert [future.result(5) for future in waiting] == ["livy"] * 3
    assert acquisitions == [1], "a second acquisition would queue behind the first"
    assert resource.attempts == 1


def test_a_statement_failure_leaves_the_resource_alone(executor):
    resource = Resource("livy", lambda: "livy", executor=executor)
    resource.get()

    # What a failed Spark statement does to the session that ran it: nothing.
    assert resource.state is ResourceState.READY
    assert resource.get() == "livy"


def test_a_failed_resource_refuses_further_use_until_it_is_reacquired(executor):
    acquisitions = []

    def acquire():
        acquisitions.append(1)
        return f"livy-{len(acquisitions)}"

    resource = Resource("livy", acquire, executor=executor)
    assert resource.get() == "livy-1"

    resource.fail(RuntimeError("the session died"))

    assert resource.state is ResourceState.FAILED
    with pytest.raises(ResourceError, match="failed"):
        resource.get()

    resource.reacquire()
    assert resource.get() == "livy-2"


def test_recovery_is_bounded_rather_than_endless(executor):
    resource = Resource("livy", lambda: "livy", executor=executor, max_attempts=2)
    resource.get()
    resource.fail(RuntimeError("dead"))
    resource.reacquire()
    resource.get()
    resource.fail(RuntimeError("dead again"))

    with pytest.raises(ResourceError, match="failed 2 times"):
        resource.reacquire()


def test_a_warm_up_nobody_asked_for_does_not_fail_the_next_command(executor):
    attempts = []

    def acquire():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("the capacity was busy")
        return "livy"

    resource = Resource("livy", acquire, executor=executor)

    # The prompt's speculative warm-up, which fails for a passing reason.
    with pytest.raises(RuntimeError):
        resource.start(speculative=True).result(5)

    assert resource.state is ResourceState.NOT_STARTED

    # The command that genuinely needs Spark tries again, rather than inheriting
    # a failure from something it never asked for.
    assert resource.get() == "livy"
    assert resource.attempts == 1


def test_a_failed_warm_up_still_reports_through_the_command_that_needed_it(executor):
    def acquire():
        raise RuntimeError("this workspace names no environment")

    resource = Resource("livy", acquire, executor=executor)
    with pytest.raises(RuntimeError):
        resource.start(speculative=True).result(5)

    with pytest.raises(RuntimeError, match="names no environment"):
        resource.get()


def test_an_acquisition_that_fails_reports_its_own_cause(executor):
    def acquire():
        raise RuntimeError("no capacity")

    resource = Resource("livy", acquire, executor=executor)

    with pytest.raises(RuntimeError, match="no capacity"):
        resource.get()
    assert resource.state is ResourceState.FAILED


def test_close_releases_only_what_was_acquired(executor):
    released = []
    never = Resource(
        "unused", lambda: "value", executor=executor, release=released.append
    )
    used = Resource("used", lambda: "value", executor=executor, release=released.append)
    used.get()

    never.close()
    used.close()

    assert released == ["value"]
    assert used.state is ResourceState.CLOSED
    with pytest.raises(ResourceError, match="closed"):
        used.get()


def test_acquisition_is_timed_where_a_session_can_see_it(executor):
    telemetry = SessionTelemetry()
    resource = Resource("livy", lambda: "livy", executor=executor, telemetry=telemetry)
    resource.get()

    assert telemetry.measures["livy.acquire"].calls == 1


# --- leaving while something is still starting -------------------------------


def test_closing_mid_acquisition_waits_so_it_can_release_what_arrives(executor):
    """The expensive thing to leak is a Spark session on a one-slot capacity.

    ``weaver session`` warms Livy at the prompt; a user who exits immediately
    would otherwise abandon a session that is still starting, and it holds the
    only slot until Fabric reaps it — so the next run queues behind a session
    nobody is using.
    """

    release = threading.Event()
    released = []

    def acquire():
        release.wait(5)
        return "livy"

    resource = Resource("livy", acquire, executor=executor, release=released.append)
    resource.start(speculative=True)

    closing = executor.submit(resource.close)
    release.set()
    closing.result(5)

    assert released == ["livy"], "what arrived was released, not abandoned"
    assert resource.state is ResourceState.CLOSED


def test_an_acquisition_that_never_finishes_is_abandoned_rather_than_hanging(executor):
    """Bounded on purpose: an exit that cannot complete is the worse failure."""

    forever = threading.Event()
    released = []

    def acquire():
        forever.wait(30)
        return "livy"

    resource = Resource(
        "livy",
        acquire,
        executor=executor,
        release=released.append,
        close_timeout=0.2,
    )
    resource.start(speculative=True)

    resource.close()

    assert released == [], "nothing had arrived to release"
    assert resource.state is ResourceState.CLOSED
    forever.set()


def test_closing_before_anything_started_waits_for_nothing(executor):
    started = []
    resource = Resource("livy", lambda: started.append(1), executor=executor)

    resource.close()

    assert started == [], "a resource nobody asked for is not acquired to close it"
