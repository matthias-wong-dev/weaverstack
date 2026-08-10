"""Bounded endpoint-specific SQL connection reuse."""

from __future__ import annotations

import threading

from weaver.sql import (
    AccessTokenAuthentication,
    SqlConnectionPool,
    SqlEndpoint,
    SqlPoolRegistry,
)


class Connection:
    def __init__(self, number):
        self.number = number
        self.closed = False

    def close(self):
        self.closed = True


AUTH = AccessTokenAuthentication(lambda: "token")


def test_connections_are_reused_and_closed_with_the_pool():
    created = []

    def factory(endpoint, authentication):
        connection = Connection(len(created))
        created.append(connection)
        return connection

    endpoint = SqlEndpoint("one.example", "Reporting")
    pool = SqlConnectionPool(endpoint, AUTH, connection_factory=factory)

    with pool.lease() as first:
        first_connection = first.connection
    with pool.lease() as second:
        assert second.connection is first_connection

    pool.close()
    assert len(created) == 1
    assert created[0].closed


def test_an_active_lease_is_never_shared_and_the_bound_is_respected():
    created = []
    endpoint = SqlEndpoint("one.example", "Reporting")

    def factory(resolved, authentication):
        connection = Connection(len(created))
        created.append(connection)
        return connection

    pool = SqlConnectionPool(
        endpoint, AUTH, max_connections=2, connection_factory=factory
    )
    acquired_third = threading.Event()

    with pool.lease() as first, pool.lease() as second:
        assert first.connection is not second.connection

        def acquire():
            with pool.lease():
                acquired_third.set()

        thread = threading.Thread(target=acquire)
        thread.start()
        assert not acquired_third.wait(0.05)

    thread.join(timeout=1)
    assert acquired_third.is_set()
    assert len(created) == 2
    pool.close()


def test_discarded_connections_are_not_reused():
    created = []

    def factory(endpoint, authentication):
        connection = Connection(len(created))
        created.append(connection)
        return connection

    pool = SqlConnectionPool(
        SqlEndpoint("one.example", "Reporting"),
        AUTH,
        connection_factory=factory,
    )
    with pool.lease() as lease:
        first = lease.connection
        lease.discard()
    with pool.lease() as lease:
        second = lease.connection

    assert first.closed
    assert second is not first
    pool.close()


def test_a_registry_keeps_separate_pools_per_stable_endpoint():
    registry = SqlPoolRegistry()
    one = SqlEndpoint(
        "shared.example", "Reporting", workspace_id="ws", warehouse_id="one"
    )
    two = SqlEndpoint(
        "shared.example", "Reporting", workspace_id="ws", warehouse_id="two"
    )

    assert registry.pool(one, AUTH) is registry.pool(one, AUTH)
    assert registry.pool(one, AUTH) is not registry.pool(two, AUTH)
    registry.close()


# --- a connection dropped while it was idle ----------------------------------


class Droppable(Connection):
    """A connection that can be killed the way a server kills an idle one.

    Silently: nothing announces the drop, and the connection only reveals it
    when something is asked of it. That silence is the whole problem — a caller
    handed one of these fails on its own statement and reads the failure as
    being about the statement.
    """

    def __init__(self, number):
        super().__init__(number)
        self.alive = True
        self.statements = []

    def drop(self):
        self.alive = False

    def cursor(self):
        return Cursor(self)


class Cursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, statement, *parameters):
        if not self.connection.alive:
            raise OSError("Communication link failure")
        self.connection.statements.append(statement)

    def fetchall(self):
        return []

    def close(self):
        pass


def _droppable_pool(created, **kwargs):
    def factory(endpoint, authentication):
        connection = Droppable(len(created))
        created.append(connection)
        return connection

    return SqlConnectionPool(
        SqlEndpoint("one.example", "Reporting"),
        AUTH,
        connection_factory=factory,
        **kwargs,
    )


def test_a_connection_dropped_while_idle_is_replaced_rather_than_handed_out(
    monkeypatch,
):
    """The failure this prevents arrives as somebody else's fault.

    A Fabric SQL endpoint drops connections it considers abandoned and says
    nothing. Handed one, the next caller fails with a communication link
    failure — in a suite, that was one wipe failing and thirteen tests erroring
    behind it, none of which had anything wrong with them.
    """

    monkeypatch.setattr("weaver.sql.pool.IDLE_VALIDATION_SECONDS", 0.0)
    created = []
    pool = _droppable_pool(created)

    with pool.lease() as first:
        first.connection.execute = None  # unused; the lease is what matters
    created[0].drop()

    with pool.lease() as second:
        assert second.connection is not created[0], "a dead connection was reused"
        assert second.connection.alive

    assert created[0].closed, "the dead one was closed rather than leaked"


def test_a_connection_reused_promptly_is_not_checked(monkeypatch):
    """The check costs a round trip, so back-to-back work must not pay it."""

    monkeypatch.setattr("weaver.sql.pool.IDLE_VALIDATION_SECONDS", 3600.0)
    created = []
    pool = _droppable_pool(created)

    with pool.lease():
        pass
    with pool.lease() as second:
        assert second.connection is created[0]

    assert created[0].statements == [], "a healthy connection was interrogated"


def test_the_pool_does_not_shrink_when_it_replaces_a_dead_connection(monkeypatch):
    """Otherwise every drop would permanently cost the pool a slot."""

    monkeypatch.setattr("weaver.sql.pool.IDLE_VALIDATION_SECONDS", 0.0)
    created = []
    pool = _droppable_pool(created, max_connections=1)

    with pool.lease():
        pass
    created[0].drop()

    with pool.lease() as replacement:
        assert replacement.connection.alive
    # And the slot is still usable afterwards, which a leaked count would deny.
    with pool.lease() as again:
        assert again.connection.alive
