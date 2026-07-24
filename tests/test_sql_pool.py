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
