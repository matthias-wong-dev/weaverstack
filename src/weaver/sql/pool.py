"""Small, bounded, thread-safe SQL connection pools."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

from .authentication import SqlAuthentication
from .connection import SqlEndpoint, connect
from .errors import SqlPoolClosedError

DEFAULT_MAX_CONNECTIONS = 4
ConnectionFactory = Callable[[SqlEndpoint, SqlAuthentication], Any]


@dataclass
class SqlConnectionLease:
    """One exclusively leased physical connection."""

    connection: Any
    usable: bool = True

    def discard(self) -> None:
        """Prevent this connection from returning to the idle pool."""

        self.usable = False


class SqlConnectionPool:
    """Bounded connection reuse owned by one workflow or Fabric session."""

    def __init__(
        self,
        endpoint: SqlEndpoint,
        authentication: SqlAuthentication,
        *,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        connection_factory: ConnectionFactory = connect,
    ) -> None:
        if max_connections < 1:
            raise ValueError("max_connections must be at least one")
        self.endpoint = endpoint
        self.authentication = authentication
        self.max_connections = max_connections
        self._connection_factory = connection_factory
        self._condition = threading.Condition()
        self._idle: list[Any] = []
        self._physical_count = 0
        self._closed = False

    @contextmanager
    def lease(self) -> Iterator[SqlConnectionLease]:
        """Lease one connection exclusively, waiting when the pool is full."""

        connection = self._acquire()
        lease = SqlConnectionLease(connection)
        try:
            yield lease
        finally:
            self._release(lease)

    def _acquire(self):
        create = False
        with self._condition:
            while True:
                if self._closed:
                    raise SqlPoolClosedError(f"SQL pool for {self.endpoint} is closed")
                if self._idle:
                    return self._idle.pop()
                if self._physical_count < self.max_connections:
                    # Reserve the slot before opening outside the lock.
                    self._physical_count += 1
                    create = True
                    break
                self._condition.wait()

        if create:
            try:
                return self._connection_factory(self.endpoint, self.authentication)
            except Exception:
                with self._condition:
                    self._physical_count -= 1
                    self._condition.notify()
                raise
        raise AssertionError("unreachable")

    def _release(self, lease: SqlConnectionLease) -> None:
        close = False
        with self._condition:
            if self._closed or not lease.usable:
                self._physical_count -= 1
                close = True
            else:
                self._idle.append(lease.connection)
            self._condition.notify()
        if close:
            _close(lease.connection)

    def close(self) -> None:
        """Close idle connections; leased connections close when returned."""

        with self._condition:
            if self._closed:
                return
            self._closed = True
            idle, self._idle = self._idle, []
            self._physical_count -= len(idle)
            self._condition.notify_all()
        for connection in idle:
            _close(connection)

    def __enter__(self) -> "SqlConnectionPool":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False


class SqlPoolRegistry:
    """One endpoint-keyed pool registry for an owning execution context."""

    def __init__(
        self,
        *,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        connection_factory: ConnectionFactory = connect,
    ) -> None:
        self.max_connections = max_connections
        self.connection_factory = connection_factory
        self._lock = threading.Lock()
        self._pools: dict[tuple[str, str, str, str, int], SqlConnectionPool] = {}
        self._closed = False

    def pool(
        self, endpoint: SqlEndpoint, authentication: SqlAuthentication
    ) -> SqlConnectionPool:
        with self._lock:
            if self._closed:
                raise SqlPoolClosedError("SQL pool registry is closed")
            key = endpoint.pool_key
            if key not in self._pools:
                self._pools[key] = SqlConnectionPool(
                    endpoint,
                    authentication,
                    max_connections=self.max_connections,
                    connection_factory=self.connection_factory,
                )
            return self._pools[key]

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pools, self._pools = tuple(self._pools.values()), {}
        for pool in pools:
            pool.close()

    def __enter__(self) -> "SqlPoolRegistry":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False


def _close(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass
