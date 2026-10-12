"""Read-only sentries at the Session and external transport boundaries."""

from weaver.fabric.client import FabricClient
from weaver.fabric.onelake import OneLakeDfsClient
from weaver.fabric.store import FabricStore


def guard_preview(session, monkeypatch):
    reads = []
    forbidden_calls = []

    def forbidden(*args, **kwargs):
        forbidden_calls.append("write_or_dispatch")
        raise AssertionError(
            "Build preview reached a write or action-dispatch boundary"
        )

    for name in (
        "execute_mutation",
        "execute_mutation_in_fabric",
        "execute_run",
        "execute_run_in_fabric",
        "execute_python",
        "execute_tsql",
        "create_delta_table",
        "create_delta_table_actions",
        "create_direct_delta_table",
        "create_direct_delta_table_actions",
        "flusher",
    ):
        if hasattr(session, name):
            monkeypatch.setattr(session, name, forbidden)
    for kind in (OneLakeDfsClient, FabricStore):
        for name in ("write", "delete", "make_directory", "copy", "move"):
            if hasattr(kind, name):
                monkeypatch.setattr(kind, name, forbidden)

    request = FabricClient.request

    def read_rest(self, method, path, **kwargs):
        if method.upper() not in {"GET", "HEAD"}:
            return forbidden()
        reads.append({"kind": "rest", "method": method, "path": path})
        return request(self, method, path, **kwargs)

    monkeypatch.setattr(FabricClient, "request", read_rest)

    def protect(name, many=False):
        original = getattr(session, name)

        def read(statements, *args, **kwargs):
            values = list(statements) if many else [statements]
            for statement in values:
                if statement.strip().split()[0].upper() not in {
                    "SELECT",
                    "SHOW",
                    "DESCRIBE",
                }:
                    return forbidden()
                reads.append({"kind": name, "statement": statement})
            return original(statements, *args, **kwargs)

        monkeypatch.setattr(session, name, read)

    for name, many in (
        ("query_tsql", False),
        ("query_tsql_sets", True),
        ("execute_spark_sql", False),
        ("execute_spark_sql_batch", True),
    ):
        protect(name, many)
    return {"reads": reads, "forbidden_calls": forbidden_calls}
