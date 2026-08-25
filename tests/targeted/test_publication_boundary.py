"""What the Warehouse to Lakehouse publication boundary proves before it lets go.

A Warehouse settles over TDS and OneLake publishes the table's Delta log after
the fact, so a downstream Lakehouse consumer can read either the previous
snapshot or one whose Parquet files it cannot open yet. These are the decisions
the boundary makes about that interval. What Fabric actually does with the wait
is proven against a real workspace.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.load_plan import OneLakeReadiness, PhysicalTargetRef
from weaver.locations import Location
from weaver.run.graph import RunNode
from weaver.run.publication import await_publication, published_commits
from weaver.run.resolution import WAREHOUSE_PROCEDURE
from weaver.run.result import RunError
from weaver.store import FilesystemStore

READINESS = (
    OneLakeReadiness(
        target=PhysicalTargetRef("lakehouse", "Published_LH"),
        schema="WH",
        object="Reporting",
    ),
)


def _estate(tmp_path, *, statements=None):
    """A Warehouse table's published log, and a Session that reaches it."""

    root = Location(str(tmp_path / "warehouse"))
    log = root.join("Tables", "Sales", "Customer", "_delta_log")
    store = FilesystemStore()
    store.make_directory(log)

    class Resolver:
        def resolve(self, item, *, item_type):
            return SimpleNamespace(id="warehouse-id", name=item.name)

        def external_root(self, _item):
            return root

        def lakehouse_spark_location(self, item):
            return SimpleNamespace(
                table_path=lambda schema, object: (
                    f"abfss://workspace/{item.name}/Tables/{schema}/{object}"
                )
            )

    class Session:
        def resolver(self, _workspace=None):
            return Resolver()

        def transport_store(self, _workspace=None):
            return store

        def execute_spark_sql_batch(self, batch, *, workspace=None):
            if statements is not None:
                statements.append(tuple(batch))
            return [{"rows": 1}]

    node = RunNode(
        node_id="load:Warehouse/Reporting_WH/Sales.Customer",
        physical_target=PhysicalTargetRef("warehouse", "Reporting_WH"),
        primitive_kind=WAREHOUSE_PROCEDURE,
        logical_id=WeaverDocumentId(
            WeaverItemId("Warehouse", "Reporting"), ObjectId("Sales", "Customer")
        ),
        await_onelake=READINESS,
    )
    return node, log, store, Session()


@weaver_test()
def test_a_table_with_no_published_log_has_no_commits_to_compare(tmp_path):
    """A table published for the first time starts from an empty baseline."""

    node, log, store, session = _estate(tmp_path)
    store.delete(log, recursive=True)

    assert published_commits(node, session, None) == frozenset()


@weaver_test()
def test_the_wait_opens_only_the_files_the_interval_added(tmp_path):
    """A file added and then removed inside the interval is never read."""

    statements: list = []
    node, log, store, session = _estate(tmp_path, statements=statements)
    store.write(
        log / "00000000000000000000.json", b'{"add":{"path":"settled.parquet"}}'
    )
    before = published_commits(node, session, None)
    store.write(
        log / "00000000000000000001.json",
        b'{"add":{"path":"kept.parquet"}}\n{"add":{"path":"gone.parquet"}}',
    )
    store.write(
        log / "00000000000000000002.json",
        b'{"remove":{"path":"gone.parquet"}}\n{"remove":{"path":"settled.parquet"}}',
    )

    await_publication(node, session, None, before=before, readiness=READINESS, poll=0.0)

    root = "abfss://workspace/Published_LH/Tables/WH/Reporting"
    assert statements == [(f"select * from parquet.`{root}/kept.parquet` limit 1",)]


@weaver_test()
def test_a_commit_that_publishes_no_files_needs_no_read(tmp_path):
    """Nothing to open means nothing to wait for."""

    statements: list = []
    node, log, store, session = _estate(tmp_path, statements=statements)
    before = published_commits(node, session, None)
    store.write(log / "00000000000000000000.json", b'{"commitInfo":{"operation":"X"}}')

    await_publication(node, session, None, before=before, readiness=READINESS, poll=0.0)

    assert statements == []


@weaver_test()
def test_a_publication_that_never_arrives_names_the_stale_snapshot(tmp_path):
    """No new commit is a freshness failure, not a readability one."""

    node, log, store, session = _estate(tmp_path)
    store.write(
        log / "00000000000000000000.json", b'{"add":{"path":"settled.parquet"}}'
    )
    before = published_commits(node, session, None)

    with pytest.raises(RunError) as raised:
        await_publication(
            node,
            session,
            None,
            before=before,
            readiness=READINESS,
            timeout=0.0,
            poll=0.0,
        )

    assert "published no new Delta commit" in str(raised.value)
    assert "previous snapshot" in str(raised.value)


@weaver_test()
def test_files_that_stay_unreadable_name_the_shortcut_that_cannot_open_them(tmp_path):
    """A commit that arrived and files that will not open is the other failure."""

    node, log, store, session = _estate(tmp_path)
    before = published_commits(node, session, None)
    store.write(log / "00000000000000000000.json", b'{"add":{"path":"kept.parquet"}}')

    class Refusing:
        def __getattr__(self, name):
            return getattr(session, name)

        def execute_spark_sql_batch(self, batch, *, workspace=None):
            raise PermissionError("403")

    with pytest.raises(RunError) as raised:
        await_publication(
            node,
            Refusing(),
            None,
            before=before,
            readiness=READINESS,
            timeout=0.0,
            poll=0.0,
        )

    assert "not readable through its Lakehouse shortcuts" in str(raised.value)
    assert "PermissionError: 403" in str(raised.value)
