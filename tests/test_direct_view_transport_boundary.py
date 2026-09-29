"""View publication guards the private stage and final OneLake file."""

from __future__ import annotations

import json
import threading
import time
import zlib
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.onelake import OneLakeDfsClient, onelake_url
from weaver.locations import Location
from weaver.sessions.console import ConsoleSession
from weaver.sessions.direct_view import VIEW_PROPERTIES
from weaver.store import StoreError
from weaver.workspaces import Workspace


class ViewStore(OneLakeDfsClient):
    def __init__(self):
        super().__init__(token="test-only", base_url="https://onelake.test")
        self.paths = {}
        self.calls = []
        self.fail_rename_after_move = False
        self.fail_append = False

    def _request(
        self, method, url, *, data=None, headers=None, expected=(200, 201, 202)
    ):
        headers = dict(headers or {})
        self.calls.append((method, url, headers))
        path = urlsplit(url).path
        query = urlsplit(url).query
        item = self.paths.get(path)
        if method == "HEAD":
            if item is None:
                return SimpleNamespace(status_code=404, headers={})
            return SimpleNamespace(
                status_code=200,
                headers={
                    "ETag": item["etag"],
                    "Content-Length": str(len(item["bytes"])),
                    "Content-Type": item["mime"],
                    "Content-Encoding": item["encoding"],
                    "x-ms-properties": item["properties"],
                },
            )
        if method == "GET":
            if item is None:
                raise StoreError("missing file")
            return SimpleNamespace(
                status_code=200, content=zlib.decompress(item["bytes"])
            )
        if method == "PUT" and query == "resource=file":
            assert headers["If-None-Match"] == "*"
            assert item is None
            self.paths[path] = {
                "etag": '"stage-created"',
                "bytes": b"",
                "mime": "application/octet-stream",
                "encoding": "deflate",
                "properties": headers["x-ms-properties"],
            }
            return SimpleNamespace(status_code=201, headers={})
        if method == "PATCH" and query.startswith("action=append"):
            if self.fail_append:
                raise StoreError("append failed")
            item["bytes"] = data
            item["etag"] = '"appended"'
            return SimpleNamespace(status_code=202, headers={})
        if method == "PATCH" and query.startswith("action=flush"):
            item["etag"] = '"flushed"'
            return SimpleNamespace(status_code=200, headers={})
        if method == "PATCH" and query == "action=setProperties":
            assert headers["If-Match"] == item["etag"]
            item.update(
                mime=headers["x-ms-content-type"],
                encoding=headers["x-ms-content-encoding"],
                properties=headers["x-ms-properties"],
                etag='"set"',
            )
            return SimpleNamespace(status_code=200, headers={})
        if method == "PUT" and "x-ms-rename-source" in headers:
            source = headers["x-ms-rename-source"]
            assert headers["If-None-Match"] == "*"
            assert headers["x-ms-source-if-match"] == self.paths[source]["etag"]
            assert item is None
            self.paths[path] = self.paths.pop(source)
            if self.fail_rename_after_move:
                raise StoreError("rename response lost")
            return SimpleNamespace(status_code=201, headers={})
        if method == "DELETE":
            assert headers["If-Match"] == item["etag"]
            del self.paths[path]
            return SimpleNamespace(status_code=200, headers={})
        raise AssertionError((method, url, headers))


def _locations():
    item = "11111111-2222-4333-8444-555555555555"
    return (
        Location(
            onelake_url(
                "workspace",
                item,
                "Files/weaver-view-stage-abc",
                base_url="https://onelake.test",
            )
        ),
        Location(
            onelake_url(
                "workspace", item, "Tables/Sales/View", base_url="https://onelake.test"
            )
        ),
    )


@weaver_test()
def test_encoded_view_publish_verifies_stage_and_final_before_success():
    store = ViewStore()
    stage, destination = _locations()
    decoded = b'{"tableType":"VIEW"}'
    snapshot = store.publish_view_file(
        stage, destination, decoded, properties=VIEW_PROPERTIES
    )
    assert snapshot.content == decoded
    assert snapshot.content_type == "application/json"
    assert snapshot.content_encoding == "deflate"
    assert snapshot.properties == VIEW_PROPERTIES
    assert len(store.paths) == 1
    assert urlsplit(store._url(destination)).path in store.paths
    assert not any(call[0] == "DELETE" for call in store.calls)
    assert any(
        call[0] == "PATCH" and "action=setProperties" in call[1] for call in store.calls
    )
    assert any(
        call[0] == "PUT" and call[2].get("x-ms-source-if-match") == '"set"'
        for call in store.calls
    )


@weaver_test()
def test_view_destination_collision_is_rejected_before_any_stage_mutation():
    store = ViewStore()
    stage, destination = _locations()
    store.paths[urlsplit(store._url(destination)).path] = {
        "etag": '"other"',
        "bytes": b"",
        "mime": "text/plain",
        "encoding": "",
        "properties": "",
    }
    with pytest.raises(StoreError, match="destination already exists"):
        store.publish_view_file(stage, destination, b"{}", properties=VIEW_PROPERTIES)
    assert len(store.calls) == 1 and store.calls[0][0] == "HEAD"
    assert not any(call[0] == "DELETE" for call in store.calls)


@weaver_test()
def test_lost_view_rename_response_reconciles_verified_file_without_spark_retry():
    store = ViewStore()
    store.fail_rename_after_move = True
    stage, destination = _locations()
    assert store.publish_view_file(
        stage, destination, b'{"tableType":"VIEW"}', properties=VIEW_PROPERTIES
    ).content
    assert (
        len(store.paths) == 1 and urlsplit(store._url(destination)).path in store.paths
    )


@weaver_test()
def test_prepublication_view_failure_cleans_only_its_owned_stage():
    store = ViewStore()
    store.fail_append = True
    stage, destination = _locations()
    with pytest.raises(StoreError, match="append failed"):
        store.publish_view_file(
            stage, destination, b'{"tableType":"VIEW"}', properties=VIEW_PROPERTIES
        )
    assert not store.paths
    assert [call[0] for call in store.calls if call[0] == "DELETE"] == ["DELETE"]


def _native_template():
    return {
        "tableType": "VIEW",
        "storage": {"compressed": False, "properties": {}},
        "allColumns": [
            {"name": "Value", "colType": '"long"', "nullable": False, "metadata": "{}"}
        ],
        "partitionColumnNames": [],
        "owner": "",
        "createTime": 1,
        "lastAccessTime": -1,
        "createVersion": "4.1.1.5.5.20260910.235373633",
        "viewText": "SELECT 1 AS Value",
        "viewOriginalText": "SELECT 1 AS Value",
        "unsupportedFeatures": [],
        "tracksPartitionsInCatalog": False,
        "schemaPreservesCase": True,
        "ignoredProperties": {},
        "properties": {
            "view.referredTempFunctionsNames": "[]",
            "view.referredTempVariablesNames": "[]",
            "view.referredTempViewNames": "[]",
            "view.catalogAndNamespace.numParts": "2",
            "view.catalogAndNamespace.part.0": "spark_catalog",
            "view.catalogAndNamespace.part.1": "opaque-live-target-namespace",
            "view.schemaMode": "COMPENSATION",
            "view.query.out.numCols": "1",
            "view.query.out.col.0": "Value",
            "view.sqlConfig.spark.sql.caseSensitive": "true",
        },
    }


def _view_session(store, *, unsupported=False, direct_view_workers=1):
    item = "11111111-2222-4333-8444-555555555555"
    root = Location(onelake_url("workspace", item, base_url=store.base_url))
    resolver = SimpleNamespace(
        configuration=SimpleNamespace(workspace="Work"),
        lakehouse=lambda ref: (
            root if ref.name == "Lake" else pytest.fail("wrong Lakehouse")
        ),
    )
    scope = SimpleNamespace(resolver=resolver, transport_store=store)
    session = ConsoleSession(
        workspace=Workspace(workspace="Work"),
        direct_view_workers=direct_view_workers,
    )
    session.scope = lambda _workspace=None: scope
    livy = object()
    session._foreground_livy = lambda _scope: livy
    calls = []

    def native(actions, **kwargs):
        calls.append(("spark", tuple(label for label, _ in actions)))
        for label, statement in actions:
            name = statement.split("`")[-2]
            schema = statement.split("`")[5]
            file = root.join("Tables", schema, name)
            template = _native_template()
            if unsupported:
                template["newRuntimeField"] = "drift"
            store.paths[urlsplit(store._url(file)).path] = {
                "etag": '"native"',
                "bytes": zlib.compress(json.dumps(template).encode()),
                "mime": "application/json",
                "encoding": "deflate",
                "properties": VIEW_PROPERTIES,
            }
        return [
            {
                "label": label,
                "succeeded": True,
                "started_after_seconds": float(i),
                "duration_seconds": 0.1,
            }
            for i, (label, _statement) in enumerate(actions)
        ]

    def analyse(actions, **kwargs):
        calls.append(("shape", tuple(label for label, _ in actions)))
        return [
            {
                "label": label,
                "succeeded": label != "bad_shape",
                "schema": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "Value",
                            "type": "long",
                            "nullable": False,
                            "metadata": {},
                        }
                    ],
                },
                "started_after_seconds": float(i),
                "duration_seconds": 0.1,
                **(
                    {
                        "error_type": "AnalysisException",
                        "error_message": "cannot analyse",
                    }
                    if label == "bad_shape"
                    else {}
                ),
            }
            for i, (label, _query) in enumerate(actions)
        ]

    session.execute_spark_sql_actions = native
    session.describe_spark_view_queries = analyse
    return session, calls, root


@weaver_test()
def test_desktop_session_uses_live_view_then_direct_files_and_shape_fallback():
    store = ViewStore()
    session, calls, root = _view_session(store)
    actions = [
        ("seed", "CREATE VIEW `Work`.`Lake`.`Sales`.`seed` AS\nSELECT 1 AS Value\n"),
        (
            "direct",
            "CREATE VIEW `Work`.`Lake`.`Sales`.`direct` AS\nSELECT 2 AS Value\n",
        ),
        (
            "bad_shape",
            "CREATE VIEW `Work`.`Lake`.`Sales`.`bad_shape` AS\nSELECT 3 AS Value\n",
        ),
        (
            "other_schema",
            "CREATE VIEW `Work`.`Lake`.`Other`.`other_schema` AS\nSELECT 4 AS Value\n",
        ),
    ]
    results = session.create_spark_view_actions(
        actions, workspace=Workspace(workspace="Work")
    )
    assert [result["label"] for result in results] == [label for label, _ in actions]
    assert all(result["succeeded"] for result in results)
    assert [result["view_route"] for result in results] == [
        "spark_template",
        "direct",
        "spark_fallback",
        "spark_template",
    ]
    assert calls == [
        ("spark", ("seed",)),
        ("shape", ("direct", "bad_shape")),
        ("spark", ("bad_shape",)),
        ("spark", ("other_schema",)),
    ]
    direct = json.loads(store.read(root.join("Tables", "Sales", "direct")))
    assert direct["viewText"] == "SELECT 2 AS Value"
    assert (
        direct["properties"]["view.catalogAndNamespace.part.1"]
        == "opaque-live-target-namespace"
    )
    session.close()


@weaver_test()
def test_console_accepts_bounded_direct_view_workers():
    session = ConsoleSession(
        workspace=Workspace(workspace="Work"), direct_view_workers=4
    )
    assert session.direct_view_workers == 4
    session.close()


@weaver_test()
@pytest.mark.parametrize("workers", [4, 8, 16])
def test_independent_view_file_publications_overlap_and_return_in_source_order(
    workers,
):
    class OverlapStore(ViewStore):
        def __init__(self):
            super().__init__()
            self.barrier = threading.Barrier(workers, timeout=5)
            self.lock = threading.Lock()
            self.active = 0
            self.peak = 0

        def publish_view_file(self, stage, destination, decoded, *, properties):
            with self.lock:
                self.active += 1
                self.peak = max(self.peak, self.active)
            try:
                self.barrier.wait()
                return super().publish_view_file(
                    stage, destination, decoded, properties=properties
                )
            finally:
                with self.lock:
                    self.active -= 1

    store = OverlapStore()
    session, calls, root = _view_session(store, direct_view_workers=workers)
    names = ["seed"] + [f"direct{i}" for i in range(workers)]
    actions = [
        (name, f"CREATE VIEW `Work`.`Lake`.`Sales`.`{name}` AS\nSELECT {i} AS Value\n")
        for i, name in enumerate(names)
    ]
    results = session.create_spark_view_actions(
        actions, workspace=Workspace(workspace="Work")
    )
    assert [one["label"] for one in results] == names
    assert [one["view_route"] for one in results] == ["spark_template"] + [
        "direct"
    ] * workers
    assert all(one["succeeded"] for one in results)
    assert store.peak == workers
    assert calls == [
        ("spark", ("seed",)),
        ("shape", tuple(names[1:])),
    ]
    for i, name in enumerate(names[1:], 1):
        assert (
            json.loads(store.read(root.join("Tables", "Sales", name)))["viewText"]
            == f"SELECT {i} AS Value"
        )
    session.close()


@weaver_test()
@pytest.mark.parametrize("workers", [0, 17, True, "4"])
def test_direct_view_worker_bound_rejects_invalid_values(workers):
    with pytest.raises(ValueError, match="direct View workers"):
        ConsoleSession(
            workspace=Workspace(workspace="Work"), direct_view_workers=workers
        )


@weaver_test()
def test_failed_view_publication_keeps_ordered_sibling_outcomes_without_retry():
    class FaultStore(ViewStore):
        def publish_view_file(self, stage, destination, decoded, *, properties):
            if destination.value.endswith("/beta"):
                raise StoreError("owned beta publication failed")
            return super().publish_view_file(
                stage, destination, decoded, properties=properties
            )

    store = FaultStore()
    session, calls, root = _view_session(store, direct_view_workers=4)
    names = ["seed", "alpha", "beta", "gamma", "delta"]
    actions = [
        (name, f"CREATE VIEW `Work`.`Lake`.`Sales`.`{name}` AS\nSELECT {i} AS Value\n")
        for i, name in enumerate(names)
    ]
    results = session.create_spark_view_actions(
        actions, workspace=Workspace(workspace="Work")
    )
    assert [one["label"] for one in results] == names
    assert [one["succeeded"] for one in results] == [True, True, False, True, True]
    assert results[2]["view_route"] == "direct"
    assert results[2]["error_type"] == "StoreError"
    assert calls == [
        ("spark", ("seed",)),
        ("shape", ("alpha", "beta", "gamma", "delta")),
    ]
    assert not store.exists(root.join("Tables", "Sales", "beta"))
    assert all(
        store.exists(root.join("Tables", "Sales", name))
        for name in ("seed", "alpha", "gamma", "delta")
    )
    session.close()


@weaver_test()
def test_view_shape_fallback_waits_for_direct_sibling_publications():
    class DrainStore(ViewStore):
        def __init__(self):
            super().__init__()
            self.lock = threading.Lock()
            self.active = 0
            self.barrier = threading.Barrier(3, timeout=3)

        def publish_view_file(self, stage, destination, decoded, *, properties):
            with self.lock:
                self.active += 1
            try:
                self.barrier.wait()
                time.sleep(0.03)
                return super().publish_view_file(
                    stage, destination, decoded, properties=properties
                )
            finally:
                with self.lock:
                    self.active -= 1

    store = DrainStore()
    session, calls, _root = _view_session(store, direct_view_workers=4)
    original = session.execute_spark_sql_actions

    def serial_spark(actions, **kwargs):
        with store.lock:
            assert store.active == 0
        return original(actions, **kwargs)

    session.execute_spark_sql_actions = serial_spark
    names = ["seed", "alpha", "bad_shape", "gamma", "delta"]
    actions = [
        (name, f"CREATE VIEW `Work`.`Lake`.`Sales`.`{name}` AS\nSELECT {i} AS Value\n")
        for i, name in enumerate(names)
    ]
    results = session.create_spark_view_actions(
        actions, workspace=Workspace(workspace="Work")
    )
    assert [one["label"] for one in results] == names
    assert [one["view_route"] for one in results] == [
        "spark_template",
        "direct",
        "spark_fallback",
        "direct",
        "direct",
    ]
    assert all(one["succeeded"] for one in results)
    assert calls == [
        ("spark", ("seed",)),
        ("shape", ("alpha", "bad_shape", "gamma", "delta")),
        ("spark", ("bad_shape",)),
    ]
    session.close()


@weaver_test()
def test_unrecognised_live_view_template_uses_spark_for_remaining_actions():
    store = ViewStore()
    session, calls, _root = _view_session(store, unsupported=True)
    actions = [
        ("seed", "CREATE VIEW `Work`.`Lake`.`Sales`.`seed` AS\nSELECT 1 AS Value\n"),
        ("next", "CREATE VIEW `Work`.`Lake`.`Sales`.`next` AS\nSELECT 2 AS Value\n"),
    ]
    results = session.create_spark_view_actions(
        actions, workspace=Workspace(workspace="Work")
    )
    assert [one["view_route"] for one in results] == [
        "spark_template",
        "spark_fallback",
    ]
    assert calls == [("spark", ("seed",)), ("spark", ("next",))]
    assert not [call for call in store.calls if call[0] == "PUT"]
    session.close()


@weaver_test()
def test_wrong_bound_view_namespace_fails_without_spark_or_onelake_write():
    store = ViewStore()
    session, calls, _root = _view_session(store)
    results = session.create_spark_view_actions(
        [("wrong", "CREATE VIEW `OtherWork`.`Lake`.`Sales`.`wrong` AS\nSELECT 1\n")],
        workspace=Workspace(workspace="Work"),
    )
    assert len(results) == 1 and not results[0]["succeeded"]
    assert not calls and not store.calls
    session.close()
