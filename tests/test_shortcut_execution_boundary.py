"""Creating, awaiting and removing OneLake shortcuts without holding a worker."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_resolver, given_workspace

from weaver.build_bundle.executors import ShortcutExecutor, ShortcutReadinessExecutor
from weaver.build_bundle.executors import shortcut as shortcut_module
from weaver.build_bundle.executors.base import (
    InstallationContext,
    ResolvedTarget,
    Waiting,
)
from weaver.build_bundle.models import CREATE_SHORTCUT, DROP_SHORTCUT, InstallAction
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import InstallError
from weaver.locations import LakehouseSparkLocation
from weaver.spark import FabricSparkTarget
from weaver.store import Entry, FilesystemStore
from weaver.targets import ItemRef

SOURCE_TARGET_ID = "Lakehouse-Raw--lakehouse-Raw_Dev"
DESTINATION_TARGET_ID = "Lakehouse-Curated--lakehouse-Curated_Dev"


def _payload(**overrides) -> bytes:
    """One shortcut, in the batched shape the action carries."""

    mapping = {
        "shortcut": "Lakehouse/Curated/Tables/Sales.Landed",
        "source": "Lakehouse/Raw/Tables/Sales.Customer",
        "source_target_id": SOURCE_TARGET_ID,
        "type": "table",
        "path": "Tables/Sales",
        "name": "Landed",
        "source_area": "Tables",
        "source_schema": "Sales",
        "source_object": "Customer",
    }
    mapping.update(overrides)
    return json.dumps({"shortcuts": [mapping]}).encode("utf-8")


def _target(target_id: str, item: str) -> ResolvedTarget:
    return ResolvedTarget(
        bound=BoundTarget(id=target_id, kind="lakehouse", item_id=item, item_name=item),
        lakehouse=ItemRef(item),
    )


def _action() -> InstallAction:
    return InstallAction(
        id="shortcuts-Lakehouse--Curated",
        kind=CREATE_SHORTCUT,
        resource_node_id=None,
        executor="shortcut",
        payload="shortcuts-Lakehouse--Curated.shortcut.json",
        payload_sha256="0" * 64,
    )


def _local_context(tmp_path, *, resolver=None, store=None):

    # With a Spark destination, as a real Lakehouse target resolves to. Without
    # one a shortcut in it cannot even be named, which used to go unnoticed
    # because the discovery wait was skipped whenever there was no Spark session.
    destination = replace(
        _target(DESTINATION_TARGET_ID, "Curated_Dev"),
        destination=FabricSparkTarget(workspace="Demo", lakehouse="Curated_Dev"),
        location=LakehouseSparkLocation(
            item="Curated_Dev",
            tables_root="abfss://workspace/item/Tables",
            files_root="abfss://workspace/item/Files",
        ),
    )
    source = _target(SOURCE_TARGET_ID, "Raw_Dev")
    local_resolver = given_resolver(
        workspace=given_workspace(catalogue="Warehouse/Weaver"),
        lakehouses=("Weaver", "Raw_Dev", "Curated_Dev", "Sales_LH"),
        root=tmp_path,
    )
    if resolver is not None and hasattr(resolver, "inner"):
        resolver.inner = local_resolver
    chosen_store = store or FilesystemStore()
    if resolver is not None and hasattr(resolver, "submit_onelake_shortcuts"):
        chosen_store.make_directory(
            local_resolver.tables_root(ItemRef("Raw_Dev")) / "Sales" / "Customer"
        )
        chosen_store.make_directory(
            local_resolver.files_root(ItemRef("Raw_Dev")) / "Sales" / "Customer"
        )
    return InstallationContext(
        spark_sql=lambda statement, exact_case=False: [],
        spark_sql_batch=lambda statements, exact_case=False: [],
        resolver=resolver or local_resolver,
        store=chosen_store,
        target=destination,
        targets={DESTINATION_TARGET_ID: destination, SOURCE_TARGET_ID: source},
    )


def run(executor, action, payload, context):
    """Resume a resumable executor until it finishes, recording each wait."""

    from weaver.mutation.serialization import freeze_value

    waits = []
    result = executor.execute(action, payload, context, state=None)
    while isinstance(result, Waiting):
        # The invocation ledger records waiting state, so it must be plain data.
        freeze_value(result.state)
        waits.append(result.delay)
        result = executor.execute(action, payload, context, state=result.state)
    return result, waits


# --- creation ----------------------------------------------------------------


@weaver_test()
def test_a_shortcut_naming_a_target_the_plan_never_declared_fails(tmp_path):
    context = _local_context(tmp_path, resolver=_ShortcutResolver())

    with pytest.raises(InstallError, match="which this plan does not declare"):
        run(
            ShortcutExecutor(),
            _action(),
            _payload(source_target_id="lakehouse-Nowhere"),
            context,
        )


class _ShortcutResolver:
    """A resolver that submits shortcuts, as the Fabric ones do.

    ``waiting`` is how many submissions report the source as still being
    published to OneLake before it succeeds.
    """

    def __init__(self, events=None, *, waiting=0):
        self.calls = []
        self.source_kinds = []
        self.batches = []
        self.events = events if events is not None else []
        self.waiting = waiting
        self.inner = None

    def __getattr__(self, name):
        if self.inner is None:
            raise AttributeError(name)
        return getattr(self.inner, name)

    def submit_onelake_shortcuts(self, item, shortcuts):
        from weaver.fabric.shortcuts import BulkSubmission

        requests = list(shortcuts)
        self.batches.append(len(requests))
        self.events.append("create")
        for request in requests:
            self.calls.append(
                (
                    item.name,
                    request["path"],
                    request["name"],
                    request["source"].name,
                    request["source_path"],
                )
            )
            self.source_kinds.append(request["source_kind"])
        if self.waiting:
            self.waiting -= 1
            return BulkSubmission(created={}, waiting=tuple(range(len(requests))))
        return BulkSubmission(
            created={
                i: {"path": f"{request['path']}/{request['name']}"}
                for i, request in enumerate(requests)
            },
            waiting=(),
        )


@weaver_test()
def test_a_shortcut_becomes_one_onelake_shortcut_without_reading_it(tmp_path):
    reads = []
    resolver = _ShortcutResolver()
    context = replace(
        _local_context(tmp_path, resolver=resolver),
        spark_sql=lambda statement, exact_case=False: reads.append(statement),
    )

    details, waits = run(ShortcutExecutor(), _action(), _payload(), context)

    assert resolver.calls == [
        ("Curated_Dev", "Tables/Sales", "Landed", "Raw_Dev", "Tables/Sales/Customer")
    ]
    assert details["shortcuts"][0]["path"] == "Tables/Sales/Landed"
    assert details["shortcuts"][0]["source"] == "Lakehouse/Raw/Tables/Sales.Customer"
    # Readiness is its own action; creation finishes once Fabric accepts.
    assert reads == [] and waits == []


class _FoldedSourceStore(FilesystemStore):
    """A Fabric estate whose physical table name was folded to lower-case."""

    def __init__(self):
        self.listed = 0

    def exists(self, location):
        return not location.value.endswith("/Customer")

    def list(self, location, *, recursive=False):
        self.listed += 1
        return [Entry(location=location / "customer", is_directory=True)]


@weaver_test()
def test_a_shortcut_uses_the_source_tables_physical_case(tmp_path):
    resolver = _ShortcutResolver()
    context = _local_context(tmp_path, resolver=resolver, store=_FoldedSourceStore())

    run(ShortcutExecutor(), _action(), _payload(), context)

    assert resolver.calls[0][-1] == "Tables/Sales/customer"


@weaver_test()
def test_a_source_still_being_published_yields_and_resolves_once(tmp_path):
    store = _FoldedSourceStore()
    resolver = _ShortcutResolver(waiting=2)
    context = _local_context(tmp_path, resolver=resolver, store=store)

    details, waits = run(ShortcutExecutor(), _action(), _payload(), context)

    assert len(waits) == 2
    assert resolver.batches == [1, 1, 1]
    assert store.listed == 1
    assert details["shortcuts"][0]["path"] == "Tables/Sales/Landed"


@weaver_test()
def test_a_source_that_never_reaches_onelake_fails_naming_the_shortcut(
    tmp_path, monkeypatch
):
    import weaver.fabric.shortcuts as shortcuts

    monkeypatch.setattr(shortcuts, "SOURCE_TIMEOUT", 0.0)
    context = _local_context(tmp_path, resolver=_ShortcutResolver(waiting=99))

    with pytest.raises(Exception, match="Lakehouse/Curated/Tables/Sales.Landed"):
        run(ShortcutExecutor(), _action(), _payload(), context)


@weaver_test()
def test_a_warehouse_source_uses_its_onelake_table_spelling(tmp_path):
    """A Warehouse source has no Lakehouse path to resolve through the store."""

    resolver = _ShortcutResolver()
    context = _local_context(tmp_path, resolver=resolver)
    warehouse = ResolvedTarget(
        bound=BoundTarget(
            id=SOURCE_TARGET_ID,
            kind="warehouse",
            item_id="Serving_WH",
            item_name="Serving_WH",
        ),
        lakehouse=ItemRef("Serving_WH"),
    )
    context = replace(context, targets={**context.targets, SOURCE_TARGET_ID: warehouse})

    run(
        ShortcutExecutor(),
        _action(),
        _payload(source="Warehouse/Serving/Sales.Customer"),
        context,
    )

    assert resolver.calls[0][-2:] == ("Serving_WH", "Tables/Sales/Customer")
    assert resolver.source_kinds == ["warehouse"]


@weaver_test()
def test_the_fabric_transport_resolves_a_bound_warehouse_by_its_declared_kind(
    monkeypatch,
):
    """A Lakehouse and Warehouse may share a name, so the source slot is typed."""

    import weaver.fabric.shortcuts as shortcuts

    resolver = given_resolver(lakehouses=("Curated_LH",), warehouses=("Serving_WH",))
    captured = {}

    def create(destination, requests, *, client):
        captured.update(destination=destination, requests=requests)
        return shortcuts.BulkShortcutResult(
            created=tuple({"path": each.qualified} for each in requests),
            calls=1,
        )

    monkeypatch.setattr(shortcuts, "create_shortcuts", create)

    resolver.create_onelake_shortcuts(
        ItemRef("Curated_LH"),
        [
            {
                "path": "Tables/Sales",
                "name": "Customer",
                "source": ItemRef("Serving_WH"),
                "source_kind": "warehouse",
                "source_path": "Tables/Sales/Customer",
            }
        ],
    )

    assert captured["destination"].type == "Lakehouse"
    assert captured["requests"][0].source.type == "Warehouse"


@weaver_test()
def test_an_environment_that_cannot_create_a_shortcut_says_so(tmp_path):
    class _WithoutShortcuts:
        """A resolver that resolves, and offers no shortcut creation."""

        def __init__(self):
            self.inner = None

        def __getattr__(self, name):
            if name == "submit_onelake_shortcuts" or self.inner is None:
                raise AttributeError(name)
            return getattr(self.inner, name)

    context = _local_context(tmp_path, resolver=_WithoutShortcuts())

    with pytest.raises(InstallError, match="no way to create a OneLake shortcut"):
        run(ShortcutExecutor(), _action(), _payload(), context)


def _two_shortcuts() -> bytes:
    first = json.loads(_payload().decode())["shortcuts"][0]
    second = dict(
        first, shortcut="Lakehouse/Curated/Tables/Sales.Second", name="Second"
    )
    return json.dumps({"shortcuts": [first, second]}).encode("utf-8")


@weaver_test()
def test_one_action_creates_its_shortcuts_as_one_batch(tmp_path):
    resolver = _ShortcutResolver()
    context = _local_context(tmp_path, resolver=resolver)

    details, _waits = run(ShortcutExecutor(), _action(), _two_shortcuts(), context)

    assert [detail["shortcut"] for detail in details["shortcuts"]] == [
        "Lakehouse/Curated/Tables/Sales.Landed",
        "Lakehouse/Curated/Tables/Sales.Second",
    ]
    assert resolver.batches == [2]


# --- readiness ---------------------------------------------------------------


class _LateSpark:
    """Spark where the shortcut is not readable for the first few tries.

    Fabric accepts the shortcut synchronously and discovers it asynchronously;
    in between the Lakehouse reports the name as neither a view nor a table.
    """

    def __init__(self, failures: int, *, path_only=False):
        self.remaining = failures
        self.path_only = path_only
        self.statements: list[str] = []
        self.exact_case: list[bool] = []

    def __call__(self, statement, *, exact_case: bool = False):
        self.statements.append(statement)
        self.exact_case.append(exact_case)
        if self.remaining > 0 and (not self.path_only or "FROM delta." in statement):
            self.remaining -= 1
            raise RuntimeError("requirement failed: it's neither a view nor a table")
        return []


def _readiness(surface="tables", names=("Landed",)) -> tuple[InstallAction, bytes]:
    action = InstallAction(
        id=f"await-{surface}-shortcuts-Lakehouse--Curated",
        kind=f"await_{'table' if surface == 'tables' else 'file'}_shortcuts",
        resource_node_id=None,
        executor="shortcut_readiness",
        payload=f"{surface}.shortcut-readiness.json",
        payload_sha256="0" * 64,
    )
    area = "Tables" if surface == "tables" else "Files"
    payload = json.dumps(
        {
            "surface": surface,
            "shortcuts": [
                {
                    "shortcut": f"Lakehouse/Curated/{area}/Sales.{name}",
                    "path": f"{area}/Sales",
                    "name": name,
                }
                for name in names
            ],
        }
    ).encode("utf-8")
    return action, payload


@weaver_test()
def test_readiness_yields_until_relation_and_delta_path_are_readable(tmp_path):
    spark = _LateSpark(failures=2)
    context = replace(_local_context(tmp_path), spark_sql=spark)

    details, waits = run(ShortcutReadinessExecutor(), *_readiness(), context)

    assert len(waits) == 1
    assert spark.statements[0] == (
        "SELECT * FROM `Demo`.`Curated_Dev`.`Sales`.`Landed` LIMIT 0"
    )
    assert spark.statements[1] == (
        "SELECT * FROM delta.`abfss://workspace/item/Tables/Sales/Landed` LIMIT 0"
    )
    # The second sweep rechecks only what was not yet readable.
    assert len(spark.statements) == 4
    assert all(spark.exact_case)
    assert "ready_after_seconds" in details


@weaver_test()
def test_readiness_waits_for_the_delta_path_after_the_relation_is_ready(tmp_path):
    spark = _LateSpark(failures=2, path_only=True)
    context = replace(_local_context(tmp_path), spark_sql=spark)

    _details, waits = run(ShortcutReadinessExecutor(), *_readiness(), context)

    relation = [s for s in spark.statements if "FROM delta." not in s]
    physical = [s for s in spark.statements if "FROM delta." in s]
    assert len(waits) == 2
    assert len(relation) == 1
    assert len(physical) == 3


@weaver_test()
def test_one_sweep_checks_every_shortcut_in_the_action(tmp_path):
    spark = _LateSpark(failures=0)
    context = replace(_local_context(tmp_path), spark_sql=spark)

    _details, waits = run(
        ShortcutReadinessExecutor(), *_readiness(names=("Landed", "Second")), context
    )

    assert waits == []
    assert len(spark.statements) == 4


@weaver_test()
def test_a_shortcut_that_never_becomes_readable_fails_naming_itself(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(shortcut_module, "ADDRESSABLE_TIMEOUT", 0)
    context = replace(_local_context(tmp_path), spark_sql=_LateSpark(failures=99))

    with pytest.raises(InstallError, match="Sales.Landed were created but did not"):
        run(ShortcutReadinessExecutor(), *_readiness(), context)


@weaver_test()
def test_files_readiness_checks_storage_not_spark(tmp_path):
    spark = _LateSpark(failures=99)
    store = _ReleasesAfter(FilesystemStore(), occupied=0, present_after=1)
    context = replace(_local_context(tmp_path, store=store), spark_sql=spark)

    details, waits = run(ShortcutReadinessExecutor(), *_readiness("files"), context)

    assert spark.statements == []
    assert len(waits) == 1
    assert "ready_after_seconds" in details


@weaver_test()
def test_a_batch_of_tsql_statements_runs_each_as_its_own_batch():
    """T-SQL refuses a CREATE VIEW that is not first in its batch."""

    from weaver.build_bundle.executors import TSqlBatchExecutor

    class _Sql:
        def __init__(self):
            self.scripts = []

        def execute_script(self, script):
            self.scripts.append(script)

    sql = _Sql()
    context = InstallationContext(
        resolver=None,
        store=FilesystemStore(),
        target=_target(DESTINATION_TARGET_ID, "Reporting_WH"),
        sql=sql,
    )
    payload = json.dumps(
        [
            "create or alter view [Rpt].[A] as select 1 as x;",
            "create or alter view [Rpt].[B] as select 1 as x;",
        ]
    ).encode("utf-8")
    action = InstallAction(
        id="shortcuts-Warehouse--Reporting",
        kind=CREATE_SHORTCUT,
        resource_node_id=None,
        executor="tsql_batch",
        payload="shortcuts.tsql-batch.json",
        payload_sha256="0" * 64,
    )

    details = TSqlBatchExecutor().execute(action, payload, context)

    assert details == {"statements": 2}
    assert sql.scripts == [
        "create or alter view [Rpt].[A] as select 1 as x;",
        "create or alter view [Rpt].[B] as select 1 as x;",
    ]


# --- releasing a name an owned object is about to take ------------------------


def _removal(awaits: bool) -> InstallAction:
    return InstallAction(
        id="drop-shortcuts-Lakehouse--Curated",
        kind=DROP_SHORTCUT,
        resource_node_id=None,
        executor="shortcut",
        payload="drop-shortcuts-Lakehouse--Curated.shortcut.json",
        payload_sha256="0" * 64,
        awaits_name_release=awaits,
    )


def _removal_payload() -> bytes:
    return json.dumps(
        {
            "remove": [
                {
                    "shortcut": "Lakehouse/Curated/Tables/Sales.Landed",
                    "path": "Tables/Sales",
                    "name": "Landed",
                }
            ]
        }
    ).encode("utf-8")


class _ReleasesAfter:
    """A store whose path answers until the namespace lets the name go.

    ``present_after`` instead makes a path appear after that many questions.
    """

    def __init__(self, inner, occupied: int, present_after: int | None = None):
        self._inner = inner
        self.remaining = occupied
        self.present_after = present_after
        self.asked = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def exists(self, location) -> bool:
        self.asked += 1
        if self.present_after is not None:
            return self.asked > self.present_after
        if self.remaining <= 0:
            return False
        self.remaining -= 1
        return True


class _Unpicks:
    """A workspace that removes the pointer and records that it did."""

    def __init__(self, inner=None):
        self.inner = inner
        self.removed = []

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def remove_onelake_shortcut(self, lakehouse, *, path, name):
        self.removed.append((path, name))


@weaver_test()
def test_a_removal_yields_until_onelake_releases_the_name(tmp_path):
    """The transition the flag exists for.

    Fabric stops listing the shortcut before OneLake gives the name up, so the
    path still answers for a while. The action yields until it does not, which
    lets the owned object be created at the same name.
    """

    resolver = _Unpicks()
    store = _ReleasesAfter(FilesystemStore(), occupied=3)
    context = _local_context(tmp_path, resolver=resolver, store=store)

    details, waits = run(
        ShortcutExecutor(), _removal(True), _removal_payload(), context
    )

    assert resolver.removed == [("Tables/Sales", "Landed")]
    assert details["removed"] == ["Lakehouse/Curated/Tables/Sales.Landed"]
    # Asked until it answered, and then once more is not needed.
    assert store.asked == 4
    assert len(waits) == 3
    assert "released_after_seconds" in details


@weaver_test()
def test_a_removal_that_reuses_no_name_does_not_poll(tmp_path):
    """The flag is what gates the wait, so an ordinary removal pays nothing."""

    resolver = _Unpicks()
    store = _ReleasesAfter(FilesystemStore(), occupied=3)
    context = _local_context(tmp_path, resolver=resolver, store=store)

    details, waits = run(
        ShortcutExecutor(), _removal(False), _removal_payload(), context
    )

    assert resolver.removed == [("Tables/Sales", "Landed")]
    assert store.asked == 0 and waits == []
    assert "released_after_seconds" not in details


@weaver_test()
def test_a_name_held_past_the_timeout_returns_to_the_create(tmp_path, monkeypatch):
    """A spent wait is not the error. The create that follows reports the name.

    Raising here would name a timeout where the useful report is which object
    already stands at the address, so the wait ends and the build goes on.
    """

    monkeypatch.setattr(shortcut_module, "NAME_RELEASE_TIMEOUT", 0.0)
    resolver = _Unpicks()
    store = _ReleasesAfter(FilesystemStore(), occupied=10**6)
    context = _local_context(tmp_path, resolver=resolver, store=store)

    details, _waits = run(
        ShortcutExecutor(), _removal(True), _removal_payload(), context
    )

    assert resolver.removed == [("Tables/Sales", "Landed")]
    assert "released_after_seconds" in details
