"""Materialising a shortcut, and closing an item with an endpoint refresh."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_resolver, given_workspace

from weaver.build_bundle.executors import ShortcutExecutor
from weaver.build_bundle.executors import shortcut as shortcut_module
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.models import CREATE_SHORTCUT, InstallAction
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
        "shortcut": "Lakehouse/Curated/Sales.Landed",
        "source": "Lakehouse/Raw/Sales.Customer",
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
    if resolver is not None and hasattr(resolver, "create_onelake_shortcut"):
        chosen_store.make_directory(
            local_resolver.tables_root(ItemRef("Raw_Dev")) / "Sales" / "Customer"
        )
        chosen_store.make_directory(
            local_resolver.files_root(ItemRef("Raw_Dev")) / "Sales" / "Customer"
        )
    return InstallationContext(
        # A host that can ask Spark and finds the shortcut readable at once. The
        # Installer supplies this on every host, so a context without it is one
        # nobody would build in production, and the executor says so rather
        # than skipping the wait.
        spark_sql=lambda statement, exact_case=False: [],
        spark_sql_batch=lambda statements, exact_case=False: [],
        resolver=resolver or local_resolver,
        store=chosen_store,
        target=destination,
        targets={DESTINATION_TARGET_ID: destination, SOURCE_TARGET_ID: source},
    )


# --- the shortcut ------------------------------------------------------------


@weaver_test()
def test_a_shortcut_naming_a_target_the_plan_never_declared_fails(tmp_path):
    context = _local_context(tmp_path)

    with pytest.raises(InstallError, match="which this plan does not declare"):
        ShortcutExecutor().execute(
            _action(), _payload(source_target_id="lakehouse-Nowhere"), context
        )


# --- Fabric: a OneLake shortcut ----------------------------------------------


class _ShortcutResolver:
    """A resolver that can make a shortcut, as the Fabric ones can.

    ``events`` is shared with the fake session so a test can prove the ordering
    between creating shortcuts and reading them.
    """

    def __init__(self, events=None):
        self.calls = []
        self.source_kinds = []
        self.events = events if events is not None else []
        self.inner = None

    def __getattr__(self, name):
        if self.inner is None:
            raise AttributeError(name)
        return getattr(self.inner, name)

    def create_onelake_shortcut(
        self, item, *, path, name, source, source_kind=None, source_path
    ):
        self.calls.append((item.name, path, name, source.name, source_path))
        self.source_kinds.append(source_kind)
        self.events.append("create")
        return {"path": f"{path}/{name}"}


@weaver_test()
def test_a_shortcut_becomes_one_onelake_shortcut(tmp_path):
    resolver = _ShortcutResolver()
    context = _local_context(tmp_path, resolver=resolver)

    details = ShortcutExecutor().execute(_action(), _payload(), context)

    assert resolver.calls == [
        ("Curated_Dev", "Tables/Sales", "Landed", "Raw_Dev", "Tables/Sales/Customer")
    ]
    assert details["shortcuts"][0]["path"] == "Tables/Sales/Landed"
    assert details["shortcuts"][0]["source"] == "Lakehouse/Raw/Sales.Customer"


class _FoldedSourceStore(FilesystemStore):
    """A Fabric estate whose physical table name was folded to lower-case."""

    def exists(self, location):
        return not location.value.endswith("/Customer")

    def list(self, location, *, recursive=False):
        return [Entry(location=location / "customer", is_directory=True)]


@weaver_test()
def test_a_shortcut_uses_the_source_tables_physical_case(tmp_path):
    resolver = _ShortcutResolver()
    context = _local_context(tmp_path, resolver=resolver, store=_FoldedSourceStore())

    ShortcutExecutor().execute(_action(), _payload(), context)

    assert resolver.calls[0][-1] == "Tables/Sales/customer"


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

    ShortcutExecutor().execute(
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

    def create(item, *, path, name, source, source_path, client):
        captured.update(destination=item, source=source)
        return {"path": f"{path}/{name}"}

    monkeypatch.setattr(shortcuts, "create_shortcut", create)

    resolver.create_onelake_shortcut(
        ItemRef("Curated_LH"),
        path="Tables/Sales",
        name="Customer",
        source=ItemRef("Serving_WH"),
        source_kind="warehouse",
        source_path="Tables/Sales/Customer",
    )

    assert captured["destination"].type == "Lakehouse"
    assert captured["source"].type == "Warehouse"


class _Conf:
    def __init__(self):
        self.values = {"spark.sql.caseSensitive": "false"}

    def get(self, name):
        return self.values[name]

    def set(self, name, value):
        self.values[name] = value


class _LateSpark:
    """Spark where the shortcut is not readable for the first few tries.

    This is what Fabric does: the shortcut is created synchronously and discovered
    asynchronously, and in between the Lakehouse reports the name as neither a view
    nor a table.

    Doubled as the capability the executor asks through rather than as a Spark
    session, because that is now the seam: the shortcut executor stays on whichever
    host is installing and only the question crosses. A double shaped like a
    session would be testing an arrangement the product no longer has.
    """

    def __init__(self, failures: int, events=None):
        self.remaining = failures
        self.statements: list[str] = []
        self.exact_case: list[bool] = []
        self.conf = _Conf()
        self.events = events if events is not None else []

    def __call__(self, statement, *, exact_case: bool = False):
        self.statements.append(statement)
        self.exact_case.append(exact_case)
        self.events.append("read")
        if self.remaining > 0:
            self.remaining -= 1
            raise RuntimeError("requirement failed: it's neither a view nor a table")
        return []


def _addressable_context(tmp_path, spark, resolver):

    destination = ResolvedTarget(
        bound=BoundTarget(
            id=DESTINATION_TARGET_ID,
            kind="lakehouse",
            item_id="Curated_Dev",
            item_name="Curated_Dev",
        ),
        lakehouse=ItemRef("Curated_Dev"),
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
    resolver.inner = local_resolver
    store = FilesystemStore()
    store.make_directory(
        local_resolver.tables_root(ItemRef("Raw_Dev")) / "Sales" / "Customer"
    )
    store.make_directory(
        local_resolver.files_root(ItemRef("Raw_Dev")) / "Sales" / "Customer"
    )
    return InstallationContext(
        spark_sql=spark,
        spark_sql_batch=lambda statements, exact_case=False: [],
        resolver=resolver,
        store=store,
        target=destination,
        targets={DESTINATION_TARGET_ID: destination, SOURCE_TARGET_ID: source},
    )


@weaver_test()
def test_a_shortcut_is_not_finished_until_it_can_be_read(tmp_path, monkeypatch):
    """Returning on the API call would make the plan's barrier a lie."""

    monkeypatch.setattr(shortcut_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    spark = _LateSpark(failures=2)
    context = _addressable_context(tmp_path, spark, _ShortcutResolver())

    details = ShortcutExecutor().execute(_action(), _payload(), context)

    reads = [s for s in spark.statements if s.startswith("SELECT")]
    assert len(reads) == 4
    assert "`Demo`.`Curated_Dev`.`Sales`.`Landed`" in reads[0]
    assert "delta.`abfss://workspace/item/Tables/Sales/Landed`" in reads[1]
    assert "addressable_after_seconds" in details


class _LateDeltaPath(_LateSpark):
    """The named relation is ready before the Delta path Python loaders read."""

    def __call__(self, statement, *, exact_case: bool = False):
        self.statements.append(statement)
        self.exact_case.append(exact_case)
        self.events.append("read")
        if "FROM delta." in statement and self.remaining > 0:
            self.remaining -= 1
            raise RuntimeError("PATH_NOT_FOUND")
        return []


@weaver_test()
def test_a_table_shortcut_waits_for_its_delta_path_after_the_relation_is_ready(
    tmp_path, monkeypatch
):
    """Python Delta loads must not start during Fabric's split discovery window."""

    monkeypatch.setattr(shortcut_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    spark = _LateDeltaPath(failures=2)
    context = _addressable_context(tmp_path, spark, _ShortcutResolver())

    ShortcutExecutor().execute(_action(), _payload(), context)

    relation = [
        statement for statement in spark.statements if "FROM delta." not in statement
    ]
    physical = [
        statement for statement in spark.statements if "FROM delta." in statement
    ]
    assert len(relation) == 1
    assert len(physical) == 3
    assert physical[0] == (
        "SELECT * FROM delta.`abfss://workspace/item/Tables/Sales/Landed` LIMIT 0"
    )


@weaver_test()
def test_a_shortcut_that_never_becomes_readable_fails_naming_itself(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(shortcut_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    monkeypatch.setattr(shortcut_module, "ADDRESSABLE_TIMEOUT", 0)
    context = _addressable_context(
        tmp_path, _LateSpark(failures=99), _ShortcutResolver()
    )

    with pytest.raises(InstallError, match="did not become readable within"):
        ShortcutExecutor().execute(_action(), _payload(), context)


@weaver_test()
def test_a_folder_shortcut_needs_no_readability_wait(tmp_path, monkeypatch):
    """A folder is a directory; there is no relation to become addressable."""

    monkeypatch.setattr(shortcut_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    spark = _LateSpark(failures=99)
    context = _addressable_context(tmp_path, spark, _ShortcutResolver())

    details = ShortcutExecutor().execute(
        _action(),
        _payload(type="folder", path="Files/Sales", source_area="Files"),
        context,
    )

    assert not spark.statements
    assert "addressable_after_seconds" not in details


class _NoTransportStore(FilesystemStore):
    """A store with no link operation, as a OneLake DFS client has none."""

    link = None


@weaver_test()
def test_an_environment_that_cannot_create_a_shortcut_says_so(tmp_path):
    """A shortcut is a OneLake shortcut, so a host that cannot make one cannot
    materialise it, and says which action it could not perform."""

    class _WithoutShortcuts:
        """A resolver that resolves, and offers no shortcut creation."""

        def __init__(self, inner):
            self.inner = inner

        def __getattr__(self, name):
            if name == "create_onelake_shortcut":
                raise AttributeError(name)
            return getattr(self.inner, name)

    context = _local_context(tmp_path, resolver=_WithoutShortcuts(None))

    with pytest.raises(InstallError, match="no way to create a OneLake shortcut"):
        ShortcutExecutor().execute(_action(), _payload(), context)


# --- several shortcuts, one action ----------------------------------------------


def _two_shortcuts() -> bytes:
    first = json.loads(_payload().decode())["shortcuts"][0]
    second = dict(first, shortcut="Lakehouse/Curated/Sales.Second", name="Second")
    return json.dumps({"shortcuts": [first, second]}).encode("utf-8")


@weaver_test()
def test_every_shortcut_is_created_before_anything_waits(tmp_path, monkeypatch):
    """The cost of a shortcut is the wait, so the waits must not serialise.

    Two shortcuts through one action means one discovery window rather than two,
    only true if both shortcuts exist before the first read is attempted.
    """

    monkeypatch.setattr(shortcut_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    events: list[str] = []
    resolver = _ShortcutResolver(events=events)
    spark = _LateSpark(failures=2, events=events)
    context = _addressable_context(tmp_path, spark, resolver)

    details = ShortcutExecutor().execute(_action(), _two_shortcuts(), context)

    assert [detail["shortcut"] for detail in details["shortcuts"]] == [
        "Lakehouse/Curated/Sales.Landed",
        "Lakehouse/Curated/Sales.Second",
    ]
    # Every create precedes every read: two shortcuts, one discovery window.
    creates = [index for index, event in enumerate(events) if event == "create"]
    reads = [index for index, event in enumerate(events) if event == "read"]
    assert len(creates) == 2 and reads
    assert max(creates) < min(reads)


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


@weaver_test()
def test_the_wait_asks_spark_rather_than_holding_one(tmp_path):
    """A desktop has no Spark session and must still wait for discovery.

    The guard was once ``context.spark is not None``, so a desktop install
    skipped the wait and the next statement to read the shortcut failed with
    "neither a view nor a table". The context carries no session at all now.
    """

    asked = _LateSpark(failures=1)
    context = _addressable_context(tmp_path, asked, _ShortcutResolver())

    ShortcutExecutor().execute(_action(), _payload(), context)

    assert asked.statements, "the discovery wait was skipped without a Spark session"
    assert all("LIMIT 0" in statement for statement in asked.statements)


@weaver_test()
def test_the_probe_carries_weavers_identifier_case(tmp_path):
    """The scope has to travel with the statement: a desktop has no Spark to
    set a conf on, and a probe analysed under the session's default case is a
    different probe."""

    asked = _LateSpark(failures=0)
    context = _addressable_context(tmp_path, asked, _ShortcutResolver())

    ShortcutExecutor().execute(_action(), _payload(), context)

    assert asked.exact_case and all(asked.exact_case)
