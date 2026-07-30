"""Materialising an alias, and closing an item with an endpoint refresh."""

from __future__ import annotations

import json

import pytest

from weaver.build_bundle.executors import alias as alias_module
from weaver.build_bundle.executors import AliasExecutor, SqlEndpointExecutor
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.models import CREATE_ALIAS, REFRESH_SQL_ENDPOINT, BuildAction
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import InstallError
from weaver.locations import Location
from weaver.resolution import LocalResolver
from weaver.store import LocalStore
from weaver.targets import ItemRef
from weaver.workspaces import LocalWorkspace

SOURCE_TARGET_ID = "Lakehouse-Raw--lakehouse-Raw_Dev"
DESTINATION_TARGET_ID = "Lakehouse-Curated--lakehouse-Curated_Dev"


def _payload(**overrides) -> bytes:
    """One alias, in the batched shape the action now carries."""

    mapping = {
        "alias": "Lakehouse/Curated/Sales.Landed",
        "source": "Lakehouse/Raw/Sales.Customer",
        "source_target_id": SOURCE_TARGET_ID,
        "area": "Tables",
        "schema": "Sales",
        "object": "Landed",
        "source_area": "Tables",
        "source_schema": "Sales",
        "source_object": "Customer",
    }
    mapping.update(overrides)
    return json.dumps({"aliases": [mapping]}).encode("utf-8")


def _target(target_id: str, item: str) -> ResolvedTarget:
    return ResolvedTarget(
        bound=BoundTarget(id=target_id, kind="lakehouse", item_id=item, item_name=item),
        lakehouse=ItemRef(item),
    )


def _action() -> BuildAction:
    return BuildAction(
        id="aliases-Lakehouse--Curated",
        kind=CREATE_ALIAS,
        resource_node_id=None,
        executor="alias",
        payload="aliases-Lakehouse--Curated.alias.json",
        payload_sha256="0" * 64,
    )


def _local_context(tmp_path, *, resolver=None, store=None):
    destination = _target(DESTINATION_TARGET_ID, "Curated_Dev")
    source = _target(SOURCE_TARGET_ID, "Raw_Dev")
    return InstallationContext(
        spark=None,
        resolver=resolver
        or LocalResolver(LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver")),
        store=store or LocalStore(),
        snapshot=Location(str(tmp_path / "snapshot")),
        target=destination,
        targets={DESTINATION_TARGET_ID: destination, SOURCE_TARGET_ID: source},
    )


# --- the emulator: a filesystem link ------------------------------------------


def test_a_local_alias_links_the_destination_to_the_source_table(tmp_path):
    store = LocalStore()
    context = _local_context(tmp_path, store=store)
    produced = context.resolver.tables_root(ItemRef("Raw_Dev")) / "Sales" / "Customer"
    store.make_directory(produced)
    store.write(produced / "_delta_log" / "00.json", b"{}")

    details = AliasExecutor().execute(_action(), _payload(), context)

    linked = context.resolver.tables_root(ItemRef("Curated_Dev")) / "Sales" / "Landed"
    assert linked.path.is_symlink()
    assert linked.path.resolve() == produced.path.resolve()
    assert store.read(linked / "_delta_log" / "00.json") == b"{}"
    assert details["aliases"][0]["alias"] == "Lakehouse/Curated/Sales.Landed"


def test_a_local_alias_replaces_one_that_is_already_there(tmp_path):
    """An alias holds no data, so re-running a build re-points it rather than failing."""

    store = LocalStore()
    context = _local_context(tmp_path, store=store)
    tables = context.resolver.tables_root
    for name in ("Customer", "Other"):
        store.make_directory(tables(ItemRef("Raw_Dev")) / "Sales" / name)

    AliasExecutor().execute(_action(), _payload(), context)
    AliasExecutor().execute(
        _action(), _payload(source_object="Other"), context
    )

    linked = tables(ItemRef("Curated_Dev")) / "Sales" / "Landed"
    assert linked.path.resolve().name == "Other"


def test_a_local_alias_over_a_missing_source_fails_rather_than_dangling(tmp_path):
    context = _local_context(tmp_path)

    with pytest.raises(InstallError, match="has no source to point at"):
        AliasExecutor().execute(_action(), _payload(), context)


def test_a_files_alias_links_into_the_destination_files_area(tmp_path):
    store = LocalStore()
    context = _local_context(tmp_path, store=store)
    produced = context.resolver.files_root(ItemRef("Raw_Dev")) / "Sales" / "Export"
    store.make_directory(produced)

    AliasExecutor().execute(
        _action(),
        _payload(area="Files", source_area="Files", source_object="Export"),
        context,
    )

    linked = context.resolver.files_root(ItemRef("Curated_Dev")) / "Sales" / "Landed"
    assert linked.path.is_symlink()
    assert linked.path.resolve() == produced.path.resolve()


def test_an_alias_naming_a_target_the_plan_never_declared_fails(tmp_path):
    context = _local_context(tmp_path)

    with pytest.raises(InstallError, match="which this plan does not declare"):
        AliasExecutor().execute(
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
        self.events = events if events is not None else []

    def create_onelake_shortcut(self, item, *, path, name, source, source_path):
        self.calls.append((item.name, path, name, source.name, source_path))
        self.events.append("create")
        return {"shortcut": f"{path}/{name}"}


def test_a_fabric_alias_becomes_one_onelake_shortcut(tmp_path):
    resolver = _ShortcutResolver()
    context = _local_context(tmp_path, resolver=resolver)

    details = AliasExecutor().execute(_action(), _payload(), context)

    assert resolver.calls == [
        ("Curated_Dev", "Tables/Sales", "Landed", "Raw_Dev", "Tables/Sales/Customer")
    ]
    assert details["aliases"][0]["shortcut"] == "Tables/Sales/Landed"
    assert details["aliases"][0]["source"] == "Lakehouse/Raw/Sales.Customer"


class _Conf:
    def __init__(self):
        self.values = {"spark.sql.caseSensitive": "false"}

    def get(self, name):
        return self.values[name]

    def set(self, name, value):
        self.values[name] = value


class _LateSpark:
    """A session where the shortcut is not readable for the first few tries.

    This is what Fabric does: the shortcut is created synchronously and discovered
    asynchronously, and in between the Lakehouse reports the name as neither a view
    nor a table.
    """

    def __init__(self, failures: int, events=None):
        self.remaining = failures
        self.statements: list[str] = []
        self.conf = _Conf()
        self.events = events if events is not None else []

    def sql(self, statement):
        self.statements.append(statement)
        self.events.append("read")
        if self.remaining > 0:
            self.remaining -= 1
            raise RuntimeError("requirement failed: it's neither a view nor a table")

        class _Result:
            def collect(self):
                return []

        return _Result()


def _addressable_context(tmp_path, spark, resolver):
    from weaver.spark import local_destination

    destination = ResolvedTarget(
        bound=BoundTarget(
            id=DESTINATION_TARGET_ID,
            kind="lakehouse",
            item_id="Curated_Dev",
            item_name="Curated_Dev",
        ),
        lakehouse=ItemRef("Curated_Dev"),
        destination=local_destination(
            item="Curated_Dev", tables_root=str(tmp_path / "Curated_Dev" / "Tables")
        ),
    )
    source = _target(SOURCE_TARGET_ID, "Raw_Dev")
    return InstallationContext(
        spark=spark,
        resolver=resolver,
        store=LocalStore(),
        snapshot=Location(str(tmp_path / "snapshot")),
        target=destination,
        targets={DESTINATION_TARGET_ID: destination, SOURCE_TARGET_ID: source},
    )


def test_a_fabric_alias_is_not_finished_until_the_shortcut_can_be_read(
    tmp_path, monkeypatch
):
    """Returning on the API call would make the plan's barrier a lie."""

    monkeypatch.setattr(alias_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    spark = _LateSpark(failures=2)
    context = _addressable_context(tmp_path, spark, _ShortcutResolver())

    details = AliasExecutor().execute(_action(), _payload(), context)

    reads = [s for s in spark.statements if s.startswith("SELECT")]
    assert len(reads) == 3
    assert "`curated_dev__sales`.`Landed`" in reads[0]
    assert "addressable_after_seconds" in details


def test_a_shortcut_that_never_becomes_readable_fails_naming_itself(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(alias_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    monkeypatch.setattr(alias_module, "ADDRESSABLE_TIMEOUT", 0)
    context = _addressable_context(tmp_path, _LateSpark(failures=99), _ShortcutResolver())

    with pytest.raises(InstallError, match="did not become readable within"):
        AliasExecutor().execute(_action(), _payload(), context)


def test_a_files_alias_needs_no_readability_wait(tmp_path, monkeypatch):
    """A folder is a directory; there is no relation to become addressable."""

    monkeypatch.setattr(alias_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    spark = _LateSpark(failures=99)
    context = _addressable_context(tmp_path, spark, _ShortcutResolver())

    details = AliasExecutor().execute(
        _action(), _payload(area="Files", source_area="Files"), context
    )

    assert not spark.statements
    assert "addressable_after_seconds" not in details


class _NoTransportStore(LocalStore):
    """A store with no link operation, as a OneLake DFS client has none."""

    link = None


def test_an_environment_with_neither_transport_says_so(tmp_path):
    context = _local_context(tmp_path, store=_NoTransportStore())

    with pytest.raises(InstallError, match="neither a OneLake shortcut nor a store link"):
        AliasExecutor().execute(_action(), _payload(), context)


# --- several aliases, one action ----------------------------------------------


def _two_aliases() -> bytes:
    first = json.loads(_payload().decode())["aliases"][0]
    second = dict(first, alias="Lakehouse/Curated/Sales.Second", object="Second")
    return json.dumps({"aliases": [first, second]}).encode("utf-8")


def test_every_shortcut_is_created_before_anything_waits(tmp_path, monkeypatch):
    """The cost of an alias is the wait, so the waits must not serialise.

    Two aliases through one action means one discovery window, not two — which is
    only true if both shortcuts exist before the first read is attempted.
    """

    monkeypatch.setattr(alias_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    events: list[str] = []
    resolver = _ShortcutResolver(events=events)
    spark = _LateSpark(failures=2, events=events)
    context = _addressable_context(tmp_path, spark, resolver)

    details = AliasExecutor().execute(_action(), _two_aliases(), context)

    assert [detail["alias"] for detail in details["aliases"]] == [
        "Lakehouse/Curated/Sales.Landed",
        "Lakehouse/Curated/Sales.Second",
    ]
    # Every create precedes every read: two aliases, one discovery window.
    creates = [index for index, event in enumerate(events) if event == "create"]
    reads = [index for index, event in enumerate(events) if event == "read"]
    assert len(creates) == 2 and reads
    assert max(creates) < min(reads)


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
        spark=None,
        resolver=None,
        store=LocalStore(),
        snapshot=Location("/tmp/snapshot"),
        target=_target(DESTINATION_TARGET_ID, "Reporting_WH"),
        sql=sql,
    )
    payload = json.dumps(
        [
            "create or alter view [Rpt].[A] as select 1 as x;",
            "create or alter view [Rpt].[B] as select 1 as x;",
        ]
    ).encode("utf-8")
    action = BuildAction(
        id="aliases-Warehouse--Reporting",
        kind=CREATE_ALIAS,
        resource_node_id=None,
        executor="tsql_batch",
        payload="aliases.tsql-batch.json",
        payload_sha256="0" * 64,
    )

    details = TSqlBatchExecutor().execute(action, payload, context)

    assert details == {"statements": 2}
    assert sql.scripts == [
        "create or alter view [Rpt].[A] as select 1 as x;",
        "create or alter view [Rpt].[B] as select 1 as x;",
    ]


# --- the endpoint refresh ----------------------------------------------------


def _refresh_action() -> BuildAction:
    return BuildAction(
        id="refresh-sql-endpoint-Lakehouse--Raw",
        kind=REFRESH_SQL_ENDPOINT,
        resource_node_id=None,
        executor="sql_endpoint",
        payload=None,
        payload_sha256=None,
    )


class _RefreshingResolver:
    def __init__(self):
        self.refreshed = []

    def refresh_sql_endpoint_metadata(self, item):
        self.refreshed.append(item.name)
        return {"lakehouse": item.name, "state": "Succeeded"}


def test_the_refresh_asks_the_environment_for_the_batchs_own_lakehouse(tmp_path):
    resolver = _RefreshingResolver()
    context = _local_context(tmp_path, resolver=resolver)

    details = SqlEndpointExecutor().execute(_refresh_action(), None, context)

    assert resolver.refreshed == ["Curated_Dev"]
    assert details["state"] == "Succeeded"


def test_the_emulator_skips_the_refresh_rather_than_inventing_an_equivalent(tmp_path):
    context = _local_context(tmp_path)

    details = SqlEndpointExecutor().execute(_refresh_action(), None, context)

    assert "no SQL analytics endpoint" in details["skipped"]
