"""Materialising an alias, and closing an item with an endpoint refresh."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from support.workspaces import given_resolver, given_workspace

from weaver.build_bundle.executors import AliasExecutor
from weaver.build_bundle.executors import alias as alias_module
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.models import CREATE_ALIAS, InstallAction
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import InstallError
from weaver.spark import FabricSparkTarget
from weaver.store import Entry, FilesystemStore
from weaver.targets import ItemRef

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


def _action() -> InstallAction:
    return InstallAction(
        id="aliases-Lakehouse--Curated",
        kind=CREATE_ALIAS,
        resource_node_id=None,
        executor="alias",
        payload="aliases-Lakehouse--Curated.alias.json",
        payload_sha256="0" * 64,
    )


def _local_context(tmp_path, *, resolver=None, store=None):

    # With a Spark destination, as a real Lakehouse target resolves to. Without
    # one an alias in it cannot even be *named*, which used to go unnoticed
    # because the discovery wait was skipped whenever there was no Spark session.
    destination = replace(
        _target(DESTINATION_TARGET_ID, "Curated_Dev"),
        destination=FabricSparkTarget(workspace="Demo", lakehouse="Curated_Dev"),
    )
    source = _target(SOURCE_TARGET_ID, "Raw_Dev")
    local_resolver = given_resolver(
        workspace=given_workspace(catalogue="Lakehouse/Weaver"),
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
        # A host that can ask Spark and finds the alias readable at once. The
        # Installer supplies this on every host, so a context without it is one
        # nobody would build in production — and the executor says so rather
        # than skipping the wait.
        spark_sql=lambda statement, exact_case=False: [],
        spark_sql_batch=lambda statements, exact_case=False: [],
        resolver=resolver or local_resolver,
        store=chosen_store,
        target=destination,
        targets={DESTINATION_TARGET_ID: destination, SOURCE_TARGET_ID: source},
    )


# --- the emulator: a filesystem link ------------------------------------------


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
        self.inner = None

    def __getattr__(self, name):
        if self.inner is None:
            raise AttributeError(name)
        return getattr(self.inner, name)

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


class _FoldedSourceStore(FilesystemStore):
    """A Fabric estate whose physical table name was folded to lower-case."""

    def exists(self, location):
        return not location.value.endswith("/Customer")

    def list(self, location, *, recursive=False):
        return [Entry(location=location / "customer", is_directory=True)]


def test_a_shortcut_uses_the_source_tables_physical_case(tmp_path):
    resolver = _ShortcutResolver()
    context = _local_context(tmp_path, resolver=resolver, store=_FoldedSourceStore())

    AliasExecutor().execute(_action(), _payload(), context)

    assert resolver.calls[0][-1] == "Tables/Sales/customer"


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

    Doubled as the *capability* the executor asks through rather than as a Spark
    session, because that is now the seam: the alias executor stays on whichever
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
    )
    source = _target(SOURCE_TARGET_ID, "Raw_Dev")
    local_resolver = given_resolver(
        workspace=given_workspace(catalogue="Lakehouse/Weaver"),
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
    assert "`Demo`.`Curated_Dev`.`Sales`.`Landed`" in reads[0]
    assert "addressable_after_seconds" in details


def test_a_shortcut_that_never_becomes_readable_fails_naming_itself(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(alias_module, "ADDRESSABLE_POLL_INTERVAL", 0)
    monkeypatch.setattr(alias_module, "ADDRESSABLE_TIMEOUT", 0)
    context = _addressable_context(
        tmp_path, _LateSpark(failures=99), _ShortcutResolver()
    )

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


class _NoTransportStore(FilesystemStore):
    """A store with no link operation, as a OneLake DFS client has none."""

    link = None


def test_an_environment_that_cannot_create_a_shortcut_says_so(tmp_path):
    """An alias is a OneLake shortcut, so a host that cannot make one cannot
    materialise it — and says which action it could not perform."""

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


def test_the_wait_asks_spark_rather_than_holding_one(tmp_path):
    """A desktop has no Spark session and must still wait for discovery.

    The guard was once ``context.spark is not None``, so a desktop install
    skipped the wait and the next statement to read the alias failed with
    "neither a view nor a table". The context carries no session at all now.
    """

    asked = _LateSpark(failures=1)
    context = _addressable_context(tmp_path, asked, _ShortcutResolver())

    AliasExecutor().execute(_action(), _payload(), context)

    assert asked.statements, "the discovery wait was skipped without a Spark session"
    assert all("LIMIT 0" in statement for statement in asked.statements)


def test_the_probe_carries_weavers_identifier_case(tmp_path):
    """The scope has to travel with the statement: a desktop has no Spark to
    set a conf on, and a probe analysed under the session's default case is a
    different probe."""

    asked = _LateSpark(failures=0)
    context = _addressable_context(tmp_path, asked, _ShortcutResolver())

    AliasExecutor().execute(_action(), _payload(), context)

    assert asked.exact_case and all(asked.exact_case)
