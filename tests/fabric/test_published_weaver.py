"""What the *installed package* has to prove, and nothing more.

Weaver's executors are one implementation. What differs between a desktop run and
a notebook is how they get their capabilities. Prove that once per capability and
every feature stops having to re-prove it — which is what lets the rest of
`tests/fabric` drive real Fabric from the checkout, and lets the ordinary
development loop run without publishing anything.

**Writing these corrected the model they were meant to confirm.** "Same
implementation, different acquisition" is true of only half of it:

``SQL``       same executor; a desktop injects one, `sql_for` opens one on the
              session identity
``Spark``     same executors; the session supplies its own catalogue
``store``     *different classes*. `OneLakeDfsClient` is how a desktop crosses
              into OneLake; `store_for` in a session returns `FabricStore` over
              `notebookutils.fs`
``resolution`` *different classes*. A desktop resolves over REST with
              `DefaultAzureCredential`; a session has no CLI, no IMDS and no
              environment variables, so that construction simply fails there.
              `resolver_for` returns a `FabricSessionResolver` backed by
              `notebookutils`, reaching REST — for a SQL endpoint, a shortcut —
              through a `notebookutils.credentials` token

The second pair matters more, because for them a desktop test proves a *different
class* rather than the same one wired differently. That is why the store and the
resolver get probes of their own rather than being assumed along with SQL.

If any of these regress, the wheel is broken however green the desktop suite is.
"""

from __future__ import annotations

import pytest
from factories import item_id, single_document_repository, warehouse_table

from weaver import ItemRef
from weaver.build_bundle.executors.base import ResolvedTarget
from weaver.build_bundle.prune import read_warehouse_inventory

pytestmark = pytest.mark.published_weaver

#: The Warehouse item the installation probe builds. Its own logical name, so it
#: cannot collide with an estate another module is publishing under the same one.
ITEM = "Warehouse/ParityReporting"


def warehouse_target(warehouse) -> ResolvedTarget:
    """A Warehouse resolves to no Spark address at all — it is reached over TDS."""

    from factories import bound_target

    return ResolvedTarget(
        bound=bound_target(
            id="target-1",
            kind="warehouse",
            item_id=warehouse.item.name,
            logical_item_name="ParityReporting",
            logical_item_type="Warehouse",
        ),
        lakehouse=ItemRef(warehouse.item.name),
        location=None,
        destination=None,
    )


def test_the_installed_package_imports_and_reports_a_version(livy_session):
    """The precondition for every other claim in this file."""

    payload = livy_session.run(
        "from importlib.metadata import version\n"
        "emit({'attr': weaver.__version__, 'dist': version('weaverstack')})\n",
        label="parity: import",
    ).payload

    assert payload["attr"] == payload["dist"]
    assert payload["dist"]


def test_the_session_resolver_reaches_rest_on_the_sessions_own_identity(
    livy_session, fabric_workspace, fabric_target_lakehouse, clean_disposable_warehouse
):
    """Resolution in a session is a *different class*, not a different credential.

    Writing this probe corrected the model it was meant to confirm. A desktop
    resolves over REST with `FabricClient` and `DefaultAzureCredential`; a session
    has no CLI, no IMDS and no environment variables, so that construction simply
    fails there. `resolver_for` returns a `FabricSessionResolver` backed by
    `notebookutils` instead, and reaches REST — where it must, as for a SQL
    endpoint — through a token from `notebookutils.credentials`.

    So this belongs with the store rather than with SQL and Spark: two of the
    four capabilities are the same implementation acquired differently, and two
    are different implementations altogether. Only the wheel can speak for these.
    """

    payload = livy_session.run(
        "from weaver import FabricWorkspace, ItemRef, WarehouseTarget\n"
        "from weaver.resolution import resolver_for\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "resolver = resolver_for(workspace)\n"
        f"target = ItemRef({fabric_target_lakehouse.name!r})\n"
        # notebookutils resolution first...
        "root = resolver.lakehouse(target).value\n"
        # ...then a Warehouse SQL endpoint, which the session resolver can only
        # answer by opening REST on a `notebookutils.credentials` token.
        "endpoint = resolver.sql_endpoint(WarehouseTarget(warehouse=ItemRef("
        f"{clean_disposable_warehouse.item.name!r})))\n"
        "emit({'kind': type(resolver).__name__, 'root': root,\n"
        "      'endpoint': bool(endpoint)})\n",
        label="parity: REST",
    ).payload

    # Named, so a regression to the desktop resolver would be loud rather than
    # quietly passing on a credential this process happens to have.
    assert payload["kind"] == "FabricSessionResolver"
    assert payload["root"]
    assert payload["endpoint"], "the session could not reach REST on its own identity"


def test_a_sql_executor_is_acquired_from_the_session_and_runs(
    livy_session, fabric_workspace, clean_disposable_warehouse
):
    """`sql_for` opens a Warehouse connection on the session's own identity.

    Only a desktop caller injects `desktop_sql_executor`; in a session the
    installed package must reach the Warehouse itself. Everything the T-SQL
    executors *do* is proven from the checkout in `test_warehouse_boundary.py`.
    """

    warehouse = clean_disposable_warehouse
    payload = livy_session.run(
        "from weaver import FabricWorkspace, ItemRef\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.build_bundle import InstallationEnvironment\n"
        "from weaver.build_bundle.targets import BoundTarget\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "environment = InstallationEnvironment(store=store_for(workspace), "
        "resolver=resolver_for(workspace), spark=spark, workspace=workspace)\n"
        "bound = BoundTarget(id='wh', kind='warehouse', "
        f"item_id={warehouse.item.name!r})\n"
        "sql = environment.sql_for(bound)\n"
        "rows = sql.query('select 1 as n')\n"
        "emit({'acquired': sql is not None, 'n': rows[0]['n']})\n",
        label="parity: SQL",
    ).payload

    assert payload["acquired"] is True
    assert payload["n"] == 1


def test_a_spark_executor_runs_one_action_in_the_session(
    livy_session, fabric_workspace, fabric_target_lakehouse
):
    """One real action, through the real executor, on the session's own Spark.

    Not what the statement says — that is checked from the checkout and on local
    Spark. What only the wheel can answer is that an installed executor, given a
    context the installed package assembled, reaches the session's catalogue at
    all.
    """

    payload = livy_session.run(
        "from weaver import FabricWorkspace, ItemRef\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.build_bundle import InstallationEnvironment, execute_action\n"
        "from weaver.build_bundle.models import BuildAction\n"
        "from weaver.build_bundle.targets import BoundTarget\n"
        "from weaver.build_bundle.executors.base import InstallationContext\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "environment = InstallationEnvironment(store=store, resolver=resolver, "
        "spark=spark, workspace=workspace)\n"
        "bound = BoundTarget(id='lh', kind='lakehouse', "
        f"item_id={fabric_target_lakehouse.name!r})\n"
        "target = environment.resolve_target(bound)\n"
        "context = InstallationContext(spark=spark, resolver=resolver, store=store,\n"
        "    snapshot=resolver.weaver_items_root, target=target, targets={'lh': target})\n"
        "action = BuildAction(id='parity', kind='create_schema', "
        "resource_node_id=None, executor='spark_schema', payload='p.json', "
        "payload_sha256=None)\n"
        "import json\n"
        # The executor creates without IF NOT EXISTS, so the probe starts from
        # nothing and clears up after itself — it must be able to run twice.
        "spark.sql('DROP SCHEMA IF EXISTS ' + "
        "target.destination.qualified_schema('Parity') + ' CASCADE')\n"
        "result = execute_action(action, json.dumps({'schema': 'Parity'}).encode(), "
        "context=context)\n"
        "seen = {'status': result.status, 'error': result.error_message,\n"
        "        'details': result.details}\n"
        "spark.sql('DROP SCHEMA IF EXISTS ' + "
        "target.destination.qualified_schema('Parity') + ' CASCADE')\n"
        "emit(seen)\n",
        label="parity: Spark",
    ).payload

    assert payload["status"] == "succeeded", payload["error"]
    # The executor reported the destination it actually addressed, which is the
    # part an installed package assembles for itself.
    assert payload["details"]["destination"]


def test_the_session_native_store_reads_back_what_it_wrote(
    livy_session, fabric_workspace, fabric_target_lakehouse
):
    """`FabricStore` over `notebookutils.fs` — a different class, not a different
    credential.

    This is the capability the parity argument is weakest about, and the reason
    it gets its own probe. `OneLakeDfsClient` is how a *desktop* crosses into
    OneLake; `store_for(FabricWorkspace)` inside a session returns something
    else entirely. Exercising the DFS client from the checkout says nothing about
    whether the session-native one writes, lists and deletes the same way — and
    the folder executor and the Files-area alias both go through it.
    """

    payload = livy_session.run(
        "from weaver import FabricWorkspace, ItemRef\n"
        "from weaver.resolution import resolver_for, store_for\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        f"target = ItemRef({fabric_target_lakehouse.name!r})\n"
        "root = resolver.files_root(target) / 'Parity'\n"
        "store.write(root / 'probe.txt', b'parity\\n')\n"
        "seen = {\n"
        "    'kind': type(store).__name__,\n"
        "    'exists': store.exists(root / 'probe.txt'),\n"
        "    'listed': sorted(e.name for e in store.list(root)),\n"
        "    'read': store.read(root / 'probe.txt').decode(),\n"
        "}\n"
        "store.delete(root, recursive=True)\n"
        "seen['removed'] = not store.exists(root / 'probe.txt')\n"
        "emit(seen)\n",
        label="parity: store",
    ).payload

    # Named so a regression to the desktop client would be obvious rather than
    # silently passing: this probe exists precisely because they are not the same.
    assert payload["kind"] != "OneLakeDfsClient"
    assert payload["exists"] is True
    assert payload["listed"] == ["probe.txt"]
    assert payload["read"] == "parity\n"
    assert payload["removed"] is True


# --- and one whole installation, composed by the installed package ------------
#
# The probes above check each capability is acquirable. This checks they compose:
# a bundle generated on the desktop, installed by the wheel, with every
# capability the install needs acquired from the session's own identity. It is
# the difference between "SQL can be opened" and "an installation that needs SQL,
# a store and a resolver completes".


def test_a_locally_generated_bundle_installs_inside_fabric(
    tmp_path, fabric_workspace, clean_disposable_warehouse, livy_session
):
    """Weaver installing a Warehouse from inside a session, on its own identity.

    The generation is local because it is pure Python and costs nothing there.
    The installation is remote because that is where the claim lives: the frozen
    T-SQL runs through the session's Fabric-native connector, not through a
    connection this process opened.

    The same body reads the inventory back Fabric-natively, so one call answers
    both — did the install work, and does an in-session read agree with the
    desktop read the tests above rely on.
    """

    from weaver import wipe_sql_target
    from weaver.build_bundle import generate_item_build_bundle
    from weaver.declaration import parse_item_repository
    from weaver.fabric import FabricResolver, OneLakeDfsClient
    from factories import FixtureCatalogue, item_bindings

    resolver = FabricResolver(fabric_workspace)
    store = OneLakeDfsClient()
    warehouse = clean_disposable_warehouse

    # This test installs from nothing, so it starts from nothing. The Warehouse
    # is emptied per *module*, and the estate fixture above has already built
    # into it — without this the install would be asked to create a table that
    # exists, and the test would fail for a reason that is not its subject.
    # Found by exactly that: it passed alone and failed in the suite.
    wipe_sql_target(warehouse.target, warehouse.workspace, sql=warehouse.executor)

    # The declaration goes into the Weaver Lakehouse, as a user's would.
    root = resolver.weaver_items_root
    if store.exists(root):
        store.delete(root, recursive=True)
    local = tmp_path / "repo"
    single_document_repository(
        local,
        item=ITEM,
        documents={
            "DWG.Customer.sql": warehouse_table(
                "DWG.Customer",
                select="select cast(1 as int) as CustomerId",
            )
        },
    )
    for path in sorted(local.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            store.write(root.join(*path.relative_to(local).parts), path.read_bytes())

    repository = parse_item_repository(root, store=store)
    bindings = item_bindings((ITEM, warehouse.item.name))
    from weaver.build_bundle import LakehouseBinding, effective_item_bindings

    bindings = effective_item_bindings(
        bindings, weaver_lakehouse=fabric_workspace.weaver_lakehouse
    )
    inventory = read_warehouse_inventory(
        warehouse_target(warehouse).bound, sql=warehouse.executor
    )
    item = item_id(ITEM)
    # Each item's inventory must carry *its own* bound target id — the planner
    # checks that pairing, and the control item's id is nothing like the
    # Warehouse's. Deriving it from the binding rather than defaulting is the
    # difference between a prepared inventory and a plausible-looking one.
    inventories = {}
    for binding in bindings.entries:
        bound = binding.to_bound_target()
        if binding.item == item:
            inventories[binding.item] = read_warehouse_inventory(
                bound, sql=warehouse.executor
            )
        else:
            # The control Lakehouse is read for real, over OneLake from here.
            # An empty inventory would be a lie rather than a simplification —
            # the catalogue schema is already there, and claiming otherwise makes
            # the planner emit a create that the session then rejects.
            from weaver.build_bundle.prune import read_lakehouse_inventory

            inventories[binding.item] = read_lakehouse_inventory(
                bound, resolver=resolver, store=store
            )
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=resolver.build_bundle("whrow3"),
        store=store,
        target_inventories=inventories,
        # The control item's own catalogue documents are already installed, so
        # the catalogue must say so — otherwise they look new, the build tries to
        # create them again, and the session rejects tables that exist. Nothing
        # is certified for the Warehouse item, which is what makes it build.
        catalogue=FixtureCatalogue.from_repository(
            repository, item="Lakehouse/_weaver"
        ),
        control_lakehouse=LakehouseBinding(
            lakehouse=ItemRef(fabric_workspace.weaver_lakehouse)
        ),
    )

    payload = livy_session.run(
        "from weaver import FabricWorkspace, ItemRef\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.build_bundle import (InstallationEnvironment, install_bundle, "
        "load_bundle)\n"
        "from weaver.build_bundle.prune import read_warehouse_inventory\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "environment = InstallationEnvironment("
        "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
        f"bundle = load_bundle(resolver.build_bundle('whrow3'), store=store)\n"
        "report = install_bundle(bundle, environment=environment)\n"
        # The same session, its own identity, reading the target back.
        "target = next(t for t in bundle.plan.targets if t.kind == 'warehouse')\n"
        "sql = environment.sql_for(target)\n"
        "seen = read_warehouse_inventory(target, sql=sql)\n"
        "emit({'status': report.status,\n"
        "      'errors': {a.action_id: a.error_message\n"
        "                 for a in report.action_results() if a.error_type},\n"
        "      'tables': list(seen.tables), 'schemas': list(seen.schemas)})\n",
        label="install in session",
    ).payload

    assert payload["status"] == "succeeded", payload["errors"]
    # The in-session read agrees with the desktop read the rest of this file uses.
    assert "DWG.Customer" in payload["tables"] or "dwg.customer" in {
        name.casefold() for name in payload["tables"]
    }
