"""Can a bundle actually execute, in its own order, against a real Lakehouse?

Everything else about ordering is proven on paper. `test_item_plan.py` says the
stages come out in the right order; `test_build_installer.py` says the installer
walks them in that order; `test_fixed_point.py` says a correct estate plans
nothing. None of them can say the order is **viable** — that a view really can be
created after its table, that a schema really exists by the time an object lands
in it, that the whole sequence survives an engine.

That question needs one bundle, installed for real, and it is the only reason to
pay for a session here. So this file is one test with everything in it rather
than several with a little each.

**No catalogue.** The bundle carries physical stages only, which is what makes
this about *physicality* and nothing else — no DML against the catalogue,
no claims to reconcile, no publication to interpret. The catalogue's own round
trip is a separate claim with its own tests
(`test_item_catalogue_fabric.py`, `spark/boundary/test_catalogue_fidelity.py`).
A production bundle interleaves those stages; this deliberately does not, so read
the pass as "the physical half installs in order", not "a whole build works".

Distinct from `test_published_weaver.py::test_a_locally_generated_bundle_installs_inside_fabric`,
which installs a *Warehouse* bundle *with* its catalogue. The interesting order
is here: a Warehouse install is a series of T-SQL scripts over one connection,
while a Lakehouse install spans a schema, two ways of making a table, a view over
one of them, directories, and the deployed runtime tree.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from conftest import staged_bundle, staged_bundle_source
from factories import (
    FixtureInventory,
    bound_target,
    folder_document,
    item_id,
    lakehouse_table,
    single_document_repository,
    spark_view,
)

from weaver.build_bundle import (
    BuildPlan,
    compute_bundle_id,
    plan_item_build,
    write_bundle,
)
from weaver.build_bundle.bundle import SUPPORTED_FORMAT_VERSION
from weaver.build_bundle.incremental import BuildSelection, Impact
from weaver.build_bundle.stages import enumerate_stages
from weaver.declaration.metadata import DELTA_TARGET

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]

ITEM = "Lakehouse/Sales"
BUNDLE = "installorder"

QUERY_SHAPED_TABLE = """\
/*
Table ID: DWG.Summary

Description: A table whose shape comes from its query.

Lineage: Declared for a test.

Dependencies: []

Schema:
  CustomerId: string
  Score: double
*/
select cast(null as string) as CustomerId
     , cast(null as double) as Score
 where 1 = 0;
"""


def estate_repository(root: Path):
    """One item holding every physical form a Lakehouse build produces.

    Two kinds of table because they are built two different ways — a Python
    document's declared columns become a plain `CREATE TABLE`, while a Spark SQL
    document's shape is resolved by running its query — and a view over one of
    them, because a view is the thing that fails if the order is wrong.
    """

    return single_document_repository(
        root,
        item=ITEM,
        schemas=("DWG", "Raw"),
        documents={
            "DWG__Customer.py": lakehouse_table("DWG.Customer"),
            "DWG.Summary.sql": QUERY_SHAPED_TABLE,
            "DWG.ActiveCustomer.sql": spark_view(
                "DWG.ActiveCustomer", depends_on="DWG.Customer"
            ),
            "Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv"),
            "lib/dates.py": "def parse(value):\n    return value\n",
        },
    )


def physical_bundle(
    repository,
    *,
    target_name: str,
    staging_name: str,
    workspace_name: str,
    resolver,
    store,
):
    """A bundle of physical stages and nothing else.

    Built from `plan_item_build`, which returns exactly the stages that touch a
    target. A whole-planner bundle would append the catalogue tail, and this test
    is not about that — so rather than generating one and ignoring half of it,
    the half that is the subject is what gets written.
    """

    item = item_id(ITEM)
    # The real workspace's display name, because four-part naming is spelled
    # with it: a plan built with the fixture default would ask Fabric for a
    # workspace that does not exist, and Fabric would rightly refuse it.
    target = bound_target(
        id="target-1", item_id=target_name, workspace_name=workspace_name
    )
    selected = {key for key in repository.source_documents if key.item == item}
    loads = _load_identities(repository, item)
    planned = plan_item_build(
        repository,
        item=item,
        target=target,
        inventory=FixtureInventory(
            target_id="target-1",
            kind="lakehouse",
            target_name=target_name,
        ),
        target_by_item={item: target},
        selected_documents=selected,
        selected_aliases=set(),
        selected_for_drop=set(),
        selected_for_build=selected,
        selected_loads=loads,
        registered={},
    )
    sequences, payloads, target_changes = enumerate_stages(list(planned.stages))
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name=repository.name,
        repository_signature=repository.signature,
        targets=(target,),
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        target_changes=target_changes,
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    location = staged_bundle(resolver, staging_name, BUNDLE)
    if store.exists(location):
        store.delete(location, recursive=True)
    return write_bundle(location, plan=plan, payloads=payloads, store=store)


def _load_identities(repository, item):
    """The item's load artefacts, where this Weaver has them."""

    try:
        from weaver.etl import item_load_artefacts
    except ImportError:  # pragma: no cover - Weaver without a load layer
        return set()
    return {
        artefact.identity for artefact in item_load_artefacts(repository, item=item)
    }


def test_a_whole_bundle_installs_in_its_own_order_against_a_real_lakehouse(
    tmp_path,
    fabric_workspace,
    fabric_alias_lakehouses,
    fabric_staging_lakehouse,
    fabric_empty_lakehouse,
    livy_session,
):
    """The one claim a session is worth paying for, made once.

    Three things at once, because they cost one install between them: the
    sequence runs without failing, it runs in the order the manifest gives, and
    what is left behind is what the source declares.

    The Lakehouse is emptied first and the test says so: an estate that already
    matches would have the planner emit nothing, and a bundle with no actions
    would satisfy every assertion below for entirely the wrong reason.
    """

    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace)
    store = OneLakeDfsClient()
    lakehouse = fabric_alias_lakehouses["producer"]
    fabric_empty_lakehouse(lakehouse.name)

    repository = estate_repository(tmp_path / "repo")
    bundle = physical_bundle(
        repository,
        target_name=lakehouse.name,
        staging_name=fabric_staging_lakehouse.name,
        workspace_name=fabric_workspace.workspace,
        resolver=resolver,
        store=store,
    )
    planned_order = [action.id for _s, _b, action in bundle.plan.actions()]
    assert planned_order, "the bundle planned no physical work to install"

    payload = livy_session.run(
        "from weaver.workspaces import Workspace\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.build_bundle import Installer, load_bundle\n"
        "from weaver.sessions import NotebookSession\n"
        "from weaver.build_bundle.prune import read_lakehouse_inventory\n"
        "from weaver.build_bundle.workflow import session_catalogue\n"
        "from weaver.targets import ItemRef\n"
        f"workspace = Workspace(workspace={fabric_workspace.workspace!r}, "
        f"catalogue={fabric_workspace.catalogue!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "session = NotebookSession(workspace=workspace, spark=spark)\n"
        "installer = Installer(session)\n"
        f"bundle = load_bundle("
        f"{staged_bundle_source(fabric_staging_lakehouse.name, BUNDLE)}, "
        "store=store)\n"
        "report = installer.install(bundle)\n"
        "target = bundle.plan.targets[0]\n"
        "seen = read_lakehouse_inventory(target, resolver=resolver, store=store,\n"
        "    catalogue=session_catalogue(session, workspace, ItemRef(target.item_id)))\n"
        "emit({'status': report.status,\n"
        "      'ran': [a.action_id for a in report.action_results()],\n"
        "      'errors': {a.action_id: a.error_message\n"
        "                 for a in report.action_results() if a.error_type},\n"
        "      'tables': list(seen.tables), 'views': list(seen.views),\n"
        "      'schemas': list(seen.schemas), 'folders': list(seen.folders),\n"
        "      'files': list(seen.files)})\n",
        label="install a whole bundle",
    ).payload

    # 1. It ran, which is the viability claim: every statement was acceptable to
    #    the engine *at the point the sequence reached it*. A view built before
    #    its table would fail here and nowhere earlier.
    assert payload["status"] == "succeeded", payload["errors"]

    # 2. It ran in the order the manifest gives. Without this the pass above
    #    could be true of an installer that reordered freely and got lucky.
    assert payload["ran"] == planned_order

    # 3. And what is there is what the source declares.
    declared = FixtureInventory.from_repository(
        repository,
        item=ITEM,
        target_kind=DELTA_TARGET,
        target_id="target-1",
        kind="lakehouse",
        target_name=lakehouse.name,
        staging_name=fabric_staging_lakehouse.name,
    )
    for field in ("tables", "views", "schemas", "folders", "files"):
        assert _folded(payload[field]) == _folded(getattr(declared, field)), field

    # 4. And the build's own account of what it would do was true of Fabric.
    #    `target_changes` is checked against the actions in pure Python and
    #    applied to inventories there too — but only here is it compared with a
    #    target that really had the actions run against it. A summary that
    #    predicted the right shape and the wrong estate would pass everything
    #    else and fail this.
    predicted = FixtureInventory(
        target_id="target-1", kind="lakehouse", target_name=lakehouse.name
    ).update_using(bundle.plan)
    for field in ("tables", "views", "schemas", "folders", "files"):
        assert _folded(payload[field]) == _folded(getattr(predicted, field)), field


def _folded(names) -> set:
    return {str(name).casefold() for name in names}
