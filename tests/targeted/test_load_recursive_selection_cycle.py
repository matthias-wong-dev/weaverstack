"""Recursive Load selection through public invocation and the Runner."""

from __future__ import annotations

import pytest
from factories import (
    _write,
    folder_document,
    installed_catalogue,
    item_bindings,
    lakehouse_table,
    logical_shortcuts,
    schema_document,
    single_document_repository,
    warehouse_table,
    warehouse_view,
)
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

import weaver
from weaver.catalogue.state import Catalogue
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.errors import CommandError, GraphError, LoadError
from weaver.load_plan import ENDPOINT_REFRESH, ONELAKE_PUBLICATION, LoadDag
from weaver.load_report import BLOCKED, FAILED, PENDING, SUCCEEDED, LoadResult
from weaver.locations import Location
from weaver.run import Runner, RunRequest, RunState
from weaver.workspaces import ExecutionSettings, RunConcurrency

ITEM = "Warehouse/Reporting"
TARGET = "Reporting_WH"


def installed(root, reads):
    documents = {
        f"Sales.{name}.sql": warehouse_table(
            f"Sales.{name}",
            select=(
                "select CustomerId from "
                + " cross join ".join(f"[Sales].[{producer}]" for producer in producers)
                if producers
                else "select cast(1 as int) as CustomerId"
            ),
        )
        for name, producers in reads.items()
    }
    repository = single_document_repository(
        root, item=ITEM, schemas=("Sales",), documents=documents
    )
    return installed_catalogue(repository, item_bindings((ITEM, TARGET)))


def prepare(monkeypatch, catalogue, *, answers=None, transport=False):
    import weaver.run
    from weaver.run import state

    workspace = given_workspace(
        catalogue="Warehouse/Weaver",
        execution=ExecutionSettings(run=RunConcurrency(warehouse_concurrency=1)),
    )
    targets = catalogue.dag().installations.values()
    session = given_session(
        workspace=workspace,
        lakehouses=tuple(t.name for t in targets if t.is_lakehouse),
        warehouses=("Weaver", *(t.name for t in targets if t.kind == "warehouse")),
        executes_here=True,
    )
    from weaver.catalogue.writer import writer_for

    catalogue = Catalogue.from_mapping(
        catalogue.to_mapping(), writer=writer_for(session)
    )
    calls = []
    requests = []
    execute_run = session.execute_run

    def execute(run, *, workspace):
        arguments = run.arguments()
        requests.append(arguments["request"])
        if transport:
            return run.decode(
                run.entry(session=session, workspace=workspace, **arguments)
            )
        return execute_run(run, workspace=workspace)

    def dispatch(node, **asked):
        name = (
            node.logical_id.object_id.object if node.logical_id else node.primitive_kind
        )
        calls.append(name)
        answer = (answers or {}).get(name, LoadResult(succeeded=True))
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(state, "read_installed_catalogue", lambda **asked: catalogue)
    monkeypatch.setattr(weaver.run, "dispatch_primitive", dispatch)
    monkeypatch.setattr(session, "execute_run", execute)
    return session, calls, requests


def invoke(monkeypatch, catalogue, *, names=None, items=ITEM, **policy):
    session, calls, _requests = prepare(monkeypatch, catalogue)
    report = weaver.load(items, names=names, session=session, **policy)
    return report, calls


@weaver_test()
def test_recursive_ancestors_execute_the_linear_chain_in_order(tmp_path, monkeypatch):
    catalogue = installed(
        tmp_path / "repository",
        {"Source": (), "Middle": ("Source",), "Final": ("Middle",), "Other": ()},
    )

    report, calls = invoke(monkeypatch, catalogue, names="Sales.Final", ancestors=True)

    assert calls == ["Source", "Middle", "Final"]
    assert report.succeeded
    assert len(report.edges) == 2


@weaver_test()
@pytest.mark.parametrize(
    "names,policy,expected",
    [
        ("Sales.Source", {"descendants": True}, ["Source", "Middle", "Final"]),
        ("Sales.Middle", {"ancestors": True}, ["Source", "Middle"]),
        ("Sales.Middle", {"descendants": True}, ["Middle", "Final"]),
        ("Sales.Other", {"ancestors": True, "descendants": True}, ["Other"]),
        (None, {"ancestors": True}, ["Source", "Middle", "Other", "Final"]),
    ],
)
def test_recursive_linear_selection_executes_only_the_selected_union(
    tmp_path, monkeypatch, names, policy, expected
):
    catalogue = installed(
        tmp_path / "repository",
        {"Source": (), "Middle": ("Source",), "Final": ("Middle",), "Other": ()},
    )

    report, calls = invoke(monkeypatch, catalogue, names=names, **policy)

    assert calls == expected
    assert report.succeeded
    assert {
        WeaverDocumentId.parse(node.logical_id).object_id.object
        for node in report.nodes
    } == set(expected)


@weaver_test()
@pytest.mark.parametrize(
    "direction,seed", [("ancestors", "Final"), ("descendants", "Source")]
)
def test_recursive_diamond_deduplicates_and_orders_dispatch(
    tmp_path, monkeypatch, direction, seed
):
    catalogue = installed(
        tmp_path / "repository",
        {
            "Source": (),
            "Left": ("Source",),
            "Right": ("Source",),
            "Final": ("Left", "Right"),
            "Other": (),
        },
    )

    report, calls = invoke(
        monkeypatch, catalogue, names=f"Sales.{seed}", **{direction: True}
    )

    assert calls == ["Source", "Left", "Right", "Final"]
    assert len(report.edges) == 4
    assert report.succeeded


@weaver_test()
def test_both_directions_expand_original_seeds_separately(tmp_path, monkeypatch):
    catalogue = installed(
        tmp_path / "repository",
        {
            "Root": (),
            "Seed": ("Root",),
            "Sibling": ("Root",),
            "Child": ("Seed", "OtherParent"),
            "OtherParent": (),
            "Leaf": ("Child",),
            "Independent": (),
        },
    )

    report, calls = invoke(
        monkeypatch,
        catalogue,
        names=r"sales\.se.d",
        ancestors=True,
        descendants=True,
    )

    assert calls == ["Root", "Seed", "Child", "Leaf"]
    assert report.succeeded
    assert len(report.edges) == 3
    assert "Sibling" not in calls
    assert "OtherParent" not in calls


@weaver_test()
def test_multiple_regex_seeds_expand_their_union_once(tmp_path, monkeypatch):
    catalogue = installed(
        tmp_path / "repository",
        {
            "Root": (),
            "Left": ("Root",),
            "Right": ("Root",),
            "LeftChild": ("Left",),
            "RightChild": ("Right",),
            "Other": (),
        },
    )

    report, calls = invoke(
        monkeypatch,
        catalogue,
        names=[r"Sales\.(Left|Right)", "sales.left"],
        ancestors=True,
        descendants=True,
    )

    assert calls == ["Root", "Left", "Right", "LeftChild", "RightChild"]
    assert len(report.nodes) == 5
    assert len(report.edges) == 4


@weaver_test()
def test_plain_names_remain_an_unordered_operator_override(tmp_path, monkeypatch):
    catalogue = installed(
        tmp_path / "repository",
        {"Source": (), "Middle": ("Source",), "Final": ("Middle",)},
    )

    report, calls = invoke(
        monkeypatch, catalogue, names=["Sales.Middle", "Sales.Final"]
    )

    assert calls == ["Final", "Middle"]
    assert report.edges == ()


@weaver_test()
@pytest.mark.parametrize("direction", ["ancestors", "descendants"])
def test_no_matching_seed_refuses_before_dispatch_or_recording(
    tmp_path, monkeypatch, direction
):
    catalogue = installed(tmp_path / "repository", {"Source": ()})
    session, calls, requests = prepare(monkeypatch, catalogue)

    with pytest.raises(LoadError, match="matches 'Sales.Missing'"):
        weaver.load(ITEM, names="Sales.Missing", session=session, **{direction: True})

    assert calls == []
    assert requests == []
    assert not session.calls


@weaver_test()
def test_expanded_dry_run_dispatches_and_records_nothing(tmp_path, monkeypatch):
    catalogue = installed(tmp_path / "repository", {"Source": (), "Final": ("Source",)})
    session, calls, requests = prepare(monkeypatch, catalogue)
    before = catalogue.to_mapping()

    report = weaver.load(
        ITEM,
        names="Sales.Final",
        ancestors=True,
        descendants=True,
        dry_run=True,
        session=session,
    )

    assert report.succeeded
    assert len(report.nodes) == 2
    assert len(report.edges) == 1
    assert not any(node.executed for node in report.nodes)
    assert calls == []
    assert not session.calls
    assert report.workflow_id is None
    assert catalogue.to_mapping() == before
    assert requests[0]["ancestors"] and requests[0]["descendants"]


@weaver_test()
@pytest.mark.parametrize("fault_tolerant", [False, True])
@pytest.mark.parametrize(
    "failure",
    [
        LoadResult(succeeded=False, error_message="source failed"),
        RuntimeError("source failed"),
    ],
)
def test_expanded_runner_retains_failure_blocking_and_fault_tolerance(
    tmp_path, monkeypatch, fault_tolerant, failure
):
    catalogue = installed(
        tmp_path / "repository",
        {"Source": (), "Child": ("Source",), "Final": ("Child",), "Other": ()},
    )
    session, calls, _requests = prepare(
        monkeypatch, catalogue, answers={"Source": failure}
    )
    policy = dict(
        names=["Sales.Source", "Sales.Other"],
        descendants=True,
        fault_tolerant=fault_tolerant,
        session=session,
    )

    if fault_tolerant:
        report = weaver.load(ITEM, **policy)
    else:
        with pytest.raises(LoadError, match="source failed") as raised:
            weaver.load(ITEM, **policy)
        report = raised.value.report

    statuses = {
        WeaverDocumentId.parse(node.logical_id).object_id.object: node.status
        for node in report.nodes
    }
    assert statuses == {
        "Source": FAILED,
        "Child": SUCCEEDED if fault_tolerant else BLOCKED,
        "Final": SUCCEEDED if fault_tolerant else BLOCKED,
        "Other": SUCCEEDED if fault_tolerant else PENDING,
    }
    assert calls == (
        ["Source", "Child", "Other", "Final"] if fault_tolerant else ["Source"]
    )
    assert not report.succeeded
    session.flush()
    recorded = "\n".join(
        statement
        for call in session.calls
        if call.kind == "tsql"
        for statement in call.body
    )
    assert "INSERT INTO [_].[Log]" in recorded
    assert "source failed" in recorded
    for node in report.nodes:
        assert node.node_id in recorded


@weaver_test()
def test_expansion_passes_through_a_nonloadable_view(tmp_path, monkeypatch):
    repository = single_document_repository(
        tmp_path / "repository",
        item=ITEM,
        schemas=("Sales",),
        documents={
            "Sales.Source.sql": warehouse_table("Sales.Source"),
            "Sales.View.sql": warehouse_view(
                "Sales.View",
                select="select CustomerId from [Sales].[Source]",
                depends_on="Sales.Source",
            ),
            "Sales.Final.sql": warehouse_table(
                "Sales.Final", select="select CustomerId from [Sales].[View]"
            ),
        },
    )
    catalogue = installed_catalogue(repository, item_bindings((ITEM, TARGET)))

    report, calls = invoke(monkeypatch, catalogue, names="Sales.Final", ancestors=True)

    assert calls == ["Source", "Final"]
    assert len(report.edges) == 1


@weaver_test()
def test_transport_roundtrip_preserves_both_expansion_flags(tmp_path, monkeypatch):
    catalogue = installed(
        tmp_path / "repository",
        {"Source": (), "Middle": ("Source",), "Final": ("Middle",), "Other": ()},
    )
    session, calls, requests = prepare(monkeypatch, catalogue, transport=True)

    report = weaver.load(
        ITEM, names="Sales.Middle", ancestors=True, descendants=True, session=session
    )

    assert calls == ["Source", "Middle", "Final"]
    assert report.succeeded
    assert requests[0]["ancestors"] and requests[0]["descendants"]
    request = RunRequest.from_mapping(requests[0])
    assert request.to_mapping() == requests[0]
    legacy = {
        key: value
        for key, value in requests[0].items()
        if key not in ("ancestors", "descendants")
    }
    restored = RunRequest.from_mapping(legacy)
    assert not restored.ancestors and not restored.descendants


@weaver_test()
@pytest.mark.parametrize("direction", ["ancestors", "descendants"])
def test_expansion_policy_remains_load_only(direction):
    with pytest.raises(CommandError, match="apply only to loads"):
        RunRequest.test((WeaverItemId.parse(ITEM),), **{direction: True})


@weaver_test()
@pytest.mark.parametrize("direction", ["ancestors", "descendants"])
def test_expansion_requires_an_installed_catalogue(direction):
    session = given_session(workspace=given_workspace(catalogue=None))

    with pytest.raises(CommandError, match="need a Weaver catalogue"):
        weaver.load("SemanticModel/Reporting", session=session, **{direction: True})

    assert not session.calls


@weaver_test()
@pytest.mark.parametrize("selected", [(), ("Warehouse/Reporting/Sales.Missing",)])
def test_empty_or_stale_identity_seed_selection_remains_empty(tmp_path, selected):
    catalogue = installed(tmp_path / "repository", {"Source": (), "Final": ("Source",)})
    request = RunRequest.load(
        (WeaverItemId.parse(ITEM),),
        selected=tuple(WeaverDocumentId.parse(one) for one in selected),
        ancestors=True,
        descendants=True,
    )
    calls = []

    result = Runner(RunState(catalogue), request).run(
        dispatch=lambda node, **asked: calls.append(node)
    )

    assert result.nodes == ()
    assert result.edges == ()
    assert calls == []


@weaver_test()
def test_cycle_refuses_expansion_before_execution(tmp_path, monkeypatch):
    catalogue = installed(tmp_path / "repository", {"Source": (), "Final": ("Source",)})
    session, calls, requests = prepare(monkeypatch, catalogue)
    rows = catalogue.to_mapping()
    tables = next(entry["tables"] for entry in rows["items"] if entry["item"] == ITEM)
    dependency = dict(tables["Dependency"][0])
    dependency.update(
        referencing_object_name="Source",
        dependency_reference="Sales.Final",
        referenced_object_name="Final",
    )
    tables["Dependency"].append(dependency)
    cyclic = Catalogue.from_mapping(rows)
    from weaver.run import state

    monkeypatch.setattr(state, "read_installed_catalogue", lambda **asked: cyclic)

    with pytest.raises(GraphError, match="dependency cycle"):
        weaver.load(ITEM, names="Sales.Final", ancestors=True, session=session)

    assert calls == []
    assert requests == []
    assert not session.calls


def crossing_catalogue(root, *, reverse=False, shared=False, hop=False):
    source = "Warehouse/Source" if reverse else "Lakehouse/Source"
    consumer = "Lakehouse/Consumer" if reverse else "Warehouse/Consumer"
    third = "Lakehouse/Unrelated"
    for item in (source, consumer, third):
        _write(root, f"{item}/schemas/Sales.yml", schema_document("Sales"))
    for item, name in ((source, "Source"), (third, "Other")):
        path = (
            f"{item}/Sales.{name}.sql"
            if item.startswith("Warehouse/")
            else f"{item}/Tables/Sales__{name}.py"
        )
        _write(
            root,
            path,
            warehouse_table(f"Sales.{name}")
            if item.startswith("Warehouse/")
            else lakehouse_table(f"Sales.{name}"),
        )
    reference = (
        f"{source}/{'Tables/' if source.startswith('Lakehouse/') else ''}Sales.Source"
    )
    if hop:
        bridge = "Lakehouse/Bridge"
        _write(root, f"{bridge}/schemas/Sales.yml", schema_document("Sales"))
        path, text = logical_shortcuts(bridge, **{"Sales.Read": reference})
        _write(root, path, text)
        reference = f"{bridge}/Tables/Sales.Read"
    shortcut_path, shortcut_text = logical_shortcuts(
        consumer, **{"Sales.Read": reference}
    )
    _write(root, shortcut_path, shortcut_text)
    if reverse:
        body = lakehouse_table("Sales.Final").replace(
            "from weaver import Table",
            "from shortcuts import Sales__Read\nfrom weaver import Table",
        )
        _write(root, f"{consumer}/Tables/Sales__Final.py", body)
    else:
        _write(
            root,
            f"{consumer}/Sales.Final.sql",
            warehouse_table(
                "Sales.Final", select="select CustomerId from [Sales].[Read]"
            ),
        )
    repository = parse_item_repository(Location(str(root)))
    bindings = item_bindings(
        (source, "Source_WH" if reverse else "Source_LH"),
        (consumer, "Consumer_LH" if reverse else "Consumer_WH"),
        (third, "Other_LH"),
        *(("Lakehouse/Bridge", "Bridge_LH"),) if hop else (),
    )
    catalogue = installed_catalogue(repository, bindings)
    if shared:
        rows = catalogue.to_mapping()
        for entry in rows["items"]:
            if entry["item"] in (source, third):
                entry["tables"]["Installation"][0]["target_name"] = "Shared_LH"
            if entry["item"] == third:
                entry["tables"]["Registry"] = [
                    row
                    for row in entry["tables"]["Registry"]
                    if row["object_name"] == "Other" or row["object_role"] == "load"
                ]
        catalogue = Catalogue.from_mapping(rows)
    return catalogue, source, consumer


@weaver_test()
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("direction", ["ancestors", "descendants"])
def test_shortcut_expansion_keeps_endpoint_and_publication_barriers(
    tmp_path, monkeypatch, reverse, direction
):
    catalogue, source, consumer = crossing_catalogue(
        tmp_path / "repository", reverse=reverse
    )
    seed = "Sales.Final" if direction == "ancestors" else "Sales.Source"

    report, calls = invoke(
        monkeypatch,
        catalogue,
        items=(source, consumer),
        names=seed,
        **{direction: True},
    )

    barrier = ONELAKE_PUBLICATION if reverse else ENDPOINT_REFRESH
    assert calls == ["Source", barrier, "Final"]
    assert len(report.edges) == 2
    waiting = next(node for node in report.nodes if node.primitive_kind == barrier)
    assert waiting.logical_id is None
    assert report.succeeded
    planned = LoadDag.from_catalogue(
        catalogue,
        items=tuple(WeaverItemId.parse(one) for one in (source, consumer)),
        names=(seed,),
        **{direction: True},
    )
    waiting = next(node for node in planned.nodes if node.primitive_kind == barrier)
    if reverse:
        assert waiting.publication_of == WeaverDocumentId.parse(
            f"{source}/Sales.Source"
        )
        assert [(one.schema, one.object) for one in waiting.publication_targets] == [
            ("Sales", "Read")
        ]
    else:
        assert waiting.refresh_tables == (("Sales", "Source"),)


@weaver_test()
@pytest.mark.parametrize("direction", ["ancestors", "descendants"])
def test_explicit_items_bound_crossings_even_on_a_shared_target(
    tmp_path, monkeypatch, direction
):
    catalogue, source, consumer = crossing_catalogue(
        tmp_path / "repository", shared=True
    )
    item = consumer if direction == "ancestors" else source
    name = "Final" if direction == "ancestors" else "Source"

    report, calls = invoke(
        monkeypatch, catalogue, items=item, names=f"Sales.{name}", **{direction: True}
    )

    assert calls == [name]
    assert report.edges == ()
    assert tuple(
        str(WeaverDocumentId.parse(node.logical_id).item) for node in report.nodes
    ) == (item,)


@weaver_test()
def test_shared_target_does_not_include_an_unrelated_logical_item(
    tmp_path, monkeypatch
):
    catalogue, source, consumer = crossing_catalogue(
        tmp_path / "repository", shared=True
    )

    report, calls = invoke(
        monkeypatch,
        catalogue,
        items=(source, consumer),
        names="Sales.Final",
        ancestors=True,
        descendants=True,
    )

    assert calls == ["Source", ENDPOINT_REFRESH, "Final"]
    assert {
        str(WeaverDocumentId.parse(node.logical_id).item)
        for node in report.nodes
        if node.logical_id
    } == {source, consumer}


@weaver_test()
@pytest.mark.parametrize(
    "direction,seed",
    [("ancestors", "Tables/Sales.Customer"), ("descendants", "Files/Sales.Customer")],
)
def test_typed_area_collision_keeps_distinct_seeds_and_dispatch(
    tmp_path, monkeypatch, direction, seed
):
    item = "Lakehouse/Landing"
    table = lakehouse_table("Sales.Customer").replace(
        "from weaver import Table",
        "from Files.Sales__Customer import Sales__Customer as FilesCustomer\nfrom weaver import Table",
    )
    repository = single_document_repository(
        tmp_path / "repository",
        item=item,
        schemas=("Sales",),
        documents={
            "Files/Sales__Customer.py": folder_document("Sales.Customer"),
            "Tables/Sales__Customer.py": table,
            "Tables/Sales__Other.py": lakehouse_table("Sales.Other"),
        },
    )
    catalogue = installed_catalogue(repository, item_bindings((item, "Landing_LH")))

    report, calls = invoke(
        monkeypatch, catalogue, items=item, names=seed, **{direction: True}
    )

    assert calls == ["Customer", "Customer"]
    assert report.order == (
        "load:Lakehouse/Landing_LH/Files/Sales.Customer",
        "load:Lakehouse/Landing_LH/Tables/Sales.Customer",
    )
    assert len(report.edges) == 1
    with pytest.raises(LoadError, match="Choose an area-qualified name"):
        invoke(
            monkeypatch,
            catalogue,
            items=item,
            names="Sales.Customer",
            **{direction: True},
        )


@weaver_test()
@pytest.mark.parametrize(
    "source_age,final_age,policy,expected",
    [
        (1, None, {"ancestors": True}, ["Source", "Final"]),
        (48, 1, {"descendants": True}, ["Source", "Final"]),
        (1, 1, {"ancestors": True, "descendants": True}, []),
    ],
)
def test_public_stale_selection_supplies_original_seeds(
    monkeypatch, source_age, final_age, policy, expected
):
    from test_health_representation import YESTERDAY, _Estate, at

    catalogue = (
        _Estate()
        .table(f"{ITEM}/Sales.Source", loaded=at(source_age), moved=at(source_age))
        .table(
            f"{ITEM}/Sales.Final",
            loaded=at(final_age) if final_age else None,
            moved=at(final_age) if final_age else None,
        )
        .reads(f"{ITEM}/Sales.Final", "Sales.Source")
        .catalogue()
    )

    report, calls = invoke(
        monkeypatch, catalogue, stale=True, as_of=YESTERDAY, **policy
    )

    assert calls == expected
    assert report.succeeded
    assert len(report.nodes) == len(expected)


@weaver_test()
def test_expanded_reload_resets_load_state_only_for_the_selected_union(
    tmp_path, monkeypatch
):
    catalogue = installed(
        tmp_path / "repository", {"Source": (), "Final": ("Source",), "Other": ()}
    )
    session, calls, _requests = prepare(monkeypatch, catalogue)

    report = weaver.load(
        ITEM, names="Sales.Final", ancestors=True, reload=True, session=session
    )

    assert report.succeeded
    assert report.reload
    assert calls == ["Source", "Final"]
    session.flush()
    statements = "\n".join(
        statement
        for call in session.calls
        if call.kind == "tsql"
        for statement in call.body
    )
    assert "MERGE INTO [_].[Bookmark]" in statements
    assert "N'Source'" in statements
    assert "N'Final'" in statements
    assert "N'Other'" not in statements
    assert statements.count("N'Pending'") == 2


@weaver_test()
@pytest.mark.parametrize(
    "direction,seed", [("ancestors", "Sales.Final"), ("descendants", "Sales.Source")]
)
def test_recursive_selection_retains_multiple_shortcut_hops(
    tmp_path, monkeypatch, direction, seed
):
    catalogue, source, consumer = crossing_catalogue(tmp_path / "repository", hop=True)

    report, calls = invoke(
        monkeypatch,
        catalogue,
        items=(source, "Lakehouse/Bridge", consumer),
        names=seed,
        **{direction: True},
    )

    assert calls == ["Source", ENDPOINT_REFRESH, "Final"]
    assert len(report.edges) == 2
    assert report.succeeded


@weaver_test()
def test_excluded_dependency_keeps_its_unresolved_diagnostic(monkeypatch):
    from test_health_representation import RAW, _Estate

    catalogue = (
        _Estate()
        .table(f"{RAW}/Tables/Sales.Source")
        .table(f"{RAW}/Tables/Sales.Final")
        .reads(f"{RAW}/Tables/Sales.Source", "Sales.Missing")
        .reads(f"{RAW}/Tables/Sales.Final", "Sales.Source")
        .catalogue()
    )
    session, calls, requests = prepare(monkeypatch, catalogue)

    with pytest.raises(LoadError, match="Sales.Missing"):
        weaver.load(RAW, names="Sales.Final", descendants=True, session=session)

    assert not calls and not requests and not session.calls


@weaver_test()
def test_expansion_retains_physical_read_diagnostics_without_managed_edges(
    tmp_path, monkeypatch
):
    from test_load_dag_representation import _same_name_estate

    item = _same_name_estate(tmp_path / "repository")
    repository = parse_item_repository(Location(str(tmp_path / "repository")))
    catalogue = installed_catalogue(repository, item_bindings((item, "Curated_LH")))

    report, calls = invoke(
        monkeypatch,
        catalogue,
        items=item,
        names="Tables/Sales.Customer",
        ancestors=True,
        descendants=True,
    )

    assert calls == ["Customer"]
    assert report.edges == ()
    assert any(
        message.code == "dependency_external"
        and "shortcuts.Sales__Customer" in message.message
        for message in report.messages
    )
