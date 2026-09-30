"""A realistic Lakehouse estate keeps scale evidence reproducible."""

from __future__ import annotations

import ast
import csv
from collections import Counter, defaultdict

import pytest
from factories import FixtureCatalogue, item_bindings, target_inventory
from support.representative_lakehouse_estate import (
    RepresentativeLakehouseSpec,
    make_representative_lakehouse_plan,
    parse_representative_lakehouse_estate,
    qualify_representative_lakehouse_estate,
    write_representative_lakehouse_estate,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    WarehouseBinding,
    effective_item_bindings,
    generate_item_build_bundle,
)
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

_TABLE_ROLES = (
    "SourceEntity",
    "SourceAdjustment",
    "Joined",
    "Aggregate",
    "EntityProjection",
    "AdjustmentProjection",
    "EnrichedEntities",
    "GroupedEntities",
    "WindowedEntities",
    "RankedEntities",
    "AggregateProjection",
    "SharedSummary",
    "SummaryPositive",
    "SummaryAll",
)
_VIEW_ROLES = (
    "ActiveEntities",
    "UnifiedEntities",
    "SummaryUnion",
    "SummaryJoin",
    "TerminalSummary",
    "PublishedBridge",
)
_TEST_ROLES = (
    "AggregateReconciles",
    "JoinedMeasuresMatch",
    "TerminalSummaryMatches",
)
_ASSUMPTION_ROLES = ("NoOrphans", "NonNegative")
_ROLE_KIND = {
    **dict.fromkeys(_TABLE_ROLES, "table"),
    **dict.fromkeys(_VIEW_ROLES, "view"),
    **dict.fromkeys(_TEST_ROLES, "test"),
    **dict.fromkeys(_ASSUMPTION_ROLES, "assumption"),
}
_PYTHON_ROLES = {
    "SourceEntity",
    "SourceAdjustment",
    "Joined",
    "EntityProjection",
    "AdjustmentProjection",
    "EnrichedEntities",
    "RankedEntities",
    "AggregateProjection",
    "SummaryAll",
    "AggregateReconciles",
    "JoinedMeasuresMatch",
    "NoOrphans",
}
_DEPENDENCIES = {
    "Joined": ("SourceEntity", "SourceAdjustment"),
    "Aggregate": ("Joined",),
    "ActiveEntities": ("Joined",),
    "EntityProjection": ("ActiveEntities",),
    "AdjustmentProjection": ("SourceAdjustment",),
    "EnrichedEntities": ("EntityProjection", "AdjustmentProjection"),
    "UnifiedEntities": ("EntityProjection", "AdjustmentProjection"),
    "GroupedEntities": ("EnrichedEntities", "UnifiedEntities"),
    "WindowedEntities": ("EnrichedEntities",),
    "RankedEntities": ("WindowedEntities",),
    "AggregateProjection": ("Aggregate",),
    "SharedSummary": ("GroupedEntities", "AggregateProjection"),
    "SummaryPositive": ("SharedSummary",),
    "SummaryAll": ("SharedSummary",),
    "SummaryUnion": ("SummaryPositive", "SummaryAll"),
    "SummaryJoin": ("SummaryPositive", "SummaryAll"),
    "TerminalSummary": ("SummaryUnion", "SummaryJoin", "RankedEntities"),
    "PublishedBridge": ("TerminalSummary",),
    "AggregateReconciles": ("Joined", "Aggregate"),
    "JoinedMeasuresMatch": ("SourceEntity", "SourceAdjustment", "Joined"),
    "TerminalSummaryMatches": (
        "SummaryUnion",
        "SummaryJoin",
        "RankedEntities",
        "PublishedBridge",
    ),
    "NoOrphans": ("SourceAdjustment", "SourceEntity"),
    "NonNegative": ("TerminalSummary",),
}


def _item(motif: int) -> str:
    suffix = "000" if motif == 0 else "001"
    return f"Lakehouse/Representative{suffix}"


def _identity(motif: int, role: str) -> str:
    kind = _ROLE_KIND[role]
    object_id = f"Scale.M{motif:03d}{role}"
    if kind in {"table", "view"}:
        return f"{_item(motif)}/Tables/{object_id}"
    return f"{_item(motif)}/{object_id}"


def _expected_identities(motifs: int) -> set[str]:
    return {_identity(motif, role) for motif in range(motifs) for role in _ROLE_KIND}


def _folder(motif: int, stream: str) -> str:
    return f"{_item(motif)}/Files/Scale.M{motif:03d}{stream}Feed"


@weaver_test()
@pytest.mark.parametrize("declarations", [50, 250, 1_000])
def test_folder_feeds_are_separate_authored_resources_consumed_by_python_tables(
    tmp_path, declarations
):
    motifs = declarations // 25
    plan = make_representative_lakehouse_plan(
        RepresentativeLakehouseSpec.from_declarations(declarations)
    )
    expected = {
        _folder(motif, stream)
        for motif in range(motifs)
        for stream in ("Entity", "Adjustment")
    }
    assert {folder.identity for folder in plan.folders} == expected
    assert not expected.intersection(plan.identities)
    assert len(plan.declarations) == declarations

    write_representative_lakehouse_estate(tmp_path, plan)
    repository = parse_representative_lakehouse_estate(tmp_path)
    assert expected <= {str(identity) for identity in repository.source_documents}
    actual_edges = {
        (edge.upstream, edge.downstream) for edge in repository.dependency_graph.edges
    }
    assert {
        (_folder(motif, stream), _identity(motif, f"Source{stream}"))
        for motif in range(motifs)
        for stream in ("Entity", "Adjustment")
    } <= actual_edges
    for motif in range(motifs):
        for stream in ("Entity", "Adjustment"):
            item = tmp_path / _item(motif)
            name = f"Scale__M{motif:03d}{stream}Feed"
            folder_source = (item / "Files" / f"{name}.py").read_text()
            table_source = (
                item / "Tables" / f"Scale__M{motif:03d}Source{stream}.py"
            ).read_text()
            assert "staging_folder()" in folder_source
            assert f"from Files.{name} import {name}" in table_source
            assert ".spark_path()" in table_source
            with (item / "lib" / "data" / f"M{motif:03d}{stream}.csv").open(
                newline="", encoding="utf-8"
            ) as source:
                rows = list(csv.DictReader(source))
            assert len(rows) == 4
            assert {row["MotifKey"] for row in rows} == {str(motif)}
            assert {row["EntityKey"] for row in rows} == {
                str(motif * 100 + offset + 1) for offset in range(4)
            }


def _expected_edges(motifs: int) -> set[tuple[str, str]]:
    edges = {
        (_identity(motif, dependency), _identity(motif, role))
        for motif in range(motifs)
        for role, dependencies in _DEPENDENCIES.items()
        for dependency in dependencies
    }
    shortcut_destination = "Lakehouse/Representative001/Tables/Scale.UpstreamAggregate"
    edges.add((_identity(0, "Aggregate"), shortcut_destination))
    for motif in range(motifs):
        for stream in ("Entity", "Adjustment"):
            edges.add((_folder(motif, stream), _identity(motif, f"Source{stream}")))
    for motif in range(1, motifs):
        if motif % 5 == 0:
            continue
        bridge = (
            shortcut_destination if motif == 1 else _identity(motif - 1, "Aggregate")
        )
        edges.add((bridge, _identity(motif, "Aggregate")))
        edges.add((bridge, _identity(motif, "AggregateReconciles")))
    return edges


def _graph_metrics(nodes: set[str], edges: set[tuple[str, str]]) -> dict[str, int]:
    incoming: dict[str, set[str]] = defaultdict(set)
    outgoing: dict[str, set[str]] = defaultdict(set)
    undirected: dict[str, set[str]] = defaultdict(set)
    for upstream, downstream in edges:
        incoming[downstream].add(upstream)
        outgoing[upstream].add(downstream)
        undirected[upstream].add(downstream)
        undirected[downstream].add(upstream)

    depths: dict[str, int] = {}
    pending = set(nodes)
    while pending:
        ready = {
            node
            for node in pending
            if not incoming[node] or incoming[node] <= depths.keys()
        }
        assert ready, "independent expected graph contains a cycle"
        for node in ready:
            depths[node] = max(
                (depths[parent] + 1 for parent in incoming[node]), default=0
            )
        pending -= ready

    components = 0
    unseen = set(nodes)
    while unseen:
        components += 1
        stack = [next(iter(unseen))]
        while stack:
            node = stack.pop()
            if node not in unseen:
                continue
            unseen.remove(node)
            stack.extend(undirected[node] & unseen)

    roots = nodes - {downstream for _upstream, downstream in edges}
    leaves = nodes - {upstream for upstream, _downstream in edges}
    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "roots": len(roots),
        "leaves": len(leaves),
        "maximum_depth": max(depths.values()),
        "maximum_fan_in": max(map(len, incoming.values())),
        "maximum_fan_out": max(map(len, outgoing.values())),
        "connected_components": components,
    }


@weaver_test()
@pytest.mark.parametrize("declarations", [50, 250, 1_000])
def test_named_scales_have_exact_shape_and_fixed_two_lakehouse_topology(declarations):
    plan = make_representative_lakehouse_plan(
        RepresentativeLakehouseSpec.from_declarations(declarations)
    )

    motifs = declarations // 25
    assert len(plan.declarations) == declarations
    assert Counter(node.kind for node in plan.declarations) == Counter(
        {
            "table": motifs * 14,
            "view": motifs * 6,
            "test": motifs * 3,
            "assumption": motifs * 2,
        }
    )
    assert Counter(node.language for node in plan.declarations) == Counter(
        {"python": motifs * 12, "sql": motifs * 13}
    )
    assert Counter((node.kind, node.language) for node in plan.declarations) == Counter(
        {
            ("table", "python"): motifs * 9,
            ("table", "sql"): motifs * 5,
            ("view", "sql"): motifs * 6,
            ("test", "python"): motifs * 2,
            ("test", "sql"): motifs,
            ("assumption", "python"): motifs,
            ("assumption", "sql"): motifs,
        }
    )
    assert Counter(node.item for node in plan.declarations) == Counter(
        {
            "Lakehouse/Representative000": 25,
            "Lakehouse/Representative001": declarations - 25,
        }
    )
    assert {node.identity for node in plan.declarations} == _expected_identities(motifs)


@weaver_test()
def test_shortcut_census_is_derived_from_every_graph_consumer():
    plan = make_representative_lakehouse_plan(
        RepresentativeLakehouseSpec.from_declarations(50)
    )

    assert len(plan.shortcuts) == 1
    shortcut = plan.shortcuts[0]
    assert shortcut.source == _identity(0, "Aggregate")
    assert shortcut.destination == (
        "Lakehouse/Representative001/Tables/Scale.UpstreamAggregate"
    )
    assert set(shortcut.consumers) == {
        _identity(1, "Aggregate"),
        _identity(1, "AggregateReconciles"),
    }
    assert set(shortcut.consumers) == {
        downstream
        for upstream, downstream in plan.graph_edges
        if upstream == shortcut.destination
    }


@weaver_test()
def test_generated_validation_sources_encode_independent_compatible_relations(tmp_path):
    plan = make_representative_lakehouse_plan(
        RepresentativeLakehouseSpec.from_declarations(50)
    )
    write_representative_lakehouse_estate(tmp_path, plan)

    for path in tmp_path.rglob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    aggregate = (
        tmp_path / "Lakehouse/Representative001/tests/Scale__M001AggregateReconciles.py"
    ).read_text(encoding="utf-8")
    joined = (
        tmp_path / "Lakehouse/Representative001/tests/Scale__M001JoinedMeasuresMatch.py"
    ).read_text(encoding="utf-8")
    no_orphans = (
        tmp_path / "Lakehouse/Representative001/assumptions/Scale__M001NoOrphans.py"
    ).read_text(encoding="utf-8")
    terminal = (
        tmp_path
        / "Lakehouse/Representative001/tests/Scale.M001TerminalSummaryMatches.sql"
    ).read_text(encoding="utf-8")
    windowed = (
        tmp_path / "Lakehouse/Representative001/Tables/Scale.M001WindowedEntities.sql"
    ).read_text(encoding="utf-8")
    enriched = (
        tmp_path / "Lakehouse/Representative001/Tables/Scale__M001EnrichedEntities.py"
    ).read_text(encoding="utf-8")
    summary_positive = (
        tmp_path / "Lakehouse/Representative001/Tables/Scale.M001SummaryPositive.sql"
    ).read_text(encoding="utf-8")
    ranked = (
        tmp_path / "Lakehouse/Representative001/Tables/Scale__M001RankedEntities.py"
    ).read_text(encoding="utf-8")

    assert "as TotalAmount" in aggregate
    assert "as EntityCount" in aggregate
    assert "Scale.UpstreamAggregate" in aggregate
    assert "unionByName" in aggregate
    assert "as EffectiveAmount" in joined
    assert '"e.MotifKey as MotifKey"' in joined
    assert 'where("ParentKey is not null")' in no_orphans
    assert "from Scale.M001SummaryUnion" in terminal
    assert "join Scale.M001SummaryJoin" in terminal
    assert "left join Scale.M001RankedEntities" in terminal
    assert "from Scale.M001PublishedBridge" in terminal
    assert "from Scale.M001TerminalSummary" not in terminal
    assert "cast(sum(EnrichedAmount) over (" in windowed
    assert "as decimal(18, 2)) as RunningAmount" in windowed
    assert "from weaver import Table" in enriched
    assert "projection.join(" in enriched
    assert "as EnrichedAmount" in enriched
    assert "select MotifKey, GroupKey, TotalAmount, EntityCount" in summary_positive
    assert "select *" not in summary_positive
    assert '.where("RankNumber <= 3").select(' in ranked
    assert '"MotifKey", "EntityKey", "GroupKey"' in ranked
    assert '"EnrichedAmount", "RunningAmount", "RankNumber"' in ranked


@weaver_test()
def test_two_motif_cold_bundle_contains_the_independent_runtime_census(tmp_path):
    source = tmp_path / "source"
    plan = make_representative_lakehouse_plan(
        RepresentativeLakehouseSpec.from_declarations(50)
    )
    write_representative_lakehouse_estate(source, plan)
    repository = parse_representative_lakehouse_estate(source)
    bindings = effective_item_bindings(
        item_bindings(
            ("Lakehouse/Representative000", "Representative000_LH"),
            ("Lakehouse/Representative001", "Representative001_LH"),
        ),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    inventories = {}
    for binding in bindings.entries:
        target = binding.to_bound_target()
        inventories[binding.item] = target_inventory(
            target_id=target.id,
            kind=target.kind,
            target_name=target.name,
        )

    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=FixtureCatalogue.from_registry_rows(),
        catalogue_binding=WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE),
    )
    actions = [action for _sequence, _batch, action in bundle.plan.actions()]
    runtime_resources = {
        action.resource_node_id
        for action in actions
        if action.kind == "write_file" and action.resource_node_id is not None
    }
    expected_runtime_resources = {
        (
            f"{_item(motif)}/file:_/Load/"
            f"{'Tables' if role in _TABLE_ROLES else 'tests' if role in _TEST_ROLES else 'assumptions'}/"
            f"Scale__M{motif:03d}{role}.py"
        )
        for motif in range(2)
        for role in (*_TABLE_ROLES, *_TEST_ROLES, *_ASSUMPTION_ROLES)
    }

    expected_runtime_resources.add(
        "Lakehouse/Representative001/file:_/Load/shortcuts.py"
    )
    expected_runtime_resources.update(
        f"{_item(motif)}/file:_/Load/lib/data/M{motif:03d}{stream}.csv"
        for motif in range(2)
        for stream in ("Entity", "Adjustment")
    )
    expected_runtime_resources.update(
        f"{_item(motif)}/file:_/Load/Files/Scale__M{motif:03d}{stream}Feed.py"
        for motif in range(2)
        for stream in ("Entity", "Adjustment")
    )

    assert bundle.plan.omitted_nodes == ()
    assert runtime_resources == expected_runtime_resources
    assert len(runtime_resources) == 47
    assert (
        sum(action.id == "shortcuts-Lakehouse--Representative001" for action in actions)
        == 1
    )


@weaver_test()
@pytest.mark.parametrize("declarations", [50, 250, 1_000])
def test_generated_repository_matches_an_independent_oracle(tmp_path, declarations):
    motifs = declarations // 25
    evidence = qualify_representative_lakehouse_estate(
        tmp_path, RepresentativeLakehouseSpec.from_declarations(declarations)
    )
    expected_nodes = (
        _expected_identities(motifs)
        | {"Lakehouse/Representative001/Tables/Scale.UpstreamAggregate"}
        | {
            _folder(motif, stream)
            for motif in range(motifs)
            for stream in ("Entity", "Adjustment")
        }
    )
    expected_edges = _expected_edges(motifs)

    assert evidence["declarations"] == {
        "total": declarations,
        "by_kind": {
            "assumption": motifs * 2,
            "table": motifs * 14,
            "test": motifs * 3,
            "view": motifs * 6,
        },
        "by_language": {"python": motifs * 12, "sql": motifs * 13},
        "by_kind_and_language": {
            "assumption": {"python": motifs, "sql": motifs},
            "table": {"python": motifs * 9, "sql": motifs * 5},
            "test": {"python": motifs * 2, "sql": motifs},
            "view": {"sql": motifs * 6},
        },
    }
    assert evidence["graph"] == _graph_metrics(expected_nodes, expected_edges)
    assert evidence["folders"] == {
        "count": motifs * 2,
        "by_item": {
            "Lakehouse/Representative000": 2,
            "Lakehouse/Representative001": motifs * 2 - 2,
        },
        "consumed": motifs * 2,
    }
    assert evidence["oracle_matches_repository"] == {
        "declaration_identities": True,
        "declaration_kinds": True,
        "declaration_languages": True,
        "ordinary_edges": True,
        "shortcut_edges": True,
        "graph_metrics": True,
        "item_engine_distribution": True,
        "validation_counts": True,
        "shortcut_census": True,
        "folder_census": True,
    }
