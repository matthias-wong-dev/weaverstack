"""A wide estate proves impact selection without using its own planner as oracle."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test
from support.wide_estate_benchmark import (
    WideEstateSpec,
    benchmark_estate,
    descendants,
    make_topology,
    parse_generated_estate,
    write_estate,
)


def _identity(branch: int, role: str) -> str:
    return f"Lakehouse/Benchmark/Tables/Scale.B{branch:03d}{role}"


@weaver_test()
def test_ten_branch_topology_has_250_objects_and_required_graph_shapes():
    topology = make_topology(WideEstateSpec(branches=10))

    assert len(topology.nodes) == 250
    assert len(topology.edges) == 340
    assert topology.statistics() == {
        "objects": 250,
        "edges": 340,
        "maximum_depth": 7,
        "independent_branches": 10,
        "maximum_fan_in": 5,
        "maximum_fan_out": 11,
    }

    assert len(descendants(topology, _identity(0, "Root"))) == 24
    assert descendants(topology, _identity(0, "Leaf00")) == ()
    assert set(descendants(topology, _identity(0, "Chain02"))) == {
        _identity(0, "Chain03"),
        _identity(0, "Chain04"),
        _identity(0, "Bridge"),
        *(_identity(0, f"Leaf{index:02d}") for index in range(4)),
    }


@weaver_test()
def test_seed_selects_one_repeatable_topology_variant():
    first = make_topology(WideEstateSpec(branches=2, seed=1))
    repeated = make_topology(WideEstateSpec(branches=2, seed=1))
    another = make_topology(WideEstateSpec(branches=2, seed=2))

    assert first == repeated
    assert first.edges != another.edges


@weaver_test()
def test_a_benchmark_needs_two_independent_branches(tmp_path):
    with pytest.raises(ValueError, match="at least 50 objects"):
        benchmark_estate(tmp_path, WideEstateSpec.from_objects(25))


@weaver_test()
def test_generated_repository_matches_the_independent_topology(tmp_path):
    topology = make_topology(WideEstateSpec(branches=1))
    write_estate(tmp_path, topology)

    repository = parse_generated_estate(tmp_path)
    graph = repository.dependency_graph

    assert graph is not None
    assert set(topology.identities) <= set(graph.nodes)
    assert {
        (edge.upstream, edge.downstream)
        for edge in graph.edges
        if edge.upstream in topology.identities
    } == set(topology.edges)


@weaver_test()
def test_every_local_benchmark_scenario_matches_the_oracle(tmp_path):
    result = benchmark_estate(tmp_path, WideEstateSpec(branches=2))

    assert result["topology"]["objects"] == 50
    assert result["topology"]["independent_branches"] == 2
    assert all(scenario["matches_oracle"] for scenario in result["scenarios"])
    assert all(
        scenario["unrelated_branches_untouched"] for scenario in result["scenarios"]
    )
    assert all("actual_affected_set" in scenario for scenario in result["scenarios"])
    mid_graph = next(
        scenario
        for scenario in result["scenarios"]
        if scenario["name"] == "mid_graph_change"
    )
    expected_mid_graph = {
        _identity(0, "Chain03"),
        _identity(0, "Chain04"),
        _identity(0, "Bridge"),
        *(_identity(0, f"Leaf{index:02d}") for index in range(4)),
    }
    assert set(mid_graph["expected_descendant_set"]) == expected_mid_graph
    assert set(mid_graph["actual_descendant_set"]) == expected_mid_graph
    assert set(result["timings"]) == {
        "topology_seconds",
        "source_write_seconds",
        "parse_seconds",
        "signature_seconds",
        "total_seconds",
    }
    assert all(value >= 0 for value in result["timings"].values())
    assert all(scenario["seconds"] >= 0 for scenario in result["scenarios"])
    assert {
        scenario["name"]: scenario["selected_object_count"]
        for scenario in result["scenarios"]
    } == {
        "full_cold_build": 50,
        "unchanged_build": 0,
        "leaf_change": 1,
        "mid_graph_change": 8,
        "high_fan_out_change": 25,
        "independent_branch_changes": 26,
    }


@pytest.mark.parametrize("objects", [250, 1000])
@weaver_test()
def test_required_scale_estates_compose(tmp_path, objects):
    result = benchmark_estate(tmp_path, WideEstateSpec.from_objects(objects))

    assert result["topology"]["objects"] == objects
    assert result["topology_oracle_matches_repository"] is True
    assert result["all_scenarios_match"] is True


@weaver_test()
def test_a_warehouse_estate_uses_tsql_documents_and_warehouse_identities(tmp_path):
    spec = WideEstateSpec.from_objects(50, engine="warehouse")
    topology = make_topology(spec)

    write_estate(tmp_path, topology)
    repository = parse_generated_estate(tmp_path)

    assert topology.identities[0] == "Warehouse/Benchmark/Scale.B000Root"
    graph = repository.dependency_graph
    assert graph is not None
    assert set(topology.identities) <= set(graph.nodes)
    assert (tmp_path / "Warehouse/Benchmark/Scale.B000Root.sql").is_file()
    assert not (tmp_path / "Warehouse/Benchmark/Tables").exists()
    assert not list((tmp_path / "Warehouse/Benchmark").glob("*.py"))


@pytest.mark.parametrize("objects", [250, 1000])
@weaver_test()
def test_required_warehouse_scale_estates_compose(tmp_path, objects):
    result = benchmark_estate(
        tmp_path, WideEstateSpec.from_objects(objects, engine="warehouse")
    )

    assert result["generator"]["engine"] == "warehouse"
    assert result["topology"]["objects"] == objects
    assert result["topology_oracle_matches_repository"] is True
    assert result["all_scenarios_match"] is True


@weaver_test()
def test_an_unknown_benchmark_engine_is_refused():
    with pytest.raises(ValueError, match="engine"):
        WideEstateSpec(branches=2, engine="kusto")


@weaver_test()
def test_a_wrong_impact_is_rejected_before_benchmark_evidence(monkeypatch, tmp_path):
    from weaver.build_bundle.incremental import Impact

    monkeypatch.setattr(
        "weaver.build_bundle.incremental.determine_impact",
        lambda *args, **kwargs: Impact(new=(), changed=(), impacted_descendants=()),
    )

    with pytest.raises(AssertionError, match="before timing"):
        benchmark_estate(tmp_path, WideEstateSpec(branches=2))
