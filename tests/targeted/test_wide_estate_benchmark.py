"""A wide estate proves impact selection without using its own planner as oracle."""

from __future__ import annotations

from support.weaver_test import weaver_test

from tools.benchmark_wide_estate import (
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
