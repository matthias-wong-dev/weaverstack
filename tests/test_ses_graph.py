"""Graph primitives — edge-agnostic, so build and load can share them."""

from __future__ import annotations

import pytest

from weaver.errors import GraphError
from weaver.ses import Graph


def chain() -> Graph:
    return Graph("ABC", [("A", "B"), ("B", "C")])


def diamond() -> Graph:
    return Graph("ABCD", [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")])


# --- construction ------------------------------------------------------------


def test_nodes_and_edges_are_sorted_and_deduplicated():
    graph = Graph(["B", "A", "A"], [("A", "B"), ("A", "B")])
    assert graph.nodes == ("A", "B")
    assert len(graph.edges) == 1


def test_an_edge_to_an_unknown_node_is_refused():
    with pytest.raises(GraphError, match="unknown node 'C'"):
        Graph("AB", [("A", "C")])


def test_a_self_edge_is_refused():
    with pytest.raises(GraphError, match="depends on itself"):
        Graph("A", [("A", "A")])


def test_an_isolated_node_is_fine():
    assert Graph("ABC").order() == ("A", "B", "C")


# --- ordering ----------------------------------------------------------------


def test_upstream_comes_before_downstream():
    assert chain().order() == ("A", "B", "C")


def test_ties_are_broken_by_name_so_plans_are_reproducible():
    graph = Graph("ZYX", [])
    assert graph.order() == ("X", "Y", "Z")
    assert graph.order() == Graph("XYZ", []).order()


def test_a_diamond_orders_both_middles_before_the_join():
    order = diamond().order()
    assert order.index("A") < order.index("B") < order.index("D")
    assert order.index("A") < order.index("C") < order.index("D")


# --- layers ------------------------------------------------------------------


def test_layers_group_what_can_run_together():
    assert diamond().layers() == (("A",), ("B", "C"), ("D",))


def test_a_chain_is_one_node_per_layer():
    assert chain().layers() == (("A",), ("B",), ("C",))


def test_independent_nodes_share_the_first_layer():
    assert Graph("ABC").layers() == (("A", "B", "C"),)


def test_a_node_sits_below_its_deepest_ancestor():
    """Long path wins, so nothing runs before everything it needs."""
    graph = Graph("ABCD", [("A", "B"), ("B", "C"), ("A", "D"), ("C", "D")])
    assert graph.layers() == (("A",), ("B",), ("C",), ("D",))


def test_every_node_appears_in_exactly_one_layer():
    graph = diamond()
    flattened = [node for layer in graph.layers() for node in layer]
    assert sorted(flattened) == list(graph.nodes)


# --- cycles ------------------------------------------------------------------


def test_a_cycle_is_refused_on_construction():
    with pytest.raises(GraphError, match="dependency cycle"):
        Graph("AB", [("A", "B"), ("B", "A")])


def test_the_cycle_message_names_the_objects():
    with pytest.raises(GraphError) as info:
        Graph("ABC", [("A", "B"), ("B", "C"), ("C", "A")])
    message = str(info.value)
    for node in "ABC":
        assert node in message
    assert "->" in message


def test_a_cycle_is_found_among_unrelated_healthy_nodes():
    with pytest.raises(GraphError, match="dependency cycle"):
        Graph("ABCDE", [("A", "B"), ("C", "D"), ("D", "E"), ("E", "C")])


# --- traversal ---------------------------------------------------------------


def test_descendants_reach_transitively_in_order():
    assert chain().descendants("A") == ("B", "C")


def test_ancestors_reach_transitively_in_order():
    assert chain().ancestors("C") == ("A", "B")


def test_a_leaf_has_no_descendants():
    assert chain().descendants("C") == ()


def test_descendants_of_a_diamond_include_the_join_once():
    assert diamond().descendants("A") == ("B", "C", "D")


def test_traversing_an_unknown_node_is_an_error():
    with pytest.raises(GraphError, match="unknown node"):
        chain().descendants("Z")


def test_roots_and_leaves():
    graph = diamond()
    assert graph.roots() == ("A",)
    assert graph.leaves() == ("D",)


def test_direct_neighbours_are_not_transitive():
    graph = chain()
    assert graph.downstream_of("A") == ("B",)
    assert graph.upstream_of("C") == ("B",)


# --- subgraphs ---------------------------------------------------------------


def test_a_subgraph_keeps_only_internal_edges():
    sub = chain().subgraph(["B", "C"])
    assert sub.nodes == ("B", "C")
    assert [str(edge) for edge in sub.edges] == ["B -> C"]


def test_a_subgraph_can_pull_in_what_it_needs():
    sub = chain().subgraph(["C"], with_ancestors=True)
    assert sub.nodes == ("A", "B", "C")
    assert sub.order() == ("A", "B", "C")


def test_a_subgraph_can_pull_in_what_depends_on_it():
    """The shape a rebuild needs: this object and everything it invalidates."""
    sub = chain().subgraph(["A"], with_descendants=True)
    assert sub.nodes == ("A", "B", "C")


def test_a_subgraph_of_one_isolated_node():
    assert diamond().subgraph(["B"]).edges == ()
