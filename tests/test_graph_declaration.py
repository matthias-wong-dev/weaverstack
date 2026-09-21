"""Graph primitives: edge-agnostic, so build and load can share them."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import GraphError
from weaver.graph import Graph


def chain() -> Graph:
    return Graph("ABC", [("A", "B"), ("B", "C")])


def diamond() -> Graph:
    return Graph("ABCD", [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")])


# --- construction ------------------------------------------------------------


@weaver_test()
def test_nodes_and_edges_are_sorted_and_deduplicated():
    graph = Graph(["B", "A", "A"], [("A", "B"), ("A", "B")])
    assert graph.nodes == ("A", "B")
    assert len(graph.edges) == 1


@weaver_test()
def test_an_edge_to_an_unknown_node_is_refused():
    with pytest.raises(GraphError, match="unknown node 'C'"):
        Graph("AB", [("A", "C")])


@weaver_test()
def test_a_self_edge_is_refused():
    with pytest.raises(GraphError, match="depends on itself"):
        Graph("A", [("A", "A")])


@weaver_test()
def test_an_isolated_node_is_fine():
    assert Graph("ABC").order() == ("A", "B", "C")


# --- ordering ----------------------------------------------------------------


@weaver_test()
def test_upstream_comes_before_downstream():
    assert chain().order() == ("A", "B", "C")


@weaver_test()
def test_ties_are_broken_by_name_so_plans_are_reproducible():
    graph = Graph("ZYX", [])
    assert graph.order() == ("X", "Y", "Z")
    assert graph.order() == Graph("XYZ", []).order()


@weaver_test()
def test_a_caller_may_break_ties_on_an_order_of_its_own():
    """A run orders ready nodes by target, not by node id."""

    graph = Graph("ABCD", [("A", "D")])
    priority = {"A": 3, "B": 1, "C": 2, "D": 0}

    assert graph.order(key=priority.get) == ("B", "C", "A", "D")


@weaver_test()
def test_a_caller_order_never_puts_a_node_before_its_upstream():
    graph = Graph("ABC", [("A", "B"), ("B", "C")])

    assert graph.order(key=lambda node: -ord(node)) == ("A", "B", "C")


@weaver_test()
def test_a_diamond_orders_both_middles_before_the_join():
    order = diamond().order()
    assert order.index("A") < order.index("B") < order.index("D")
    assert order.index("A") < order.index("C") < order.index("D")


# --- layers ------------------------------------------------------------------


@weaver_test()
def test_layers_group_what_can_run_together():
    assert diamond().layers() == (("A",), ("B", "C"), ("D",))


@weaver_test()
def test_a_chain_is_one_node_per_layer():
    assert chain().layers() == (("A",), ("B",), ("C",))


@weaver_test()
def test_independent_nodes_share_the_first_layer():
    assert Graph("ABC").layers() == (("A", "B", "C"),)


@weaver_test()
def test_a_node_sits_below_its_deepest_ancestor():
    """Long path wins, so nothing runs before everything it needs."""
    graph = Graph("ABCD", [("A", "B"), ("B", "C"), ("A", "D"), ("C", "D")])
    assert graph.layers() == (("A",), ("B",), ("C",), ("D",))


@weaver_test()
def test_every_node_appears_in_exactly_one_layer():
    graph = diamond()
    flattened = [node for layer in graph.layers() for node in layer]
    assert sorted(flattened) == list(graph.nodes)


# --- cycles ------------------------------------------------------------------


@weaver_test()
def test_a_cycle_is_refused_on_construction():
    with pytest.raises(GraphError, match="dependency cycle"):
        Graph("AB", [("A", "B"), ("B", "A")])


@weaver_test()
def test_the_cycle_message_names_the_objects():
    with pytest.raises(GraphError) as info:
        Graph("ABC", [("A", "B"), ("B", "C"), ("C", "A")])
    message = str(info.value)
    for node in "ABC":
        assert node in message
    assert "->" in message


@weaver_test()
def test_a_cycle_is_found_among_unrelated_healthy_nodes():
    with pytest.raises(GraphError, match="dependency cycle"):
        Graph("ABCDE", [("A", "B"), ("C", "D"), ("D", "E"), ("E", "C")])


@weaver_test()
def test_a_node_that_depends_on_itself_is_refused_before_ordering():
    """A self-edge is caught on construction, so the cycle walk never sees it."""

    with pytest.raises(GraphError, match="A depends on itself"):
        Graph("A", [("A", "A")])


#: Longer than the interpreter's own limit, so a walk that used the call stack
#: would raise RecursionError instead of naming what is wrong.
DEEP = 4000


def _names(count: int) -> list[str]:
    # Zero-padded so sorted order is numeric order, which is what makes the
    # reported cycle predictable.
    return [f"n{index:05d}" for index in range(count)]


@weaver_test()
def test_a_chain_deeper_than_the_recursion_limit_still_orders():
    """Valid and very deep. Ordering is iterative, and so is the check for a
    cycle it is about to conclude there is none of."""

    names = _names(DEEP)
    edges = list(zip(names, names[1:]))

    assert Graph(names, edges).order() == tuple(names)


@weaver_test()
def test_a_cycle_deeper_than_the_recursion_limit_is_reported_as_a_cycle():
    names = _names(DEEP)
    edges = list(zip(names, names[1:])) + [(names[-1], names[0])]

    with pytest.raises(GraphError) as info:
        Graph(names, edges)

    reported = str(info.value).removeprefix("dependency cycle: ").split(" -> ")
    assert reported[0] == names[0]
    assert reported[-1] == names[0]
    assert len(reported) == DEEP + 1


@weaver_test()
def test_an_acyclic_tail_attached_to_a_deep_cycle_reports_the_cycle():
    """The tail is residual too, so a walk that gave up would list it instead."""

    names = _names(DEEP)
    edges = list(zip(names, names[1:])) + [
        (names[-1], names[0]),
        (names[0], "tail"),
    ]

    with pytest.raises(GraphError) as info:
        Graph(names + ["tail"], edges)

    reported = str(info.value).removeprefix("dependency cycle: ").split(" -> ")
    assert "tail" not in reported
    assert reported[0] == reported[-1] == names[0]


@weaver_test()
def test_a_disconnected_cyclic_component_is_found_from_a_healthy_start():
    """The first candidate in order leads nowhere, and the cycle is elsewhere."""

    edges = [("A", "B"), ("Y", "Z"), ("Z", "Y")]

    with pytest.raises(GraphError) as info:
        Graph("ABYZ", edges)

    assert "dependency cycle: Y -> Z -> Y" in str(info.value)


@weaver_test()
def test_the_same_graph_always_reports_the_same_cycle():
    edges = [("C", "D"), ("D", "E"), ("E", "C"), ("A", "C")]
    reported = set()

    for _attempt in range(5):
        with pytest.raises(GraphError) as info:
            Graph("ABCDE", edges)
        reported.add(str(info.value))

    assert len(reported) == 1


# --- traversal ---------------------------------------------------------------


@weaver_test()
def test_descendants_reach_transitively_in_order():
    assert chain().descendants("A") == ("B", "C")


@weaver_test()
def test_ancestors_reach_transitively_in_order():
    assert chain().ancestors("C") == ("A", "B")


@weaver_test()
def test_a_leaf_has_no_descendants():
    assert chain().descendants("C") == ()


@weaver_test()
def test_descendants_of_a_diamond_include_the_join_once():
    assert diamond().descendants("A") == ("B", "C", "D")


@weaver_test()
def test_traversing_an_unknown_node_is_an_error():
    with pytest.raises(GraphError, match="unknown node"):
        chain().descendants("Z")


@weaver_test()
def test_roots_and_leaves():
    graph = diamond()
    assert graph.roots() == ("A",)
    assert graph.leaves() == ("D",)


@weaver_test()
def test_direct_neighbours_are_not_transitive():
    graph = chain()
    assert graph.downstream_of("A") == ("B",)
    assert graph.upstream_of("C") == ("B",)


# --- subgraphs ---------------------------------------------------------------


@weaver_test()
def test_a_subgraph_keeps_only_internal_edges():
    sub = chain().subgraph(["B", "C"])
    assert sub.nodes == ("B", "C")
    assert [str(edge) for edge in sub.edges] == ["B -> C"]


@weaver_test()
def test_a_subgraph_can_pull_in_what_it_needs():
    sub = chain().subgraph(["C"], with_ancestors=True)
    assert sub.nodes == ("A", "B", "C")
    assert sub.order() == ("A", "B", "C")


@weaver_test()
def test_a_subgraph_can_pull_in_what_depends_on_it():
    """The shape a rebuild needs: this object and everything it invalidates."""
    sub = chain().subgraph(["A"], with_descendants=True)
    assert sub.nodes == ("A", "B", "C")


@weaver_test()
def test_a_subgraph_of_one_isolated_node():
    assert diamond().subgraph(["B"]).edges == ()
