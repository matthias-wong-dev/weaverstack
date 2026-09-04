#!/usr/bin/env python3
"""Lay out a real installed Weaver catalogue DAG for the website's scale page.

Input is the export `Catalogue.dag()` produces, the same installed graph load
planning, validation planning and health read. The layout runs here rather than
in the browser, so the page ships a static SVG that renders with no JavaScript
and the viewer script only adds lineage highlighting on top of it.

    python3 tools/build_scale_dag.py \
        ~/dev/dwg-platform/ilovegov-etl/gui/catalogue-dag.json

It writes `docs/assets/scale-dag.svg`, `docs/assets/scale-dag.json`, and
`docs/scale/index.html`, which is the page's two prose fragments in
`tools/scale-page/` joined around the inlined graph. The JSON carries the
adjacency and node detail the viewer shows when a node is selected.

Nodes are grouped by the Fabric item that owns them and laid into topological
layers inside it, following the same rules as the source viewer: shortcuts take
a dedicated first layer, validations a dedicated last one, and Weaver's own
catalogue item and `_` schema are left out because they are Weaver's internals
rather than the estate the page is about.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_SVG = ROOT / "docs" / "assets" / "scale-dag.svg"
OUT_JSON = ROOT / "docs" / "assets" / "scale-dag.json"
OUT_PAGE = ROOT / "docs" / "scale" / "index.html"

#: The scale page either side of the graph, joined around it on every run.
HEAD = ROOT / "tools" / "scale-page" / "head.html"
TAIL = ROOT / "tools" / "scale-page" / "tail.html"

#: Weaver's own catalogue item. Its `_` tables are Weaver's internals and say
#: nothing about the size of the estate, so they are not drawn.
CATALOGUE_ITEMS = {"Warehouse/Catalogue", "Warehouse/_weaver"}
UNDERSCORE_SCHEMA = re.compile(r"^(?:Files/|Tables/)?_$")

# --- geometry ------------------------------------------------------------------

NODE_W = 132
NODE_H = 28
COL_GAP = 36
SUB_GAP = 9
ROW_GAP = 6
ITEM_PAD = 18
ITEM_HEAD = 34
ITEM_GAP = 26
MARGIN = 14

#: The tallest a single column of nodes is drawn. A layer with more than this
#: wraps into adjacent sub-columns, drawn close together so they still read as
#: one layer. Without it a 46-node layer sets the height of the whole picture.
ROWS_CAP = 14


def kind_of(node: dict) -> str:
    """The visual class one node draws with."""

    if node["role"] in ("test", "assumption"):
        return "validation"
    if node["role"] == "shortcut":
        return "shortcut"
    if node["item_type"] == "Lakehouse" and node["registry_type"] == "folder":
        return "file"
    if node["registry_type"] == "view":
        return "view"
    return "table"


def label_of(node: dict) -> str:
    """What the box says: the object name, which is what a reader scans for."""

    return node["object_name"]


def domain_of(node: dict) -> str:
    """The business schema, with the Lakehouse Files/ or Tables/ prefix removed."""

    return re.sub(r"^(?:Files|Tables)/", "", node["schema_name"]).split("/")[0] or "_"


def load(path: Path) -> tuple[list[dict], list[dict]]:
    """The export, with Weaver's own internals filtered out of both halves."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    nodes = [
        node
        for node in payload["nodes"]
        if node["target"] not in CATALOGUE_ITEMS
        and not UNDERSCORE_SCHEMA.match(node["schema_name"])
    ]
    kept = {node["logical_id"] for node in nodes}
    edges = []
    for edge in payload["edges"]:
        # A shortcut edge names the node it passes through. Where that node
        # survived, the edge is redrawn to it so the shortcut appears in the
        # path rather than being bypassed.
        through = edge.get("through")
        upstream = through if through in kept else edge["upstream"]
        if upstream in kept and edge["downstream"] in kept:
            edges.append({**edge, "upstream": upstream})
    return nodes, edges


def layer(nodes: list[dict], edges: list[dict]) -> int:
    """Assign each node in one item a topological layer, and return the last one.

    Shortcuts occupy layer 0 because they are how data arrives in the item, and
    validations the final layer because they read what everything else built.
    Everything between is ranked by its longest path to a sink, so a node sits
    immediately left of the thing furthest downstream of it.
    """

    ids = {node["logical_id"] for node in nodes}
    shortcuts = [n for n in nodes if n["kind"] == "shortcut"]
    validations = [n for n in nodes if n["kind"] == "validation"]
    ranked = [n for n in nodes if n["kind"] not in ("shortcut", "validation")]
    ranked_ids = {n["logical_id"] for n in ranked}

    outgoing: dict[str, list[str]] = defaultdict(list)
    indegree = {node_id: 0 for node_id in ranked_ids}
    for edge in edges:
        if edge["upstream"] in ranked_ids and edge["downstream"] in ranked_ids:
            outgoing[edge["upstream"]].append(edge["downstream"])
            indegree[edge["downstream"]] += 1

    queue = sorted(node_id for node_id, count in indegree.items() if count == 0)
    order: list[str] = []
    while queue:
        current = queue.pop(0)
        order.append(current)
        for following in sorted(outgoing[current]):
            indegree[following] -= 1
            if indegree[following] == 0:
                queue.append(following)

    to_sink = {node_id: 0 for node_id in ranked_ids}
    for node_id in reversed(order):
        for following in outgoing[node_id]:
            to_sink[node_id] = max(to_sink[node_id], to_sink[following] + 1)

    depth = max(to_sink.values(), default=0)
    offset = 1 if shortcuts else 0
    for node in ranked:
        node["layer"] = offset + depth - to_sink[node["logical_id"]]
    for node in shortcuts:
        node["layer"] = 0
    last = depth + offset + (1 if validations else 0)
    for node in validations:
        node["layer"] = last
    assert ids  # every node in this item was reached
    return last


def place(nodes: list[dict], edges: list[dict]) -> tuple[list[dict], float, float]:
    """Position every node, and return the items with the drawing's size."""

    for node in nodes:
        node["kind"] = kind_of(node)
        node["item"] = node["target"]

    by_item: dict[str, list[dict]] = defaultdict(list)
    for node in nodes:
        by_item[node["item"]].append(node)

    # Lakehouses first: data arrives there and flows into the Warehouses.
    order = sorted(
        by_item, key=lambda name: (0 if name.startswith("Lakehouse/") else 1, name)
    )

    items = []
    y = MARGIN
    width = 0.0
    for name in order:
        owned = by_item[name]
        inside = [
            edge
            for edge in edges
            if any(n["logical_id"] == edge["upstream"] for n in owned)
            and any(n["logical_id"] == edge["downstream"] for n in owned)
        ]
        last = layer(owned, inside)

        columns: dict[int, list[dict]] = defaultdict(list)
        for node in owned:
            columns[node["layer"]].append(node)

        # The item is as tall as its tallest sub-column, which is not the same
        # as its largest layer once wrapping has split that layer up.
        rows = max(
            -(-len(column) // max(1, -(-len(column) // ROWS_CAP)))
            for column in columns.values()
        )
        item_h = ITEM_HEAD + ITEM_PAD + rows * (NODE_H + ROW_GAP)

        x = MARGIN + ITEM_PAD
        for index in range(last + 1):
            column = sorted(
                columns.get(index, []), key=lambda n: (domain_of(n), n["object_name"])
            )
            # A layer taller than the cap is split across sub-columns, filled
            # top to bottom so reading order down a sub-column stays the sort
            # order. The last sub-column is the short one.
            width_in = max(1, -(-len(column) // ROWS_CAP))
            tall = -(-len(column) // width_in) if column else 1
            for position, node in enumerate(column):
                sub, row = divmod(position, tall)
                # Centre each sub-column against the item's tallest layer.
                height_in = min(tall, len(column) - sub * tall)
                top = y + ITEM_HEAD + (rows - height_in) * (NODE_H + ROW_GAP) / 2
                node["x"] = x + sub * (NODE_W + SUB_GAP)
                node["y"] = top + row * (NODE_H + ROW_GAP)
            x += width_in * NODE_W + (width_in - 1) * SUB_GAP + COL_GAP

        item_w = x - COL_GAP - MARGIN + ITEM_PAD

        items.append(
            {
                "name": name,
                "x": MARGIN,
                "y": y,
                "w": item_w,
                "h": item_h,
                "count": len(owned),
                "layers": last + 1,
            }
        )
        width = max(width, item_w)
        y += item_h + ITEM_GAP

    return items, width + MARGIN * 2, y - ITEM_GAP + MARGIN


def edge_path(source: dict, target: dict) -> str:
    """One edge, as a cubic that leaves the right of a box and enters the left."""

    x1, y1 = source["x"] + NODE_W, source["y"] + NODE_H / 2
    x2, y2 = target["x"], target["y"] + NODE_H / 2
    if x2 <= x1:
        # A back edge, which layering makes rare. Bow it under both boxes so it
        # does not disappear behind the row it came from.
        drop = max(y1, y2) + NODE_H
        return f"M{x1} {y1} C{x1 + 40} {drop} {x2 - 40} {drop} {x2} {y2}"
    reach = min(60, (x2 - x1) / 2 + 10)
    return f"M{x1} {y1} C{x1 + reach} {y1} {x2 - reach} {y2} {x2} {y2}"


def render(nodes, edges, items, width, height) -> str:
    """The whole graph as one SVG, with every node addressable by index."""

    index = {node["logical_id"]: number for number, node in enumerate(nodes)}
    parts = [
        f'<svg class="scale-dag" viewBox="0 0 {width:.0f} {height:.0f}" '
        f'width="{width:.0f}" height="{height:.0f}" '
        f'xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-labelledby="scale-dag-title scale-dag-desc">',
        '<title id="scale-dag-title">An installed Weaver estate</title>',
        f'<desc id="scale-dag-desc">{len(nodes)} objects and {len(edges)} '
        f"dependencies across {len(items)} Fabric items, laid into topological "
        "layers inside each item.</desc>",
    ]

    for item in items:
        kind = "lakehouse" if item["name"].startswith("Lakehouse/") else "warehouse"
        parts.append(
            f'<rect class="item {kind}" x="{item["x"]}" y="{item["y"]:.0f}" '
            f'width="{item["w"]}" height="{item["h"]:.0f}" rx="10"/>'
        )
        parts.append(
            f'<text class="item-label" x="{item["x"] + ITEM_PAD}" '
            f'y="{item["y"] + 22:.0f}">{escape(item["name"].upper())}'
            f'<tspan class="item-count" dx="10">{item["count"]} objects &#183; '
            f"{item['layers']} layers</tspan></text>"
        )

    parts.append('<g class="edges">')
    for edge in edges:
        source, target = (
            nodes[index[edge["upstream"]]],
            nodes[index[edge["downstream"]]],
        )
        kind = " shortcut" if edge["kind"] == "shortcut" else ""
        parts.append(
            f'<path class="e{kind}" d="{edge_path(source, target)}" '
            f'data-u="{index[edge["upstream"]]}" data-d="{index[edge["downstream"]]}"/>'
        )
    parts.append("</g>")

    parts.append('<g class="nodes">')
    for number, node in enumerate(nodes):
        label = label_of(node)
        # Trim to what the box holds at the drawn font size, so nothing spills.
        shown = escape(label) if len(label) <= 19 else escape(label[:18]) + "&#8230;"
        parts.append(
            f'<g class="n k-{node["kind"]}" data-i="{number}" tabindex="0" '
            f'role="button" aria-label="{escape(label)}">'
            f'<rect x="{node["x"]}" y="{node["y"]:.0f}" width="{NODE_W}" '
            f'height="{NODE_H}" rx="5"/>'
            f'<text x="{node["x"] + 8}" y="{node["y"] + 19:.0f}">{shown}</text>'
            f"</g>"
        )
    parts.append("</g></svg>")
    return "\n".join(parts)


def detail(nodes, edges) -> dict:
    """What the viewer needs: one record per node, and the adjacency both ways."""

    index = {node["logical_id"]: number for number, node in enumerate(nodes)}
    up: list[list[int]] = [[] for _ in nodes]
    down: list[list[int]] = [[] for _ in nodes]
    for edge in edges:
        u, d = index[edge["upstream"]], index[edge["downstream"]]
        down[u].append(d)
        up[d].append(u)
    return {
        "nodes": [
            {
                "id": node["logical_id"],
                "name": node["object_name"],
                "schema": node["schema_name"],
                "item": node["item"],
                "kind": node["kind"],
                "type": node["registry_type"],
                "role": node["role"],
                "layer": node["layer"],
                "loadable": node["is_loadable"],
            }
            for node in nodes
        ],
        "up": up,
        "down": down,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export", type=Path, help="catalogue-dag.json to lay out")
    args = parser.parse_args()

    nodes, edges = load(args.export)
    items, width, height = place(nodes, edges)
    svg = render(nodes, edges, items, width, height)
    OUT_SVG.write_text(svg, encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(detail(nodes, edges), separators=(",", ":")), "utf-8"
    )

    # The graph is inlined rather than loaded, so the page needs no request and
    # no script to draw it. Its prose lives either side of the graph, in two
    # fragments this joins.
    OUT_PAGE.write_text(
        (HEAD.read_text(encoding="utf-8") + svg + TAIL.read_text(encoding="utf-8")),
        encoding="utf-8",
    )

    print(f"{len(nodes)} nodes, {len(edges)} edges, {len(items)} items")
    for item in items:
        print(
            f"  {item['name']:<26} {item['count']:>4} objects, {item['layers']} layers"
        )
    print(f"{OUT_SVG.relative_to(ROOT)}  {width:.0f} x {height:.0f}")
    print(f"{OUT_JSON.relative_to(ROOT)}  {OUT_JSON.stat().st_size // 1024} KB")
    print(f"{OUT_PAGE.relative_to(ROOT)}  {OUT_PAGE.stat().st_size // 1024} KB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
