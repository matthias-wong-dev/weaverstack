// The estate layout. Every item is one band; inside it, shortcuts take the
// first layer, Tests and Assumptions the last, and every other node sits by its
// longest path to a sink within the item, so the item's outputs align on the
// right. Positions depend on the installed graph alone, never on the focus, so
// moving the focus never moves a node.

var GEOMETRY = { COL: 214, SUB: 196, ROW: 24, HEAD: 50, PAD: 18, GAP: 30 };
var BUDGET = { NODES: 150, EDGES: 450, CONTEXT: 80, REVEAL: 40, REVEALED: 240 };

function itemLevels(m, items) {
  var ups = new Map(), level = new Map(), visiting = new Set();
  items.forEach(function (it) { ups.set(it.label, new Set()); });
  m.nodes.forEach(function (n) {
    n.vup.forEach(function (u) {
      var from = m.nodes.get(u).item;
      if (from !== n.item && ups.has(n.item) && ups.has(from)) ups.get(n.item).add(from);
    });
  });
  function visit(label) {
    if (level.has(label)) return level.get(label);
    if (visiting.has(label)) return -1;
    visiting.add(label);
    var deepest = -1;
    Array.from(ups.get(label)).sort(compare).forEach(function (u) { deepest = Math.max(deepest, visit(u)); });
    visiting.delete(label);
    level.set(label, deepest + 1);
    return deepest + 1;
  }
  items.forEach(function (it) { visit(it.label); });
  return level;
}

/* Layers inside one item: longest path to a sink over the item's own edges. */
function itemLayers(m, it) {
  var shortcuts = [], validations = [], ranked = [], inItem = new Set();
  it.nodes.forEach(function (n) {
    if (n.kind === KIND.SHORTCUT) shortcuts.push(n);
    else if (isValidation(n)) validations.push(n);
    else { ranked.push(n); inItem.add(n.key); }
  });
  var out = new Map(), indegree = new Map(), order = [], ordered = new Set(), queue = [];
  ranked.forEach(function (n) {
    indegree.set(n.key, n.vup.filter(function (u) { return inItem.has(u); }).length);
  });
  ranked.forEach(function (n) { if (!indegree.get(n.key)) queue.push(n.key); });
  for (var i = 0; i < queue.length; i++) {
    order.push(queue[i]);
    ordered.add(queue[i]);
    m.nodes.get(queue[i]).vdown.forEach(function (d) {
      if (!inItem.has(d)) return;
      var k = indegree.get(d) - 1;
      indegree.set(d, k);
      if (k === 0) queue.push(d);
    });
  }
  // A cycle cannot occur in an installed graph; its nodes would still be placed.
  ranked.forEach(function (n) { if (!ordered.has(n.key)) order.push(n.key); });
  var toSink = new Map();
  for (var j = order.length - 1; j >= 0; j--) {
    var key = order[j], best = 0;
    m.nodes.get(key).vdown.forEach(function (d) { if (toSink.has(d)) best = Math.max(best, toSink.get(d) + 1); });
    toSink.set(key, best);
  }
  var depth = 0;
  toSink.forEach(function (v) { depth = Math.max(depth, v); });
  var offset = shortcuts.length ? 1 : 0;
  ranked.forEach(function (n) { out.set(n.key, offset + depth - toSink.get(n.key)); });
  shortcuts.forEach(function (n) { out.set(n.key, 0); });
  var last = (ranked.length ? depth + offset : offset - 1) + 1;
  validations.forEach(function (n) { out.set(n.key, last); });
  return { layer: out, shortcuts: shortcuts.length > 0, validations: validations.length > 0, last: last };
}

/* Row order inside each layer: alphabetical, then barycentre sweeps over the
   item's own edges so that related nodes sit near one another. */
function orderLayers(m, it, layers) {
  var byLayer = [];
  it.nodes.forEach(function (n) {
    var l = layers.layer.get(n.key);
    (byLayer[l] = byLayer[l] || []).push(n);
  });
  for (var l = 0; l < byLayer.length; l++) byLayer[l] = byLayer[l] || [];
  var index = new Map();
  function renumber(list) { list.forEach(function (n, i) { index.set(n.key, i / Math.max(1, list.length - 1)); }); }
  byLayer.forEach(renumber);
  function sweep(list, neighbours) {
    var bary = new Map();
    list.forEach(function (n) {
      var near = neighbours(n).filter(function (k) { return index.has(k) && m.nodes.get(k).item === it.label; });
      bary.set(n.key, near.length ? near.reduce(function (s, k) { return s + index.get(k); }, 0) / near.length : index.get(n.key));
    });
    list.sort(function (a, b) { return bary.get(a.key) - bary.get(b.key) || compare(a.schema, b.schema) || compare(a.label, b.label) || compare(a.key, b.key); });
    renumber(list);
  }
  for (var pass = 0; pass < 3; pass++) {
    for (var f = 1; f < byLayer.length; f++) sweep(byLayer[f], function (n) { return n.vup; });
    for (var b = byLayer.length - 2; b >= 0; b--) sweep(byLayer[b], function (n) { return n.vdown; });
  }
  return byLayer;
}

function layoutEstate(m) {
  var items = visibleItems(m).filter(function (it) { return it.nodes.length; });
  var levels = itemLevels(m, items);
  items.sort(function (a, b) {
    return levels.get(a.label) - levels.get(b.label) || typeRank(a.type) - typeRank(b.type) || compare(a.name, b.name);
  });
  var G = GEOMETRY, pos = new Map(), bands = [];
  items.forEach(function (it) {
    var layers = itemLayers(m, it), byLayer = orderLayers(m, it, layers);
    var maxRows = Math.max(40, Math.ceil(Math.sqrt(it.nodes.length) * 5));
    var x = 0, rows = 1, columns = [];
    byLayer.forEach(function (list, l) {
      var subs = Math.max(1, Math.ceil(list.length / maxRows)), perSub = Math.max(1, Math.ceil(list.length / subs));
      var width = subs > 1 ? subs * G.SUB : G.COL;
      columns.push({ layer: l, x: x, width: width, count: list.length,
        kind: layers.shortcuts && l === 0 ? "shortcuts" : layers.validations && l === layers.last ? "validations" : "objects" });
      list.forEach(function (n, i) {
        var sub = Math.floor(i / perSub), row = i - sub * perSub;
        pos.set(n.key, { x: x + sub * G.SUB, y: row, layer: l });
      });
      rows = Math.max(rows, Math.min(list.length, perSub));
      x += width;
    });
    bands.push({ item: it, width: G.PAD * 2 + x, height: G.HEAD + rows * G.ROW + G.PAD, columns: columns, level: levels.get(it.label) });
  });
  // Items whose producers sit further left go further right: each level of
  // the item graph is a column of bands, so cross-item edges run forward.
  var byLevel = [];
  bands.forEach(function (b) { (byLevel[b.level] = byLevel[b.level] || []).push(b); });
  var cx = 0, width = 0, height = 0, centre = new Map();
  byLevel.forEach(function (column) {
    if (!column) return;
    column.forEach(function (b) {
      var ups = [];
      b.item.nodes.forEach(function (n) {
        n.vup.forEach(function (u) { var c = centre.get(m.nodes.get(u).item); if (c !== undefined) ups.push(c); });
      });
      b.order = ups.length ? ups.reduce(function (a, v) { return a + v; }, 0) / ups.length : Infinity;
    });
    column.sort(function (a, b) {
      return (a.order - b.order) || typeRank(a.item.type) - typeRank(b.item.type) || compare(a.item.name, b.item.name);
    });
    var cy = 0, columnWidth = 0;
    column.forEach(function (b) {
      b.x = cx; b.y = cy;
      centre.set(b.item.label, cy + b.height / 2);
      cy += b.height + G.GAP;
      columnWidth = Math.max(columnWidth, b.width);
    });
    height = Math.max(height, cy - G.GAP);
    cx += columnWidth + G.GAP * 3;
    width = cx - G.GAP * 3;
  });
  bands.forEach(function (b) {
    b.item.nodes.forEach(function (n) {
      var p = pos.get(n.key);
      p.x = b.x + G.PAD + p.x + 8;
      p.y = b.y + G.HEAD + p.y * G.ROW + G.ROW / 2;
      p.band = b;
    });
    b.columns.forEach(function (c) { c.x += b.x + G.PAD; });
  });
  return { bands: bands, pos: pos, width: width, height: height };
}

/* ---- the window around a focus ------------------------------------------ */

/* Three levels: the focus and its lineage to the requested depths, a bounded
   ring of surrounding context, and for every shown node the neighbours left
   out, which become continuation markers. */
function focusWindow(m, focus, upDepth, downDepth, revealed) {
  if (!m.nodes.has(focus)) return null;
  var dist = new Map([[focus, 0]]);
  function walk(next, sign, depth) {
    var limit = depth < 0 ? Infinity : depth, frontier = [focus];
    for (var step = 1; step <= limit && frontier.length; step++) {
      var found = [];
      frontier.forEach(function (k) {
        next(m.nodes.get(k)).forEach(function (j) { if (!dist.has(j)) { dist.set(j, sign * step); found.push(j); } });
      });
      frontier = found;
    }
  }
  walk(function (n) { return n.vup; }, -1, upDepth);
  walk(function (n) { return n.vdown; }, 1, downDepth);
  var ranked = Array.from(dist, function (e) { return { key: e[0], dist: e[1] }; });
  ranked.sort(function (a, b) {
    return Math.abs(a.dist) - Math.abs(b.dist) || (a.dist > 0) - (b.dist > 0) ||
      compare(m.nodes.get(a.key).label, m.nodes.get(b.key).label) || compare(a.key, b.key);
  });
  var lineage = new Map();
  ranked.slice(0, BUDGET.NODES).forEach(function (r) { lineage.set(r.key, r.dist); });
  var shown = new Set(lineage.keys()), context = new Set();
  var room = Math.min(BUDGET.CONTEXT, BUDGET.NODES - lineage.size), ring = Array.from(lineage.keys());
  for (var hop = 1; hop <= 2 && room > 0; hop++) {
    var next = [];
    ring.forEach(function (k) {
      var n = m.nodes.get(k);
      n.vup.concat(n.vdown).forEach(function (j) { if (!shown.has(j) && next.indexOf(j) < 0) next.push(j); });
    });
    next.sort(function (a, b) { return compare(m.nodes.get(a).label, m.nodes.get(b).label) || compare(a, b); });
    next = next.slice(0, room);
    next.forEach(function (j) { shown.add(j); context.add(j); });
    room -= next.length;
    ring = next;
  }
  var extra = 0;
  (revealed || []).forEach(function (k) {
    if (m.nodes.has(k) && !shown.has(k) && extra < BUDGET.REVEALED) { shown.add(k); context.add(k); extra += 1; }
  });
  var edges = [], contextEdges = [];
  shown.forEach(function (d) {
    m.nodes.get(d).vup.forEach(function (u) {
      if (!shown.has(u)) return;
      var du = lineage.get(u), dd = lineage.get(d);
      var line = du !== undefined && dd !== undefined && ((dd <= 0 && du < 0 && du < dd) || (du >= 0 && dd > 0 && dd > du));
      (line ? edges : contextEdges).push({ u: u, d: d, side: line ? (dd <= 0 ? "up" : "down") : null });
    });
  });
  var edgeTotal = edges.length + contextEdges.length;
  edges = edges.concat(contextEdges).slice(0, BUDGET.EDGES);
  var more = new Map();
  shown.forEach(function (k) {
    var n = m.nodes.get(k);
    var up = n.vup.filter(function (j) { return !shown.has(j); }), down = n.vdown.filter(function (j) { return !shown.has(j); });
    if (up.length || down.length) more.set(k, { up: up, down: down });
  });
  return { focus: focus, lineage: lineage, context: context, shown: shown, edges: edges, more: more,
    reach: dist.size, truncated: dist.size > lineage.size, edgesDropped: edgeTotal - edges.length };
}

/* The neighbours a marker reveals, nearest names first. */
function revealFrom(m, keys) {
  return keys.slice().sort(function (a, b) { return compare(m.nodes.get(a).label, m.nodes.get(b).label) || compare(a, b); }).slice(0, BUDGET.REVEAL);
}
