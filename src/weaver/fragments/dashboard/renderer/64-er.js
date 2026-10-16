// The ER diagram: declared keys and relationships, or one semantic model's
// relationships, around a focus table. Relationship graphs may contain cycles;
// every walk keeps a visited set.

var ER = { NODES: 60, BOX: 220, HEAD: 34, ROW: 18, GAP_X: 36, GAP_ROW: 84, COLUMNS: 8 };

/* The declared relationships of the Lakehouses and Warehouses, or of one of
   them: a relationship belongs to an item when either end is in it. Both ends
   keep their foreign and primary columns. */
function erGraph(m, item) {
  var md = metadata(m), nodes = new Map(), edges = [];
  function node(id) {
    if (nodes.has(id)) return;
    var parts = id.split("|"), owner = m.byIdentity.get(id) || null, declared = md.tables.get(id);
    // Primary keys travel with the table; the key table holds the others.
    var keys = declared && declared.primarykey ? [{ type: "Primary key", columns: splitList(declared.primarykey) }] : [];
    nodes.set(id, { id: id, label: owner ? owner.label : parts[1] + "." + parts[2], item: parts[0],
      columns: (md.columns.get(id) || []).map(function (c) { return c.column; }),
      keys: keys.concat((md.keys.get(id) || []).map(function (k) { return { type: k.type, columns: splitList(k.columns) }; })),
      owner: owner, refs: [], back: [] });
  }
  md.foreignKeys.forEach(function (fk) {
    if (item && item !== "all" && fk.from.split("|")[0] !== item && fk.to.split("|")[0] !== item) return;
    node(fk.from); node(fk.to);
    edges.push({ from: fk.from, to: fk.to, fromColumns: fk.fromColumns, toColumns: fk.toColumns, label: "", active: true, many: true });
  });
  edges.forEach(function (e, i) { e.id = i; nodes.get(e.from).refs.push(e); nodes.get(e.to).back.push(e); });
  return { nodes: nodes, edges: edges };
}

/* Up to ER.NODES tables around the focus, nearest first; a signed distance
   puts the tables it references above and those referencing it below. */
function erWindow(g, focus, depth) {
  if (!g.nodes.has(focus)) return null;
  var dist = new Map([[focus, 0]]), frontier = [focus], limit = depth < 0 ? Infinity : depth;
  for (var step = 1; step <= limit && frontier.length; step++) {
    var next = [];
    frontier.forEach(function (id) {
      var n = g.nodes.get(id), sign = dist.get(id) < 0 ? -1 : 1;
      n.refs.forEach(function (e) { if (!dist.has(e.to)) { dist.set(e.to, dist.get(id) === 0 ? step : sign * step); next.push(e.to); } });
      n.back.forEach(function (e) { if (!dist.has(e.from)) { dist.set(e.from, dist.get(id) === 0 ? -step : sign * step); next.push(e.from); } });
    });
    frontier = next;
  }
  var ranked = Array.from(dist, function (x) { return { id: x[0], dist: x[1] }; }).sort(function (a, b) {
    return Math.abs(a.dist) - Math.abs(b.dist) || compare(g.nodes.get(a.id).label, g.nodes.get(b.id).label) || compare(a.id, b.id);
  });
  var kept = new Map();
  ranked.slice(0, ER.NODES).forEach(function (r) { kept.set(r.id, r.dist); });
  var more = new Map();
  kept.forEach(function (_, id) {
    var n = g.nodes.get(id), hidden = new Set();
    n.refs.forEach(function (e) { if (!kept.has(e.to)) hidden.add(e.to); });
    n.back.forEach(function (e) { if (!kept.has(e.from)) hidden.add(e.from); });
    if (hidden.size) more.set(id, Array.from(hidden));
  });
  return { focus: focus, kept: kept, reach: dist.size, truncated: dist.size > kept.size, more: more,
    edges: g.edges.filter(function (e) { return kept.has(e.from) && kept.has(e.to); }) };
}

function erRows(n, edges) {
  var keyCols = [], fkCols = [], rows = [];
  n.keys.forEach(function (k) { k.columns.forEach(function (c) { if (keyCols.indexOf(c) < 0) keyCols.push(c); }); });
  edges.forEach(function (e) {
    if (e.from === n.id) e.fromColumns.forEach(function (c) { if (fkCols.indexOf(c) < 0) fkCols.push(c); });
    if (e.to === n.id) e.toColumns.forEach(function (c) { if (keyCols.indexOf(c) < 0 && fkCols.indexOf(c) < 0) keyCols.push(c); });
  });
  keyCols.forEach(function (c) { rows.push({ name: c, mark: "key" }); });
  fkCols.forEach(function (c) { if (keyCols.indexOf(c) < 0) rows.push({ name: c, mark: "fk" }); });
  var rest = n.columns.filter(function (c) { return keyCols.indexOf(c) < 0 && fkCols.indexOf(c) < 0; });
  var room = Math.max(0, ER.COLUMNS - rows.length);
  rest.slice(0, room).forEach(function (c) { rows.push({ name: c, mark: "" }); });
  return { rows: rows, hidden: rest.length - Math.min(room, rest.length) };
}

/* The ER diagram around a focus table, drawn into container. s holds the
   focus, depth and viewport; onPick hears a table chosen in the diagram. */
function erDiagram(container, m, s, options) {
  if (s.depth === undefined) s.depth = 1;
  var g = erGraph(m, options.item), host = el("div", { "class": "wd-er wd-er-embedded" }), line = el("div", { "class": "wd-status-line" });
  if (!g.nodes.size) { container.appendChild(empty("No declared relationship is recorded" + (options.item && options.item !== "all" ? " for " + options.item : "") + ".")); return { focus: function () {} }; }
  if (!s.focus || !g.nodes.has(s.focus)) {
    var best = null;
    g.nodes.forEach(function (n) { if (!best || n.refs.length + n.back.length > best.refs.length + best.back.length || (n.refs.length + n.back.length === best.refs.length + best.back.length && compare(n.label, best.label) < 0)) best = n; });
    s.focus = best ? best.id : null;
  }
  container.appendChild(el("div", { "class": "wd-toolbar" }, [
    segmented("Depth", DEPTHS, s.depth, function (d) { s.depth = d; s.view = null; draw(); saveState(); }, depthLabel), line]));
  container.appendChild(host);
  container.appendChild(el("div", { "class": "wd-legend" }, [
    el("span", { "class": "wd-key", text: "⚿ key column" }), el("span", { "class": "wd-key", text: "↗ foreign key column" }),
    el("span", { "class": "wd-key", text: "* many · 1 one" }),
    el("span", { "class": "wd-key" }, [el("i", { "class": "wd-key-more", text: "+3" }), "related tables beyond the window: select one to move there"])
  ]));
  function pick(id) { s.focus = id; s.view = null; draw(); saveState(); if (options.onPick) options.onPick(id); }
  function draw() {
    host.textContent = "";
    var win = erWindow(g, s.focus, s.depth);
    if (!win) { host.appendChild(empty("Search for a table.")); return; }
    line.textContent = win.kept.size + " of " + fmt(g.nodes.size) + " related tables" + (win.truncated ? " · cut to the " + ER.NODES + " nearest of " + win.reach + "; lower the depth, or select a table with a +N marker to move there" : "");
    // Top to bottom: the tables others reference sit above the tables that
    // reference them, one row per distance from the focus.
    var levels = new Map();
    win.kept.forEach(function (d, id) { addTo(levels, d, id); });
    var order = Array.from(levels.keys()).sort(function (a, b) { return b - a; }), boxes = new Map(), placedX = new Map();
    var y = 0, left = Infinity, right = -Infinity;
    order.forEach(function (d) {
      var ids = levels.get(d);
      // Under the tables each relates to in the row above, where it has any.
      var bary = new Map(ids.map(function (id) {
        var n = g.nodes.get(id), near = n.refs.map(function (e) { return e.to; }).concat(n.back.map(function (e) { return e.from; }))
          .filter(function (k) { return placedX.has(k); }).map(function (k) { return placedX.get(k); });
        return [id, near.length ? near.reduce(function (a, b) { return a + b; }, 0) / near.length : 0];
      }));
      ids.sort(function (a, b) { return bary.get(a) - bary.get(b) || compare(g.nodes.get(a).label, g.nodes.get(b).label); });
      var rows = ids.map(function (id) { return erRows(g.nodes.get(id), win.edges); });
      var height = Math.max.apply(null, rows.map(function (r) { return ER.HEAD + Math.max(1, r.rows.length + (r.hidden ? 1 : 0)) * ER.ROW + 8; }));
      var width = ids.length * ER.BOX + (ids.length - 1) * ER.GAP_X, x = -width / 2;
      ids.forEach(function (id, i) {
        var r = rows[i], h = ER.HEAD + Math.max(1, r.rows.length + (r.hidden ? 1 : 0)) * ER.ROW + 8;
        boxes.set(id, { x: x, y: y, h: h, rows: r.rows, hidden: r.hidden });
        placedX.set(id, x + ER.BOX / 2);
        left = Math.min(left, x); right = Math.max(right, x + ER.BOX);
        x += ER.BOX + ER.GAP_X;
      });
      y += height + ER.GAP_ROW;
    });
    var x0 = left - 80, x1 = right + 80, top = -40, bottom = y - ER.GAP_ROW + 40;
    var svg = sv("svg", { "class": "wd-er-svg", role: "img", "aria-label": "Relationship diagram" }), stage = sv("g");
    svg.appendChild(stage);
    win.edges.forEach(function (e) {
      var a = boxes.get(e.from), b = boxes.get(e.to), path, ax, ay, bx, by;
      if (a.y === b.y) {
        // Same row: arc over the boxes between them.
        ax = a.x + ER.BOX / 2; bx = b.x + ER.BOX / 2; ay = a.y; by = b.y;
        var lift = Math.min(90, 30 + Math.abs(bx - ax) / 6);
        path = "M" + ax + " " + ay + "C" + ax + " " + (ay - lift) + " " + bx + " " + (by - lift) + " " + bx + " " + by;
      } else {
        var down = a.y < b.y;
        ax = a.x + ER.BOX / 2; bx = b.x + ER.BOX / 2;
        ay = down ? a.y + a.h : a.y; by = down ? b.y : b.y + b.h;
        var c = Math.max(30, Math.abs(by - ay) / 2) * (down ? 1 : -1);
        path = "M" + ax + " " + ay + "C" + ax + " " + (ay + c) + " " + bx + " " + (by - c) + " " + bx + " " + by;
      }
      stage.appendChild(sv("path", { "class": "wd-er-edge" + (e.active ? "" : " wd-er-inactive") + (e.from === s.focus || e.to === s.focus ? " wd-er-focus" : ""), d: path },
        [sv("title", {}, [(e.label ? e.label + ": " : "") + g.nodes.get(e.from).label + " (" + e.fromColumns.join(", ") + ") → " + g.nodes.get(e.to).label + " (" + e.toColumns.join(", ") + ")" + (e.active ? "" : " · inactive")])]));
      var dy = a.y === b.y ? -8 : ay < by ? 12 : -5, ey = a.y === b.y ? -8 : ay < by ? -5 : 12;
      stage.appendChild(sv("text", { "class": "wd-er-card", x: ax + 6, y: ay + dy }, [e.many === false ? "1" : "*"]));
      stage.appendChild(sv("text", { "class": "wd-er-card", x: bx + 6, y: by + ey }, [e.one === false ? "*" : "1"]));
    });
    boxes.forEach(function (box, id) {
      var n = g.nodes.get(id), focus = id === s.focus;
      var group = sv("g", { "class": "wd-er-box" + (focus ? " wd-er-box-focus" : ""), transform: "translate(" + box.x + " " + box.y + ")", tabindex: "0", role: "button", "aria-label": n.label });
      group.appendChild(sv("rect", { width: ER.BOX, height: box.h, rx: 9 }));
      group.appendChild(sv("rect", { "class": "wd-er-head", width: ER.BOX, height: ER.HEAD - 6, rx: 9 }));
      group.appendChild(sv("text", { "class": "wd-er-title", x: 12, y: 17 }, [clipText(n.label, 28)]));
      group.appendChild(sv("text", { "class": "wd-er-item", x: 12, y: 29 }, [clipText(itemName(n.item), 34)]));
      box.rows.forEach(function (r, i) {
        group.appendChild(sv("text", { "class": "wd-er-col" + (r.mark ? " wd-er-" + r.mark : ""), x: 12, y: ER.HEAD + i * ER.ROW + 13 },
          [(r.mark === "key" ? "⚿ " : r.mark === "fk" ? "↗ " : "   ") + clipText(r.name, 30)]));
      });
      if (box.hidden) group.appendChild(sv("text", { "class": "wd-er-col wd-er-hidden", x: 12, y: ER.HEAD + box.rows.length * ER.ROW + 13 }, [box.hidden + " more described columns"]));
      var more = win.more.get(id);
      if (more) {
        var badge = sv("g", { "class": "wd-er-more", transform: "translate(" + (ER.BOX - 34) + " -8)" }, [sv("rect", { width: 30, height: 16, rx: 8 }), sv("text", { x: 15, y: 11.5 }, ["+" + more.length]),
          sv("title", {}, [more.length + " related tables beyond the window. Select to focus here."])]);
        group.appendChild(badge);
      }
      group.addEventListener("click", function () { pick(id); });
      group.addEventListener("keydown", function (ev) { if (ev.key === "Enter") pick(id); });
      stage.appendChild(group);
    });
    host.appendChild(svg);
    panZoom(svg, stage, s, { x: x0, y: top - 40, w: x1 - x0, h: bottom - top + 80 });
  }
  draw();
  return { has: function (id) { return g.nodes.has(id); }, focus: function (id) { if (g.nodes.has(id)) { s.focus = id; s.view = null; draw(); } } };
}

function clipText(text, n) { text = String(text || ""); return text.length > n ? text.slice(0, n - 1) + "…" : text; }

function sv(tag, attrs, children) {
  var e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  setAttrs(e, attrs);
  (children || []).forEach(function (c) { if (c) e.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
  return e;
}

/* Pan and zoom for an SVG stage, with the same gestures as the graph. */
function panZoom(svg, stage, s, bounds) {
  function box() { return svg.getBoundingClientRect(); }
  function apply() { stage.setAttribute("transform", "translate(" + s.view.x + " " + s.view.y + ") scale(" + s.view.k + ")"); }
  function fit() {
    var b = box(), k = Math.min(1.1, Math.max(0.2, Math.min(b.width / bounds.w, b.height / bounds.h)));
    s.view = { k: k, x: b.width / 2 - (bounds.x + bounds.w / 2) * k, y: b.height / 2 - (bounds.y + bounds.h / 2) * k };
    apply();
  }
  if (s.view) apply(); else (root.requestAnimationFrame || setTimeout)(fit);
  var drag = null;
  svg.addEventListener("pointerdown", function (e) {
    if (e.target.closest && e.target.closest(".wd-er-box")) return;
    drag = { x: e.clientX, y: e.clientY, v: s.view, touch: e.pointerType === "touch" };
  });
  svg.addEventListener("pointermove", function (e) {
    if (!drag || !s.view) return;
    s.view = { k: drag.v.k, x: drag.v.x + e.clientX - drag.x, y: drag.v.y + (drag.touch ? 0 : e.clientY - drag.y) };
    apply();
  });
  ["pointerup", "pointercancel", "pointerleave"].forEach(function (t) { svg.addEventListener(t, function () { drag = null; }); });
  svg.addEventListener("wheel", function (e) {
    if (!(e.ctrlKey || e.metaKey) && pageScrolls()) return;
    e.preventDefault();
    var b = box(), k = Math.min(2.5, Math.max(0.1, s.view.k * Math.exp(-e.deltaY * 0.0025))), f = k / s.view.k, px = e.clientX - b.left, py = e.clientY - b.top;
    s.view = { k: k, x: px - (px - s.view.x) * f, y: py - (py - s.view.y) * f };
    apply();
  }, { passive: false });
}
