// The ER diagram: declared keys and relationships around the selected tables.
// A table's parents are the tables it references, on the primary key side, and
// its children the tables referencing it. Relationship graphs may contain
// cycles; every walk keeps a visited set.

var ER = { NODES: 80, EXPANDED: 120, WRAP: 8, BOX: 220, HEAD: 34, ROW: 18, GAP_X: 64, GAP_ROW: 120, COLUMNS: 8 };

/* The declared tables and relationships of the Lakehouses and Warehouses, or
   of one of them: a relationship belongs to an item when either end is in it.
   A table with no relationship is a node of its own. Both ends of a
   relationship keep their foreign and primary columns. */
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
  md.tables.forEach(function (_, id) { if (!item || item === "all" || id.split("|")[0] === item) node(id); });
  edges.forEach(function (e, i) { e.id = i; nodes.get(e.from).refs.push(e); nodes.get(e.to).back.push(e); });
  return { nodes: nodes, edges: edges };
}

function parentsOf(n) { return n.refs.map(function (e) { return e.to; }); }
function childrenOf(n) { return n.back.map(function (e) { return e.from; }); }

/* The selected tables with their parents to depth up and their children to
   depth down, nearest first up to ER.NODES, plus what the reader has expanded.
   rank orders the rows: parents above, children below. With nothing selected
   the window is the whole graph, or its ER.NODES most related tables, ranked
   by each table's longest chain of references. Every related table left out
   is counted on the shown table it hangs from. */
function erWindow(g, selection, up, down, expanded) {
  var seeds = (Array.isArray(selection) ? selection : [selection]).filter(function (id) { return g.nodes.has(id); });
  var rank = new Map(), reach = g.nodes.size;
  if (seeds.length) {
    seeds.forEach(function (id) { rank.set(id, 0); });
    [[parentsOf, -1, up], [childrenOf, 1, down]].forEach(function (w) {
      var limit = w[2] < 0 ? Infinity : w[2], frontier = seeds.slice();
      for (var step = 1; step <= limit && frontier.length; step++) {
        var found = [];
        frontier.forEach(function (id) {
          w[0](g.nodes.get(id)).forEach(function (j) { if (!rank.has(j)) { rank.set(j, w[1] * step); found.push(j); } });
        });
        frontier = found;
      }
    });
    reach = rank.size;
    var ranked = Array.from(rank, function (x) { return { id: x[0], d: x[1] }; }).sort(function (a, b) {
      return Math.abs(a.d) - Math.abs(b.d) || a.d - b.d || compare(g.nodes.get(a.id).label, g.nodes.get(b.id).label) || compare(a.id, b.id);
    });
    rank = new Map(ranked.slice(0, ER.NODES).map(function (r) { return [r.id, r.d]; }));
  } else {
    var most = Array.from(g.nodes.values()).sort(function (a, b) {
      return b.refs.length + b.back.length - a.refs.length - a.back.length || compare(a.label, b.label) || compare(a.id, b.id);
    }).slice(0, ER.NODES), inside = new Set(most.map(function (n) { return n.id; }));
    var visiting = new Set();
    var level = function (id) {
      if (rank.has(id)) return rank.get(id);
      if (visiting.has(id)) return 0;
      visiting.add(id);
      var deepest = -1;
      parentsOf(g.nodes.get(id)).forEach(function (p) { if (p !== id && inside.has(p)) deepest = Math.max(deepest, level(p)); });
      visiting.delete(id);
      rank.set(id, deepest + 1);
      return deepest + 1;
    };
    inside.forEach(level);
  }
  var kept = rank.size, added = new Set();
  // An expanded table sits a row above the shown table it is a parent of, or a
  // row below the one it is a child of.
  (expanded || []).forEach(function (id) {
    if (!g.nodes.has(id) || rank.has(id) || added.size >= ER.EXPANDED) return;
    var n = g.nodes.get(id), at = null;
    childrenOf(n).forEach(function (c) { if (rank.has(c)) at = at === null ? rank.get(c) - 1 : Math.min(at, rank.get(c) - 1); });
    if (at === null) parentsOf(n).forEach(function (p) { if (rank.has(p)) at = at === null ? rank.get(p) + 1 : Math.max(at, rank.get(p) + 1); });
    if (at === null) return;
    rank.set(id, at); added.add(id);
  });
  var more = new Map(), hidden = new Set();
  rank.forEach(function (_, id) {
    var n = g.nodes.get(id), out = function (j) { return !rank.has(j); };
    var ups = unique(parentsOf(n).filter(out)), downs = unique(childrenOf(n).filter(out));
    ups.concat(downs).forEach(function (j) { hidden.add(j); });
    if (ups.length || downs.length) more.set(id, { up: ups, down: downs });
  });
  return { seeds: new Set(seeds), rank: rank, expanded: added, reach: reach, truncated: reach > kept, more: more, hidden: hidden.size,
    edges: g.edges.filter(function (e) { return rank.has(e.from) && rank.has(e.to); }) };
}

function unique(list) { return list.filter(function (x, i) { return list.indexOf(x) === i; }); }

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

/* Boxes for a window, one row per rank, each placed under the tables it relates
   to in the rows above. */
function erLayout(g, win) {
  var levels = new Map();
  win.rank.forEach(function (d, id) { addTo(levels, d, id); });
  var order = Array.from(levels.keys()).sort(function (a, b) { return a - b; }), boxes = new Map(), placedX = new Map();
  var y = 0, left = Infinity, right = -Infinity;
  order.forEach(function (d) {
    var ids = levels.get(d);
    var bary = new Map(ids.map(function (id) {
      var n = g.nodes.get(id), near = parentsOf(n).concat(childrenOf(n)).filter(function (k) { return placedX.has(k); }).map(function (k) { return placedX.get(k); });
      return [id, near.length ? near.reduce(function (a, b) { return a + b; }, 0) / near.length : 0];
    }));
    ids.sort(function (a, b) { return bary.get(a) - bary.get(b) || compare(g.nodes.get(a).label, g.nodes.get(b).label) || compare(a, b); });
    // A rank wider than ER.WRAP tables wraps onto further rows.
    for (var start = 0; start < ids.length; start += ER.WRAP) {
      var line = ids.slice(start, start + ER.WRAP);
      var rows = line.map(function (id) { return erRows(g.nodes.get(id), win.edges); });
      var heights = rows.map(function (r) { return ER.HEAD + Math.max(1, r.rows.length + (r.hidden ? 1 : 0)) * ER.ROW + 8; });
      var width = line.length * ER.BOX + (line.length - 1) * ER.GAP_X, x = -width / 2;
      line.forEach(function (id, i) {
        boxes.set(id, { x: x, y: y, h: heights[i], rows: rows[i].rows, hidden: rows[i].hidden });
        placedX.set(id, x + ER.BOX / 2);
        left = Math.min(left, x); right = Math.max(right, x + ER.BOX);
        x += ER.BOX + ER.GAP_X;
      });
      y += Math.max.apply(null, heights) + (start + ER.WRAP < ids.length ? ER.GAP_ROW / 2 : ER.GAP_ROW);
    }
  });
  return { boxes: boxes, bounds: { x: left - 80, y: -80, w: right - left + 160, h: y - ER.GAP_ROW + 160 } };
}

/* The ER page's workspace, framed as the Load orchestration graph is: s holds
   the selection, depths, expansions and viewport. options.item limits it to one
   item's relationships; options.tools come first in its toolbar; options.onPick
   hears a table chosen in the diagram, and options.describe fills the details. */
function erWorkspace(body, m, s, options) {
  var g = erGraph(m, options.item);
  s.selection = (s.selection || []).filter(function (id) { return g.nodes.has(id); });
  s.expanded = (s.expanded || []).filter(function (id) { return g.nodes.has(id); });
  if (s.up === undefined) s.up = 1;
  if (s.down === undefined) s.down = 1;
  var f = workspaceFrame(s, function () {}), drawing = el("div", { "class": "wd-er-drawing" }), zoom = null, notice = null;
  function focus() { return s.selection[s.selection.length - 1] || null; }
  /* A plain choice replaces the selection; Shift, Ctrl or ⌘ adds or removes. */
  function select(id, adding) {
    if (!id) s.selection = [];
    else if (adding) {
      var at = s.selection.indexOf(id);
      if (at >= 0) s.selection.splice(at, 1); else s.selection.push(id);
    } else s.selection = [id];
    s.expanded = []; s.view = null; notice = null;
    draw();
    saveState();
  }
  function expand(id, side) {
    var hidden = win().more.get(id)[side].slice().sort(function (a, b) { return compare(g.nodes.get(a).label, g.nodes.get(b).label) || compare(a, b); });
    var before = zoom && zoom.screenOf(id);
    s.expanded = s.expanded.concat(hidden.slice(0, BUDGET.REVEAL).filter(function (k) { return s.expanded.indexOf(k) < 0; }));
    draw(id, before);
    saveState();
  }
  function win() { return erWindow(g, s.selection, s.up, s.down, s.expanded); }
  function status(w) {
    f.line.textContent = "";
    var parts = [];
    if (w) {
      parts.push(fmt(w.rank.size) + " of " + fmt(g.nodes.size) + " tables shown" + (w.seeds.size > 1 ? " · " + w.seeds.size + " selected" : ""));
      if (!w.seeds.size && w.truncated) parts.push("the most related; search or select a table to follow its relationships");
      if (w.seeds.size && !w.edges.length && !w.hidden) parts.push(w.seeds.size > 1 ? "none has a declared relationship" : "it has no declared relationship");
      if (w.expanded.size) parts.push(fmt(w.expanded.size) + " expanded");
      if (w.hidden) parts.push(fmt(w.hidden) + " more beside them; select a ↑ or ↓ marker to expand");
      if (w.seeds.size && w.truncated) parts.push(el("b", { "class": "wd-warn-text", text: "cut to the " + ER.NODES + " nearest of " + fmt(w.reach) + "; lower a depth" }));
    }
    if (notice) parts.push(el("b", { "class": "wd-warn-text", text: notice }));
    parts.forEach(function (p, i) { if (i) f.line.appendChild(document.createTextNode(" · ")); f.line.appendChild(typeof p === "string" ? document.createTextNode(p) : p); });
  }
  function renderChips() {
    selectionChips(f.chips, s.selection, focus(), function (id) { var n = g.nodes.get(id); return { kind: n.owner ? n.owner.kind : "Table", label: n.label, title: n.item + " · " + n.label }; },
      function (id) { if (options.onPick) options.onPick(id); if (zoom) zoom.fitBoxes([id]); }, function (id, adding) { select(id, adding); if (id && options.onPick && s.selection.indexOf(id) >= 0) options.onPick(id); });
  }
  /* Draw the window; given pin, keep that table where it was on screen. */
  function draw(pin, screen) {
    drawing.textContent = "";
    var w = win();
    status(w);
    renderChips();
    if (!w) { drawing.appendChild(empty("Search for a table, or select one, to draw its relationships.")); zoom = null; return; }
    var lay = erLayout(g, w), boxes = lay.boxes, current = focus();
    var svg = sv("svg", { "class": "wd-er-svg", role: "img", "aria-label": "Relationship diagram" }), stage = sv("g");
    svg.appendChild(stage);
    w.edges.forEach(function (e) {
      var a = boxes.get(e.from), b = boxes.get(e.to), path, ax, ay, bx, by;
      if (e.from === e.to) {
        // A table referencing itself: a loop off its right side.
        ax = a.x + ER.BOX; ay = a.y + ER.HEAD; bx = ax; by = ay + 26;
        path = "M" + ax + " " + ay + "C" + (ax + 44) + " " + (ay - 10) + " " + (bx + 44) + " " + (by + 10) + " " + bx + " " + by;
      } else if (a.y === b.y) {
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
      var lit = w.seeds.has(e.from) || w.seeds.has(e.to);
      stage.appendChild(sv("path", { "class": "wd-er-edge" + (e.active ? "" : " wd-er-inactive") + (lit ? " wd-er-focus" : ""), d: path },
        [sv("title", {}, [(e.label ? e.label + ": " : "") + g.nodes.get(e.from).label + " (" + e.fromColumns.join(", ") + ") → " + g.nodes.get(e.to).label + " (" + e.toColumns.join(", ") + ")" + (e.active ? "" : " · inactive")])]));
      if (e.from === e.to) return;
      var dy = a.y === b.y ? -8 : ay < by ? 12 : -5, ey = a.y === b.y ? -8 : ay < by ? -5 : 12;
      stage.appendChild(sv("text", { "class": "wd-er-card", x: ax + 6, y: ay + dy }, [e.many === false ? "1" : "*"]));
      stage.appendChild(sv("text", { "class": "wd-er-card", x: bx + 6, y: by + ey }, [e.one === false ? "*" : "1"]));
    });
    boxes.forEach(function (box, id) {
      var n = g.nodes.get(id), seed = w.seeds.has(id);
      var group = sv("g", { "class": "wd-er-box" + (seed ? " wd-er-box-focus" : "") + (id === current ? " wd-er-box-current" : ""), transform: "translate(" + box.x + " " + box.y + ")", tabindex: "0", role: "button", "aria-label": n.label });
      group.appendChild(sv("rect", { width: ER.BOX, height: box.h, rx: 9 }));
      group.appendChild(sv("rect", { "class": "wd-er-head", width: ER.BOX, height: ER.HEAD - 6, rx: 9 }));
      group.appendChild(sv("text", { "class": "wd-er-title", x: 12, y: 17 }, [clipText(n.label, 28)]));
      group.appendChild(sv("text", { "class": "wd-er-item", x: 12, y: 29 }, [clipText(itemName(n.item), 34)]));
      box.rows.forEach(function (r, i) {
        group.appendChild(sv("text", { "class": "wd-er-col" + (r.mark ? " wd-er-" + r.mark : ""), x: 12, y: ER.HEAD + i * ER.ROW + 13 },
          [(r.mark === "key" ? "⚿ " : r.mark === "fk" ? "↗ " : "   ") + clipText(r.name, 30)]));
      });
      if (box.hidden) group.appendChild(sv("text", { "class": "wd-er-col wd-er-hidden", x: 12, y: ER.HEAD + box.rows.length * ER.ROW + 13 }, [box.hidden + " more described columns"]));
      group.addEventListener("click", function (ev) { pickBox(id, ev.shiftKey || ev.metaKey || ev.ctrlKey); });
      group.addEventListener("keydown", function (ev) { if (ev.key === "Enter") pickBox(id, ev.shiftKey || ev.metaKey || ev.ctrlKey); });
      stage.appendChild(group);
      var more = w.more.get(id);
      if (more) {
        // Parents left out above the box, children below.
        if (more.up.length) stage.appendChild(marker(box, id, "up", more.up.length));
        if (more.down.length) stage.appendChild(marker(box, id, "down", more.down.length));
      }
    });
    drawing.appendChild(svg);
    zoom = panZoom(svg, stage, s, lay.bounds, boxes);
    if (pin && screen) zoom.pin(pin, screen);
  }
  function marker(box, id, side, count) {
    var text = (side === "up" ? "↑ " : "↓ ") + count, w = 12 + text.length * 6.4;
    var badge = sv("g", { "class": "wd-er-more", tabindex: "0", role: "button", transform: "translate(" + (box.x + ER.BOX - w - 10) + " " + (side === "up" ? box.y - 9 : box.y + box.h - 8) + ")" },
      [sv("rect", { width: w, height: 17, rx: 8.5 }), sv("text", { x: w / 2, y: 12 }, [text]),
        sv("title", {}, [count + (side === "up" ? (count === 1 ? " parent table" : " parent tables") : (count === 1 ? " child table" : " child tables")) + " not shown. Select to show " + (count > BUDGET.REVEAL ? "the first " + BUDGET.REVEAL : count === 1 ? "it" : "them") + "."])]);
    badge.addEventListener("click", function (ev) { ev.stopPropagation(); expand(id, side); });
    badge.addEventListener("keydown", function (ev) { if (ev.key === "Enter") expand(id, side); });
    return badge;
  }
  function pickBox(id, adding) {
    select(id, adding);
    if (options.onPick && s.selection.indexOf(id) >= 0) options.onPick(id);
  }
  f.mount(body, (options.tools || []).concat([
    segmented("Parents", DEPTHS, s.up, function (d) { s.up = d; s.expanded = []; s.view = null; draw(); saveState(); }, depthLabel),
    segmented("Children", DEPTHS, s.down, function (d) { s.down = d; s.expanded = []; s.view = null; draw(); saveState(); }, depthLabel),
    el("span", { "class": "wd-grow" }),
    el("div", { "class": "wd-zoom" }, [
      el("button", { "class": "wd-icon", type: "button", title: "Zoom out", "aria-label": "Zoom out", onclick: function () { if (zoom) zoom.zoomBy(0.8); } }, ["−"]),
      el("button", { "class": "wd-icon", type: "button", title: "Zoom in", "aria-label": "Zoom in", onclick: function () { if (zoom) zoom.zoomBy(1.25); } }, ["+"]),
      el("button", { "class": "wd-text-btn", type: "button", title: "Fit what is shown", onclick: function () { if (zoom) zoom.fit(); } }, ["Fit"])
    ])
  ]));
  f.host.classList.add("wd-er");
  f.host.appendChild(drawing);
  f.legend.appendChild(el("div", { "class": "wd-legend" }, [
    el("span", { "class": "wd-key", text: "⚿ key column" }), el("span", { "class": "wd-key", text: "↗ foreign key column" }),
    el("span", { "class": "wd-key", text: "* many · 1 one" }), el("span", { "class": "wd-key" }, [el("i", { "class": "wd-key-line wd-up" }), "Relationship of a selected table"]),
    el("span", { "class": "wd-key" }, [el("i", { "class": "wd-key-more", text: "↑ 3" }), "parents not shown"]),
    el("span", { "class": "wd-key" }, [el("i", { "class": "wd-key-more", text: "↓ 3" }), "children not shown: select to expand"])
  ]));
  if (options.describe) options.describe(f.side);
  if (!g.nodes.size) {
    drawing.appendChild(empty("No declared table is recorded" + (options.item && options.item !== "all" ? " for " + itemName(options.item) : "") + "."));
    return { has: function () { return false; }, select: function () {}, say: function () {}, side: f.side };
  }
  draw();
  return {
    has: function (id) { return g.nodes.has(id); },
    select: function (id, adding) { if (g.nodes.has(id)) select(id, adding); },
    /* A note on the status line until the next drawing. */
    say: function (text) { notice = text; status(win()); },
    side: f.side
  };
}

function clipText(text, n) { text = String(text || ""); return text.length > n ? text.slice(0, n - 1) + "…" : text; }

function sv(tag, attrs, children) {
  var e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  setAttrs(e, attrs);
  (children || []).forEach(function (c) { if (c) e.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
  return e;
}

/* Pan and zoom for an SVG stage, with the same gestures as the graph. */
function panZoom(svg, stage, s, bounds, boxes) {
  function box() { return svg.getBoundingClientRect(); }
  function apply() { stage.setAttribute("transform", "translate(" + s.view.x + " " + s.view.y + ") scale(" + s.view.k + ")"); }
  function fitRect(r) {
    var b = box(), k = Math.min(1.1, Math.max(0.15, Math.min(b.width / r.w, b.height / r.h)));
    s.view = { k: k, x: b.width / 2 - (r.x + r.w / 2) * k, y: b.height / 2 - (r.y + r.h / 2) * k };
    apply();
  }
  function fit() { fitRect(bounds); }
  if (s.view) apply(); else (root.requestAnimationFrame || setTimeout)(fit);
  var drag = null;
  svg.addEventListener("pointerdown", function (e) {
    if (e.target.closest && (e.target.closest(".wd-er-box") || e.target.closest(".wd-er-more"))) return;
    drag = { x: e.clientX, y: e.clientY, v: s.view, touch: e.pointerType === "touch" };
  });
  svg.addEventListener("pointermove", function (e) {
    if (!drag || !s.view) return;
    s.view = { k: drag.v.k, x: drag.v.x + e.clientX - drag.x, y: drag.v.y + (drag.touch ? 0 : e.clientY - drag.y) };
    apply();
  });
  ["pointerup", "pointercancel", "pointerleave"].forEach(function (t) { svg.addEventListener(t, function () { drag = null; }); });
  function zoomAt(f, px, py) {
    var k = Math.min(2.5, Math.max(0.1, s.view.k * f));
    f = k / s.view.k;
    s.view = { k: k, x: px - (px - s.view.x) * f, y: py - (py - s.view.y) * f };
    apply();
  }
  svg.addEventListener("wheel", function (e) {
    if (!(e.ctrlKey || e.metaKey) && pageScrolls()) return;
    e.preventDefault();
    var b = box();
    zoomAt(Math.exp(-e.deltaY * 0.0025), e.clientX - b.left, e.clientY - b.top);
  }, { passive: false });
  return {
    fit: fit,
    zoomBy: function (f) { if (!s.view) return; var b = box(); zoomAt(f, b.width / 2, b.height / 2); },
    fitBoxes: function (ids) {
      var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
      ids.forEach(function (id) { var b = boxes.get(id); if (b) { x0 = Math.min(x0, b.x); y0 = Math.min(y0, b.y); x1 = Math.max(x1, b.x + ER.BOX); y1 = Math.max(y1, b.y + b.h); } });
      if (x0 < Infinity) fitRect({ x: x0 - 200, y: y0 - 160, w: x1 - x0 + 400, h: y1 - y0 + 320 });
    },
    screenOf: function (id) { var b = boxes.get(id); return b && s.view ? { x: b.x * s.view.k + s.view.x, y: b.y * s.view.k + s.view.y } : null; },
    pin: function (id, screen) { var b = boxes.get(id); if (!b || !s.view) return; s.view = { k: s.view.k, x: screen.x - b.x * s.view.k, y: screen.y - b.y * s.view.k }; apply(); }
  };
}
