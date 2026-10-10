/* The Catalogue Dashboard renderer. Runs inside HTML Content (Regular) with the
   page's data in window.__WD_DATA, and under Node for its tests. No libraries. */
(function (root) {
  "use strict";

  var NODE_BUDGET = 150;
  var EDGE_BUDGET = 450;
  var DEPTHS = [0, 1, 2, -1];
  var RECENT_MS = 120000;
  var NODE_W = 200, NODE_H = 30, COL_GAP = 92, ROW_GAP = 10;
  var GROUP_HEAD = 22, GROUP_GAP = 18, GROUP_PAD = 10;

  /* ---- data -------------------------------------------------------------- */

  function time(text) {
    if (!text) return null;
    var t = Date.parse(text);
    return isNaN(t) ? null : t;
  }

  function buildGraph(data) {
    var nodes = new Map(), byKey = new Map();
    (data.nodes || []).forEach(function (r) {
      var n = { id: r[0], label: r[1], kind: r[2], item: r[3], key: r[4], desc: r[5] || "",
        result: null, started: null, completed: null, ms: null, failures: null, stat: null, columns: [] };
      nodes.set(n.id, n);
      byKey.set(n.key, n);
    });
    (data.loads || []).forEach(function (r) {
      var n = byKey.get(r[0]);
      if (n) { n.result = r[1]; n.started = time(r[2]); n.completed = time(r[3]); n.ms = r[4]; }
    });
    (data.tests || []).forEach(function (r) {
      var n = byKey.get(r[0]);
      if (n && !n.result) { n.result = r[1]; n.completed = time(r[2]); n.failures = r[3]; }
    });
    (data.stats || []).forEach(function (r) {
      var n = byKey.get(r[0]);
      if (n) n.stat = { read: r[1], inserted: r[2], updated: r[3], deleted: r[4], rejected: r[5] };
    });
    (data.columns || []).forEach(function (r) {
      var n = byKey.get(r[0]);
      if (n) n.columns.push({ name: r[1], note: r[2] || "" });
    });
    var up = new Map(), down = new Map(), kinds = new Map(), stubs = new Map();
    function add(map, a, b) { var s = map.get(a); if (!s) map.set(a, s = new Set()); s.add(b); }
    (data.edges || []).forEach(function (r) {
      var d = r[0], u = r[1], kind = r[2];
      if (!nodes.has(d)) return;
      if (kind === "External" || !nodes.has(u)) {
        var list = stubs.get(d);
        if (!list) stubs.set(d, list = []);
        list.push(u);
        return;
      }
      add(up, d, u);
      add(down, u, d);
      kinds.set(u + "\u0001" + d, kind);
    });
    return { nodes: nodes, up: up, down: down, kinds: kinds, stubs: stubs,
      at: time(data.at) || Date.now() };
  }

  function tone(result) {
    switch (result) {
      case "Succeeded": case "Skipped": return "ok";
      case "Failed": case "Error": return "bad";
      case "Blocked": case "Rejected": return "warn";
      case "Pending": return "wait";
      default: return "none";
    }
  }

  /* Waiting, Candidate or Done from this node's direct upstreams only. An
     upstream with no status of its own, such as a shortcut, does not count. */
  function readiness(g, id) {
    var n = g.nodes.get(id);
    if (!n || !n.result) return null;
    var t = n.completed, open = 0, fresh = 0, total = 0;
    (g.up.get(id) || new Set()).forEach(function (u) {
      var m = g.nodes.get(u);
      if (!m.result) return;
      total += 1;
      if (m.result === "Pending") open += 1;
      else if (t === null || (m.completed !== null && m.completed > t)) fresh += 1;
    });
    if (n.result !== "Pending" && open === 0 && fresh === 0) return "Done";
    if (open === 0 && fresh === total) return "Candidate";
    return "Waiting";
  }

  /* ---- the window around a focus ---------------------------------------- */

  function windowAround(g, focus, upDepth, downDepth, budget) {
    budget = budget || { nodes: NODE_BUDGET, edges: EDGE_BUDGET };
    if (!g.nodes.has(focus)) return null;
    var dist = new Map([[focus, 0]]);
    function walk(adjacency, sign, depth) {
      var limit = depth < 0 ? Infinity : depth, frontier = [focus];
      for (var step = 1; step <= limit && frontier.length; step++) {
        var next = [];
        frontier.forEach(function (n) {
          (adjacency.get(n) || new Set()).forEach(function (m) {
            if (!dist.has(m)) { dist.set(m, sign * step); next.push(m); }
          });
        });
        frontier = next;
      }
    }
    walk(g.up, -1, upDepth);
    walk(g.down, 1, downDepth);
    var ranked = Array.from(dist, function (e) { return { id: e[0], dist: e[1] }; });
    ranked.sort(function (a, b) {
      return Math.abs(a.dist) - Math.abs(b.dist) || (a.dist > 0) - (b.dist > 0) ||
        compare(g.nodes.get(a.id).label, g.nodes.get(b.id).label) || compare(a.id, b.id);
    });
    var kept = ranked.slice(0, budget.nodes), keep = new Map();
    kept.forEach(function (n) { keep.set(n.id, n.dist); });
    var edges = [];
    keep.forEach(function (_, d) {
      (g.up.get(d) || new Set()).forEach(function (u) {
        if (keep.has(u)) edges.push({ u: u, d: d, kind: g.kinds.get(u + "\u0001" + d) });
      });
    });
    edges.sort(function (a, b) {
      return Math.abs(keep.get(a.u)) + Math.abs(keep.get(a.d)) -
        Math.abs(keep.get(b.u)) - Math.abs(keep.get(b.d)) || compare(a.d, b.d) || compare(a.u, b.u);
    });
    var edgeCount = edges.length;
    edges = edges.slice(0, budget.edges);
    var stubs = [];
    kept.forEach(function (n) {
      (g.stubs.get(n.id) || []).forEach(function (ref) { stubs.push({ d: n.id, ref: ref }); });
    });
    var stubRoom = Math.max(0, budget.edges - edges.length), stubCount = stubs.length;
    stubs = stubs.slice(0, stubRoom);
    var more = {};
    kept.forEach(function (n) {
      var u = 0, d = 0;
      (g.up.get(n.id) || new Set()).forEach(function (m) { if (!keep.has(m)) u += 1; });
      (g.down.get(n.id) || new Set()).forEach(function (m) { if (!keep.has(m)) d += 1; });
      if (u || d) more[n.id] = { up: u, down: d };
    });
    return { focus: focus, nodes: kept, edges: edges, stubs: stubs, more: more, reach: dist.size,
      truncated: dist.size > kept.length || edgeCount > edges.length || stubCount > stubs.length };
  }

  function compare(a, b) { a = String(a).toLowerCase(); b = String(b).toLowerCase(); return a < b ? -1 : a > b ? 1 : 0; }

  /* Columns by signed distance; rows grouped by item and ordered by the
     barycentre of each node's neighbours in the column nearer the focus. */
  function layout(g, win) {
    var columns = new Map(), distOf = new Map();
    win.nodes.forEach(function (n) {
      distOf.set(n.id, n.dist);
      var c = columns.get(n.dist);
      if (!c) columns.set(n.dist, c = []);
      c.push(n.id);
    });
    var row = new Map(), order = Array.from(columns.keys()).sort(function (a, b) {
      return Math.abs(a) - Math.abs(b) || a - b;
    });
    var positions = new Map(), groups = [], minY = Infinity, maxY = -Infinity;
    order.forEach(function (dist) {
      var ids = columns.get(dist), nearer = dist > 0 ? dist - 1 : dist + 1;
      var bary = new Map();
      ids.forEach(function (id) {
        var neighbours = dist === 0 ? [] : Array.from((dist > 0 ? g.up : g.down).get(id) || [])
          .filter(function (m) { return row.has(m) && distOf.get(m) === nearer; })
          .map(function (m) { return row.get(m); });
        bary.set(id, neighbours.length ? neighbours.reduce(function (a, b) { return a + b; }, 0) / neighbours.length : Infinity);
      });
      var byItem = new Map();
      ids.forEach(function (id) {
        var item = g.nodes.get(id).item, list = byItem.get(item);
        if (!byItem.has(item)) byItem.set(item, list = []);
        list.push(id);
      });
      var items = Array.from(byItem.keys()).map(function (item) {
        var list = byItem.get(item), finite = list.map(function (id) { return bary.get(id); }).filter(isFinite);
        return { item: item, ids: list, bary: finite.length ? finite.reduce(function (a, b) { return a + b; }, 0) / finite.length : Infinity };
      });
      items.sort(function (a, b) { return (a.bary - b.bary) || compare(a.item, b.item); });
      var height = 0;
      items.forEach(function (group) {
        group.ids.sort(function (a, b) {
          return (bary.get(a) - bary.get(b)) || compare(g.nodes.get(a).label, g.nodes.get(b).label) || compare(a, b);
        });
        height += GROUP_HEAD + group.ids.length * (NODE_H + ROW_GAP) - ROW_GAP + 2 * GROUP_PAD;
      });
      height += GROUP_GAP * (items.length - 1);
      var y = -height / 2, x = dist * (NODE_W + COL_GAP), index = 0;
      items.forEach(function (group) {
        var top = y;
        y += GROUP_PAD + GROUP_HEAD;
        group.ids.forEach(function (id) {
          positions.set(id, { x: x, y: y });
          row.set(id, index++);
          y += NODE_H + ROW_GAP;
        });
        y += GROUP_PAD - ROW_GAP;
        groups.push({ item: group.item, x: x - GROUP_PAD, y: top, w: NODE_W + 2 * GROUP_PAD, h: y - top });
        minY = Math.min(minY, top);
        maxY = Math.max(maxY, y);
        y += GROUP_GAP;
      });
    });
    var dists = order.length ? order : [0];
    return { positions: positions, groups: groups,
      bounds: { x: Math.min.apply(null, dists) * (NODE_W + COL_GAP) - GROUP_PAD - 60,
        y: minY - 30, w: (Math.max.apply(null, dists) - Math.min.apply(null, dists)) * (NODE_W + COL_GAP) + NODE_W + 2 * GROUP_PAD + 120,
        h: maxY - minY + 60 } };
  }

  function edgePath(a, b) {
    var x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2;
    var c = Math.max(40, Math.abs(x2 - x1) / 2);
    return "M" + r(x1) + " " + r(y1) + "C" + r(x1 + c) + " " + r(y1) + " " + r(x2 - c) + " " + r(y2) + " " + r(x2) + " " + r(y2);
  }
  function r(v) { return Math.round(v * 10) / 10; }

  /* Lineage through the window: everything upstream and downstream of id. */
  function lineage(win, id) {
    var up = new Map(), down = new Map(), seen = new Set([id]), edges = new Set();
    win.edges.forEach(function (e) {
      (up.get(e.d) || up.set(e.d, []).get(e.d)).push(e);
      (down.get(e.u) || down.set(e.u, []).get(e.u)).push(e);
    });
    [[up, "u"], [down, "d"]].forEach(function (pair) {
      var stack = [id], visited = new Set([id]);
      while (stack.length) {
        var n = stack.pop();
        (pair[0].get(n) || []).forEach(function (e) {
          edges.add(e.u + "\u0001" + e.d);
          var m = e[pair[1]];
          if (!visited.has(m)) { visited.add(m); seen.add(m); stack.push(m); }
        });
      }
    });
    return { nodes: seen, edges: edges };
  }

  /* ---- search ------------------------------------------------------------ */

  function search(g, query, limit) {
    var terms = String(query || "").toLowerCase().split(/\s+/).filter(Boolean);
    if (!terms.length) return [];
    var found = [];
    g.nodes.forEach(function (n) {
      var label = n.label.toLowerCase(), item = n.item.toLowerCase(), desc = n.desc.toLowerCase(),
        id = n.id.toLowerCase(), score = 0;
      for (var i = 0; i < terms.length; i++) {
        var t = terms[i], s = 0;
        if (label === t) s = 100;
        else if (label.indexOf(t) === 0) s = 80;
        else if ((" " + label.replace(/[._/\-]/g, " ")).indexOf(" " + t) >= 0) s = 60;
        else if (label.indexOf(t) >= 0) s = 40;
        else if (item.indexOf(t) >= 0) s = 25;
        else if (id.indexOf(t) >= 0) s = 20;
        else if (desc.indexOf(t) >= 0) s = 10;
        if (!s) return;
        score += s;
      }
      found.push({ node: n, score: score });
    });
    found.sort(function (a, b) {
      return b.score - a.score || a.node.label.length - b.node.label.length || compare(a.node.label, b.node.label);
    });
    return found.slice(0, limit || 12).map(function (f) { return f.node; });
  }

  /* ---- runs and findings ------------------------------------------------- */

  function workflows(log) {
    var by = new Map();
    (log || []).forEach(function (r) {
      var w = by.get(r[0]), done = time(r[8]) || time(r[7]) || 0;
      if (!w) by.set(r[0], w = { id: r[0], start: Infinity, end: 0, count: 0 });
      w.start = Math.min(w.start, time(r[7]) || done);
      w.end = Math.max(w.end, done);
      w.count += 1;
    });
    return Array.from(by.values()).sort(function (a, b) { return b.end - a.end; });
  }

  function gantt(log, workflow) {
    var rows = (log || []).filter(function (r) { return r[0] === workflow; }).map(function (r) {
      var start = time(r[7]), end = time(r[8]) || start;
      var label = r[5] ? r[2] + "/" + r[3] + "/" + r[4] + "." + r[5] : (r[9] || r[2] + "/" + r[3]);
      var colon = label.indexOf(":"), verb = "";
      if (!r[5] && colon > 0) { verb = label.slice(0, colon); label = label.slice(colon + 1); }
      return { task: r[1], label: label, verb: verb === "load" ? "" : verb, result: r[6], start: start, end: end };
    }).filter(function (row) { return row.start !== null; });
    rows.sort(function (a, b) { return a.start - b.start || compare(a.label, b.label); });
    var t0 = Infinity, t1 = -Infinity;
    rows.forEach(function (row) { t0 = Math.min(t0, row.start); t1 = Math.max(t1, row.end); });
    return { rows: rows, t0: t0, t1: Math.max(t1, t0 + 1000) };
  }

  function findings(data) {
    var at = time(data.at) || Date.now(), views = new Set(data.views || []), out = [];
    function name(key) {
      var p = String(key).split("|");
      return p[0] + "/" + p[1] + (p[3] ? "/" + p[2] + "." + p[3] : "");
    }
    (data.loads || []).forEach(function (r) {
      var done = time(r[3]), severity = 0, text = "";
      if (r[1] === "Failed") { severity = 1; text = "The load failed"; }
      else if (r[1] === "Error") { severity = 1; text = "The load could not be evaluated"; }
      else if (r[1] === "Blocked") { severity = 2; text = "An upstream failure prevented it"; }
      else if (r[1] === "Rejected") { severity = 2; text = "Completed with rejected rows"; }
      else if (r[1] === "Succeeded" && done !== null && at - done > 86400000 && !views.has(r[0])) {
        severity = 2; text = "No successful load for " + Math.floor((at - done) / 3600000) + " hours";
      } else if (r[1] === "Pending") { severity = 3; text = "Rebuilt and not loaded since"; }
      if (severity) out.push({ severity: severity, object: name(r[0]), what: "Load", result: r[1], text: text, at: done });
    });
    (data.tests || []).forEach(function (r) {
      if (r[1] === "Succeeded") return;
      var severity = r[1] === "Failed" || r[1] === "Error" ? 1 : r[1] === "Blocked" ? 2 : 3;
      var text = r[1] === "Failed" ? fmt(r[3]) + " failing rows" : r[1] === "Pending" ? "Rebuilt and not run since" :
        r[1] === "Error" ? "Could not be evaluated" : r[1];
      out.push({ severity: severity, object: name(r[0]), what: "Test", result: r[1], text: text, at: time(r[2]) });
    });
    out.sort(function (a, b) { return a.severity - b.severity || (b.at || 0) - (a.at || 0) || compare(a.object, b.object); });
    return out;
  }

  function fmt(n) { return n === null || n === undefined ? "" : Number(n).toLocaleString("en-AU"); }

  function duration(ms) {
    if (ms === null || ms === undefined) return "";
    var s = ms / 1000;
    if (s < 60) return (s < 10 ? s.toFixed(1) : Math.round(s)) + " s";
    var m = Math.floor(s / 60);
    if (m < 60) return m + " min " + Math.round(s % 60) + " s";
    return Math.floor(m / 60) + " h " + (m % 60) + " min";
  }

  function stamp(t) {
    if (t === null || t === undefined) return "";
    return new Date(t).toISOString().replace("T", " ").slice(0, 19) + " UTC";
  }

  /* ---- DOM --------------------------------------------------------------- */

  var SVG = "http://www.w3.org/2000/svg";

  function el(tag, attrs, children) {
    var e = document.createElement(tag);
    set(e, attrs);
    (children || []).forEach(function (c) { if (c !== null && c !== undefined) e.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
    return e;
  }
  function sv(tag, attrs, children) {
    var e = document.createElementNS(SVG, tag);
    set(e, attrs);
    (children || []).forEach(function (c) { if (c) e.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
    return e;
  }
  function set(e, attrs) {
    Object.keys(attrs || {}).forEach(function (k) {
      var v = attrs[k];
      if (v === null || v === undefined || v === false) return;
      if (k.slice(0, 2) === "on") e.addEventListener(k.slice(2), v);
      else if (k === "text") e.textContent = v;
      else e.setAttribute(k, v);
    });
  }

  var GLYPHS = {
    "Table": "M1.5 2.5h11v9h-11zM1.5 5.5h11M5.5 5.5v6",
    "View": "M1 7c2.4-4.2 9.6-4.2 12 0c-2.4 4.2-9.6 4.2-12 0zM7 5.4a1.6 1.6 0 1 0 0.01 0z",
    "Folder": "M1.5 3h4l1.5 1.5h5.5v7h-11z",
    "Shortcut": "M3 11l8-8M5.5 3H11v5.5",
    "Test": "M7 1a6 6 0 1 0 0.01 0zM4.2 7.2l2 2 3.8-4.2",
    "Assumption": "M7 1.2l6 11H1zM7 5.2v3.4M7 10v0.6",
    "Semantic model": "M7 1l5.5 2.8v6.4L7 13l-5.5-2.8V3.8zM1.5 3.8L7 6.6l5.5-2.8M7 6.6V13",
    "Report": "M2.5 12V7.5M7 12V2.5M11.5 12V5M1 12.5h12"
  };

  function state() {
    root.__WD_STATE = root.__WD_STATE || {};
    return root.__WD_STATE;
  }

  function themeOf(mount) {
    var s = state();
    if (!s.theme) {
      var dark = root.matchMedia && root.matchMedia("(prefers-color-scheme: dark)").matches;
      s.theme = dark ? "dark" : "light";
    }
    mount.setAttribute("data-theme", s.theme);
  }

  function themeButton(mount) {
    return el("button", { "class": "wd-icon", title: "Switch light and dark", "aria-label": "Switch light and dark",
      onclick: function () { var s = state(); s.theme = s.theme === "dark" ? "light" : "dark"; mount.setAttribute("data-theme", s.theme); } },
      [el("span", { "aria-hidden": "true", text: "◐" })]);
  }

  function chip(result, label) {
    return el("span", { "class": "wd-chip wd-" + tone(result) }, [label || result || "No status"]);
  }

  /* ---- the graph view ---------------------------------------------------- */

  function GraphView(container, g, view, options) {
    var self = this;
    this.g = g;
    this.view = view;
    this.options = options || {};
    this.positions = new Map();
    this.svg = sv("svg", { "class": "wd-svg", role: "img", "aria-label": "Lineage graph" });
    this.svg.appendChild(sv("defs", {}, [
      sv("marker", { id: "wd-arrow", viewBox: "0 0 8 8", refX: "7.4", refY: "4", markerWidth: "7", markerHeight: "7", orient: "auto" },
        [sv("path", { d: "M0.5 0.8L7.5 4L0.5 7.2z", "class": "wd-arrowhead" })]),
      sv("marker", { id: "wd-arrow-hl", viewBox: "0 0 8 8", refX: "7.4", refY: "4", markerWidth: "7", markerHeight: "7", orient: "auto" },
        [sv("path", { d: "M0.5 0.8L7.5 4L0.5 7.2z", "class": "wd-arrowhead wd-hl" })])
    ]));
    this.stage = sv("g", { "class": "wd-stage" });
    this.svg.appendChild(this.stage);
    this.banner = el("div", { "class": "wd-banner", hidden: "hidden" });
    this.legend = el("div", { "class": "wd-legend" });
    container.appendChild(this.svg);
    container.appendChild(this.banner);
    container.appendChild(this.legend);
    this.container = container;
    this.bindPanZoom();
    this.drawLegend();
    root.addEventListener("resize", function () { self.applyView(); });
  }

  GraphView.prototype.drawLegend = function () {
    var readinessMode = this.options.colour === "readiness";
    var entries = readinessMode
      ? [["ok", "Done"], ["next", "Candidate"], ["wait", "Waiting"], ["none", "Not loaded"]]
      : [["ok", "Succeeded"], ["bad", "Failed"], ["warn", "Blocked or rejected"], ["wait", "Pending"], ["none", "No status"]];
    this.legend.textContent = "";
    var self = this;
    entries.forEach(function (e) {
      self.legend.appendChild(el("span", { "class": "wd-key" }, [el("i", { "class": "wd-swatch wd-" + e[0] }), e[1]]));
    });
    this.legend.appendChild(el("span", { "class": "wd-key" }, [el("i", { "class": "wd-more-key", text: "+3" }), "more beyond the window"]));
  };

  GraphView.prototype.bindPanZoom = function () {
    var self = this, drag = null;
    this.svg.addEventListener("pointerdown", function (e) {
      if (e.target.closest && e.target.closest(".wd-node")) return;
      drag = { x: e.clientX, y: e.clientY, vx: self.view.x, vy: self.view.y };
      self.svg.setPointerCapture(e.pointerId);
      self.svg.classList.add("wd-dragging");
    });
    this.svg.addEventListener("pointermove", function (e) {
      if (!drag) return;
      self.view.x = drag.vx + e.clientX - drag.x;
      self.view.y = drag.vy + e.clientY - drag.y;
      self.applyView();
    });
    function end() { drag = null; self.svg.classList.remove("wd-dragging"); }
    this.svg.addEventListener("pointerup", end);
    this.svg.addEventListener("pointercancel", end);
    this.svg.addEventListener("wheel", function (e) {
      e.preventDefault();
      var box = self.svg.getBoundingClientRect();
      self.zoomAt(Math.exp(-e.deltaY * 0.0015), e.clientX - box.left, e.clientY - box.top);
    }, { passive: false });
  };

  GraphView.prototype.zoomAt = function (factor, px, py) {
    var k = Math.min(2.5, Math.max(0.15, this.view.k * factor)), f = k / this.view.k;
    this.view.x = px - (px - this.view.x) * f;
    this.view.y = py - (py - this.view.y) * f;
    this.view.k = k;
    this.applyView();
  };

  GraphView.prototype.zoomBy = function (factor) {
    var box = this.svg.getBoundingClientRect();
    this.zoomAt(factor, box.width / 2, box.height / 2);
  };

  GraphView.prototype.applyView = function () {
    this.stage.setAttribute("transform", "translate(" + r(this.view.x) + " " + r(this.view.y) + ") scale(" + r(this.view.k * 1000) / 1000 + ")");
  };

  GraphView.prototype.fit = function () {
    if (!this.layout) return;
    var box = this.svg.getBoundingClientRect(), b = this.layout.bounds;
    var k = Math.min(1.25, Math.max(0.15, Math.min(box.width / b.w, box.height / b.h)));
    this.view.k = k;
    this.view.x = box.width / 2 - (b.x + b.w / 2) * k;
    this.view.y = box.height / 2 - (b.y + b.h / 2) * k;
    this.applyView();
  };

  GraphView.prototype.centre = function (id) {
    var p = this.layout && this.layout.positions.get(id);
    if (!p) return null;
    var box = this.svg.getBoundingClientRect();
    return { x: box.width / 2 - (p.x + NODE_W / 2) * this.view.k, y: box.height / 2 - (p.y + NODE_H / 2) * this.view.k };
  };

  GraphView.prototype.show = function (focus, upDepth, downDepth, animate) {
    var g = this.g, win = windowAround(g, focus, upDepth, downDepth);
    this.win = win;
    if (!win) return;
    var lay = layout(g, win), before = this.positions, origin = before.get(focus);
    this.layout = lay;
    var stage = this.stage, self = this;
    stage.textContent = "";
    var groupsLayer = sv("g", { "class": "wd-groups" }), edgeLayer = sv("g", { "class": "wd-edges" }),
      nodeLayer = sv("g", { "class": "wd-nodes" });
    stage.appendChild(groupsLayer); stage.appendChild(edgeLayer); stage.appendChild(nodeLayer);
    lay.groups.forEach(function (box) {
      groupsLayer.appendChild(sv("g", { "class": "wd-group" }, [
        sv("rect", { x: box.x, y: box.y, width: box.w, height: box.h, rx: 12 }),
        sv("text", { x: box.x + 12, y: box.y + 17 }, [box.item])
      ]));
    });
    var edgeEls = win.edges.map(function (e) {
      var path = sv("path", { "class": "wd-edge wd-edge-" + String(e.kind || "Dependency").toLowerCase(), "marker-end": "url(#wd-arrow)" });
      edgeLayer.appendChild(path);
      return { e: e, el: path };
    });
    var stubEls = win.stubs.map(function (s, i) {
      var p = lay.positions.get(s.d), title = sv("title", {}, [s.ref]);
      var line = sv("g", { "class": "wd-stub" }, [title, sv("path", { "marker-end": "url(#wd-arrow)" }), sv("circle", { r: 3 })]);
      edgeLayer.appendChild(line);
      return { s: s, el: line, k: i };
    });
    var stubIndex = new Map();
    stubEls.forEach(function (s) { var n = (stubIndex.get(s.s.d) || 0); s.k = n; stubIndex.set(s.s.d, n + 1); });
    var nodeEls = win.nodes.map(function (n) {
      var node = g.nodes.get(n.id), mode = self.options.colour === "readiness";
      var state = mode ? readiness(g, n.id) : node.result;
      var t = mode ? ({ Done: "ok", Candidate: "next", Waiting: "wait" }[state] || "none") : tone(node.result);
      var recent = node.completed !== null && g.at - node.completed <= RECENT_MS && node.result !== "Pending";
      var label = node.label.length > 27 ? node.label.slice(0, 26) + "…" : node.label;
      var parts = [
        sv("title", {}, [node.id + " · " + node.kind + " · " + (state || (mode ? "no readiness" : "no status")) +
          (node.completed ? " · " + stamp(node.completed) : "")]),
        recent ? sv("rect", { "class": "wd-pulse", x: -3, y: -3, width: NODE_W + 6, height: NODE_H + 6, rx: 10 }) : null,
        n.id === focus ? sv("rect", { "class": "wd-halo", x: -5, y: -5, width: NODE_W + 10, height: NODE_H + 10, rx: 11 }) : null,
        sv("rect", { "class": "wd-box", width: NODE_W, height: NODE_H, rx: node.kind === "Test" || node.kind === "Assumption" ? 15 : 8 }),
        sv("path", { "class": "wd-glyph", d: GLYPHS[node.kind] || "M7 2a5 5 0 1 0 0.01 0z", transform: "translate(10 8)" }),
        sv("text", { x: 32, y: 19.5 }, [label])
      ];
      var more = win.more[n.id];
      if (more && more.up) parts.push(sv("g", { "class": "wd-more", transform: "translate(-12 -9)" },
        [sv("title", {}, [more.up + " more upstream"]), sv("rect", { width: 26, height: 16, rx: 8 }), sv("text", { x: 13, y: 11.5 }, ["+" + more.up])]));
      if (more && more.down) parts.push(sv("g", { "class": "wd-more", transform: "translate(" + (NODE_W - 14) + " -9)" },
        [sv("title", {}, [more.down + " more downstream"]), sv("rect", { width: 26, height: 16, rx: 8 }), sv("text", { x: 13, y: 11.5 }, ["+" + more.down])]));
      var group = sv("g", { "class": "wd-node wd-" + t + (n.id === focus ? " wd-focus" : "") + (recent ? " wd-recent" : ""),
        tabindex: "0", role: "button", "aria-label": node.label + ", " + node.kind + ", " + (state || "no status") }, parts);
      group.addEventListener("click", function () { if (self.options.onPick) self.options.onPick(n.id); });
      group.addEventListener("keydown", function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); if (self.options.onPick) self.options.onPick(n.id); } });
      group.addEventListener("pointerenter", function () { self.highlight(n.id); });
      group.addEventListener("pointerleave", function () { self.highlight(null); });
      group.addEventListener("focus", function () { self.highlight(n.id); });
      group.addEventListener("blur", function () { self.highlight(null); });
      nodeLayer.appendChild(group);
      return { id: n.id, el: group };
    });
    this.nodeEls = nodeEls;
    this.edgeEls = edgeEls;

    var current = new Map();
    function place(t) {
      nodeEls.forEach(function (n) {
        var to = lay.positions.get(n.id), from = before.get(n.id) || origin || to;
        var p = { x: from.x + (to.x - from.x) * t, y: from.y + (to.y - from.y) * t };
        current.set(n.id, p);
        n.el.setAttribute("transform", "translate(" + r(p.x) + " " + r(p.y) + ")");
        if (!before.has(n.id)) n.el.style.opacity = String(t);
      });
      edgeEls.forEach(function (e) { e.el.setAttribute("d", edgePath(current.get(e.e.u), current.get(e.e.d))); });
      stubEls.forEach(function (s) {
        var p = current.get(s.s.d), y = p.y + NODE_H / 2 + Math.min(s.k, 3) * 5 - (s.k ? 2 : 0);
        s.el.childNodes[1].setAttribute("d", "M" + r(p.x - 46) + " " + r(y) + "L" + r(p.x - 2) + " " + r(y));
        s.el.childNodes[2].setAttribute("cx", r(p.x - 49));
        s.el.childNodes[2].setAttribute("cy", r(y));
      });
    }
    this.positions = lay.positions;
    var target = this.centre(focus), from = { x: this.view.x, y: this.view.y };
    if (!animate || !before.size || !root.requestAnimationFrame) {
      place(1);
      if (animate && target) { this.view.x = target.x; this.view.y = target.y; this.applyView(); }
      return;
    }
    var started = null;
    groupsLayer.style.opacity = "0";
    function frame(now) {
      if (started === null) started = now;
      var t = Math.min(1, (now - started) / 420), e = t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
      place(e);
      if (target) { self.view.x = from.x + (target.x - from.x) * e; self.view.y = from.y + (target.y - from.y) * e; self.applyView(); }
      groupsLayer.style.opacity = String(e);
      if (t < 1) root.requestAnimationFrame(frame);
    }
    root.requestAnimationFrame(frame);
  };

  GraphView.prototype.highlight = function (id) {
    var path = id ? lineage(this.win, id) : null;
    this.svg.classList.toggle("wd-tracing", !!path);
    this.nodeEls.forEach(function (n) { n.el.classList.toggle("wd-on", !!path && path.nodes.has(n.id)); });
    this.edgeEls.forEach(function (e) {
      var on = !!path && path.edges.has(e.e.u + "\u0001" + e.e.d);
      e.el.classList.toggle("wd-on", on);
      e.el.setAttribute("marker-end", on ? "url(#wd-arrow-hl)" : "url(#wd-arrow)");
    });
  };

  GraphView.prototype.showBanner = function () {
    var w = this.win;
    if (w && w.truncated) {
      this.banner.hidden = false;
      this.banner.textContent = "Showing the " + w.nodes.length + " of " + w.reach + " nodes nearest the focus, within " +
        NODE_BUDGET + " nodes and " + EDGE_BUDGET + " edges. Lower a depth to see the rest.";
    } else this.banner.hidden = true;
  };

  /* ---- controls shared by Explore and Live run ---------------------------- */

  function depthControl(label, value, onChange) {
    var group = el("div", { "class": "wd-seg", role: "radiogroup", "aria-label": label + " depth" }, [el("span", { "class": "wd-seg-label", text: label })]);
    DEPTHS.forEach(function (d) {
      group.appendChild(el("button", { "class": "wd-seg-btn", role: "radio", "aria-checked": String(d === value),
        onclick: function () {
          Array.prototype.forEach.call(group.querySelectorAll("button"), function (b) { b.setAttribute("aria-checked", "false"); });
          this.setAttribute("aria-checked", "true");
          onChange(d);
        } }, [d < 0 ? "Max" : String(d)]));
    });
    return group;
  }

  function searchBox(g, onPick) {
    var input = el("input", { "class": "wd-search", type: "search", placeholder: "Search objects, items or descriptions", "aria-label": "Search", autocomplete: "off", spellcheck: "false" });
    var list = el("ul", { "class": "wd-results", role: "listbox", hidden: "hidden" }), results = [], active = 0;
    function render() {
      list.textContent = "";
      list.hidden = !results.length;
      results.forEach(function (n, i) {
        list.appendChild(el("li", { role: "option", "aria-selected": String(i === active), "class": "wd-result",
          onmousedown: function (e) { e.preventDefault(); choose(n); } },
          [el("span", { "class": "wd-result-label", text: n.label }), el("span", { "class": "wd-result-meta", text: n.kind + " · " + n.item }),
            n.desc ? el("span", { "class": "wd-result-desc", text: n.desc }) : null]));
      });
    }
    function choose(n) { input.value = ""; results = []; render(); input.blur(); onPick(n.id); }
    input.addEventListener("input", function () { results = search(g, input.value, 10); active = 0; render(); });
    input.addEventListener("keydown", function (e) {
      if (e.key === "ArrowDown") { active = Math.min(results.length - 1, active + 1); render(); e.preventDefault(); }
      else if (e.key === "ArrowUp") { active = Math.max(0, active - 1); render(); e.preventDefault(); }
      else if (e.key === "Enter" && results[active]) choose(results[active]);
      else if (e.key === "Escape") { input.value = ""; results = []; render(); }
    });
    input.addEventListener("blur", function () { setTimeout(function () { list.hidden = true; }, 120); });
    input.addEventListener("focus", function () { if (results.length) list.hidden = false; });
    return el("div", { "class": "wd-searchbox" }, [input, list]);
  }

  function defaultFocus(g) {
    var best = null;
    g.nodes.forEach(function (n) { if (n.completed !== null && (!best || n.completed > best.completed)) best = n; });
    if (best) return best.id;
    var first = null;
    g.nodes.forEach(function (n) { if (!first || compare(n.id, first) < 0) first = n.id; });
    return first;
  }

  function graphPanel(mount, g, s, options) {
    if (!s.focus || !g.nodes.has(s.focus)) s.focus = defaultFocus(g);
    if (s.up === undefined) s.up = 2;
    if (s.down === undefined) s.down = 2;
    var restored = !!s.view;
    s.view = s.view || { x: 0, y: 0, k: 1 };
    var canvas = el("div", { "class": "wd-canvas" }), view;
    function pick(id) {
      s.focus = id;
      view.show(id, s.up, s.down, true);
      view.showBanner();
      if (options.onFocus) options.onFocus(id);
    }
    var bar = el("div", { "class": "wd-bar" }, [
      searchBox(g, pick),
      depthControl("Up", s.up, function (d) { s.up = d; view.show(s.focus, s.up, s.down, true); view.showBanner(); }),
      depthControl("Down", s.down, function (d) { s.down = d; view.show(s.focus, s.up, s.down, true); view.showBanner(); }),
      el("span", { "class": "wd-grow" }),
      el("div", { "class": "wd-zoom" }, [
        el("button", { "class": "wd-icon", title: "Zoom out", "aria-label": "Zoom out", onclick: function () { view.zoomBy(1 / 1.25); } }, ["−"]),
        el("button", { "class": "wd-icon", title: "Zoom in", "aria-label": "Zoom in", onclick: function () { view.zoomBy(1.25); } }, ["+"]),
        el("button", { "class": "wd-text-btn", title: "Fit the window to the screen", onclick: function () { view.fit(); } }, ["Fit"])
      ]),
      options.extra || null
    ]);
    mount.appendChild(bar);
    mount.appendChild(canvas);
    view = new GraphView(canvas, g, s.view, { colour: options.colour, onPick: pick });
    if (!g.nodes.size) {
      canvas.appendChild(el("div", { "class": "wd-empty", text: "The catalogue records no installed objects yet. Build a project to see its graph." }));
      return null;
    }
    view.show(s.focus, s.up, s.down, false);
    view.showBanner();
    if (restored) view.applyView();
    else root.requestAnimationFrame ? root.requestAnimationFrame(function () { view.fit(); }) : view.fit();
    if (options.onFocus) options.onFocus(s.focus);
    return view;
  }

  /* ---- pages --------------------------------------------------------------- */

  function details(panel, g, id) {
    var n = g.nodes.get(id);
    panel.textContent = "";
    if (!n) return;
    var ups = (g.up.get(id) || new Set()).size, downs = (g.down.get(id) || new Set()).size;
    panel.appendChild(el("div", { "class": "wd-detail-head" }, [
      el("div", { "class": "wd-kind", text: n.kind }),
      el("h2", { text: n.label }),
      el("div", { "class": "wd-muted", text: n.item + " · " + ups + " upstream · " + downs + " downstream" })
    ]));
    panel.appendChild(el("p", { "class": n.desc ? "wd-desc" : "wd-desc wd-muted", text: n.desc || "No description is declared." }));
    var isTest = n.kind === "Test" || n.kind === "Assumption";
    panel.appendChild(el("h3", { text: isTest ? "Last run" : "Last load" }));
    if (n.result) {
      panel.appendChild(el("div", { "class": "wd-line" }, [chip(n.result), el("span", { "class": "wd-muted", text: " " + stamp(n.completed) + (n.ms ? " · " + duration(n.ms) : "") })]));
      if (n.stat) panel.appendChild(el("div", { "class": "wd-stats" }, [["read", n.stat.read], ["inserted", n.stat.inserted], ["updated", n.stat.updated], ["deleted", n.stat.deleted], ["rejected", n.stat.rejected]]
        .map(function (p) { return el("div", { "class": "wd-stat" }, [el("b", { text: fmt(p[1]) }), el("span", { text: p[0] })]); })));
      if (isTest && n.result === "Failed") panel.appendChild(el("div", { "class": "wd-muted", text: fmt(n.failures) + " failing rows" }));
    } else panel.appendChild(el("div", { "class": "wd-muted", text: "Nothing has settled for it." }));
    var tests = [];
    (g.down.get(id) || new Set()).forEach(function (d) { if (g.kinds.get(id + "\u0001" + d) === "Validation") tests.push(g.nodes.get(d)); });
    if (tests.length) {
      panel.appendChild(el("h3", { text: "Tests" }));
      tests.sort(function (a, b) { return compare(a.label, b.label); });
      panel.appendChild(el("ul", { "class": "wd-tests" }, tests.map(function (t) {
        return el("li", {}, [el("span", { text: t.label }), el("span", { "class": "wd-muted", text: " " + t.kind }),
          el("span", { "class": "wd-grow" }), chip(t.result), t.result === "Failed" ? el("span", { "class": "wd-muted", text: " " + fmt(t.failures) }) : null]);
      })));
    }
    if (n.columns.length) {
      panel.appendChild(el("h3", { text: "Columns" }));
      n.columns.sort(function (a, b) { return compare(a.name, b.name); });
      panel.appendChild(el("dl", { "class": "wd-columns" }, n.columns.reduce(function (all, c) {
        return all.concat([el("dt", { text: c.name }), el("dd", { text: c.note })]);
      }, [])));
    }
  }

  function explore(mount, data) {
    var g = buildGraph(data), s = state().explore = state().explore || {};
    var layoutEl = el("div", { "class": "wd-split" }), main = el("div", { "class": "wd-main" }), panel = el("aside", { "class": "wd-panel" });
    layoutEl.appendChild(main);
    layoutEl.appendChild(panel);
    mount.appendChild(layoutEl);
    graphPanel(main, g, s, { colour: "status", extra: themeButton(mount), onFocus: function (id) { details(panel, g, id); } });
  }

  function overview(mount, data) {
    var h = data.health || {}, items = [
      ["Red tests", h.red, "latest result is not Succeeded", h.red ? "bad" : "ok"],
      ["Failed loads", h.failed, "Failed, Error or Blocked", h.failed ? "bad" : "ok"],
      ["Pending loads", h.pending, "rebuilt and not loaded since", h.pending ? "wait" : "ok"],
      ["Stale loads", h.stale, "no success in over 24 hours", h.stale ? "warn" : "ok"]
    ];
    var latest = time(h.latest);
    mount.appendChild(el("div", { "class": "wd-head" }, [el("h1", { text: "Catalogue health" }), el("span", { "class": "wd-grow" }), themeButton(mount)]));
    var tiles = el("div", { "class": "wd-tiles" }, items.map(function (t) {
      return el("div", { "class": "wd-tile wd-" + t[3] }, [el("div", { "class": "wd-tile-label", text: t[0] }),
        el("div", { "class": "wd-tile-value", text: fmt(t[1] || 0) }), el("div", { "class": "wd-tile-note", text: t[2] })]);
    }));
    tiles.appendChild(el("div", { "class": "wd-tile wd-none" }, [el("div", { "class": "wd-tile-label", text: "Latest settled" }),
      el("div", { "class": "wd-tile-value", text: latest ? stamp(latest).slice(11, 19) : "none" }),
      el("div", { "class": "wd-tile-note", text: latest ? stamp(latest).slice(0, 10) + " UTC" : "no load or test has settled" })]));
    mount.appendChild(tiles);
    var list = findings(data);
    mount.appendChild(el("h3", { "class": "wd-section", text: "Findings" }));
    if (!list.length) {
      mount.appendChild(el("div", { "class": "wd-allclear", text: "No findings. Every load and test settled successfully within the last 24 hours." }));
    } else {
      var shown = list.slice(0, 200);
      mount.appendChild(el("div", { "class": "wd-table-wrap" }, [el("table", { "class": "wd-table" }, [
        el("thead", {}, [el("tr", {}, ["Severity", "Object", "What", "Finding", "Settled"].map(function (t) { return el("th", { text: t }); }))]),
        el("tbody", {}, shown.map(function (f) {
          var sev = ["", "Error", "Warning", "Info"][f.severity];
          return el("tr", {}, [el("td", {}, [el("span", { "class": "wd-chip wd-" + ["", "bad", "warn", "none"][f.severity], text: sev })]),
            el("td", { "class": "wd-mono", text: f.object }), el("td", { text: f.what }),
            el("td", {}, [chip(f.result), " " + f.text]), el("td", { "class": "wd-muted", text: stamp(f.at) })]);
        }))
      ])]));
      if (list.length > shown.length) mount.appendChild(el("div", { "class": "wd-muted", text: "Showing 200 of " + list.length + " findings, most severe first." }));
    }
    mount.appendChild(observed(data));
  }

  function observed(data) {
    return el("div", { "class": "wd-observed", text: "Observed " + stamp(time(data.at)) +
      ". The page refreshes itself every 30 seconds, or at the capacity's minimum interval when that is longer: 5 minutes unless a capacity admin lowers it." });
  }

  function live(mount, data) {
    var g = buildGraph(data), s = state().live = state().live || {}, flows = workflows(data.log);
    if (!s.workflow || !flows.some(function (w) { return w.id === s.workflow; }) || s.followLatest !== false) s.workflow = flows.length ? flows[0].id : null;
    var select = el("select", { "class": "wd-select", "aria-label": "Workflow", onchange: function () {
      s.workflow = select.value; s.followLatest = select.selectedIndex === 0; s.gantt = null; drawGantt();
    } }, flows.map(function (w, i) {
      return el("option", { value: w.id, selected: w.id === s.workflow ? "selected" : null, text: (i === 0 ? "Latest · " : "") + stamp(w.start).slice(0, 16) + " · " + w.count + " steps · " + w.id.slice(0, 8) });
    }));
    var summary = el("span", { "class": "wd-muted" });
    mount.appendChild(el("div", { "class": "wd-head" }, [el("h1", { text: "Live run" }), select, summary, el("span", { "class": "wd-grow" }), themeButton(mount)]));
    var ganttEl = el("div", { "class": "wd-gantt" }), tip = el("div", { "class": "wd-tip", hidden: "hidden" });
    var graphEl = el("div", { "class": "wd-live-graph" });
    mount.appendChild(ganttEl);
    mount.appendChild(graphEl);
    mount.appendChild(observed(data));

    function drawGantt() {
      ganttEl.textContent = "";
      ganttEl.appendChild(tip);
      if (!s.workflow) { ganttEl.appendChild(el("div", { "class": "wd-empty", text: "No workflow has run yet. Run weaver load to see its timeline." })); return; }
      var model = gantt(data.log, s.workflow), rows = model.rows, span = model.t1 - model.t0;
      var ok = rows.filter(function (x) { return tone(x.result) === "ok"; }).length;
      summary.textContent = stamp(model.t0) + " · " + duration(span) + " · " + rows.length + " steps · " + ok + " succeeded, " + (rows.length - ok) + " not";
      s.gantt = s.gantt || { a: 0, b: 1 };
      var labelW = 280, rowH = 22, top = 26, svg = sv("svg", { "class": "wd-gantt-svg" });
      ganttEl.appendChild(svg);
      function draw() {
        var width = Math.max(300, ganttEl.clientWidth - 8), plot = width - labelW - 20, height = top + rows.length * rowH + 8;
        svg.setAttribute("width", width); svg.setAttribute("height", height);
        svg.textContent = "";
        var a = model.t0 + span * s.gantt.a, b = model.t0 + span * s.gantt.b;
        function x(t) { return labelW + (t - a) / (b - a) * plot; }
        for (var i = 0; i <= 5; i++) {
          var t = a + (b - a) * i / 5, gx = x(t);
          svg.appendChild(sv("line", { "class": "wd-grid", x1: r(gx), x2: r(gx), y1: top - 6, y2: height }));
          svg.appendChild(sv("text", { "class": "wd-axis", x: r(gx), y: 14, "text-anchor": "middle" }, ["+" + duration(t - model.t0)]));
        }
        var clip = sv("clipPath", { id: "wd-plot" }, [sv("rect", { x: labelW, y: 0, width: plot + 4, height: height })]);
        svg.appendChild(sv("defs", {}, [clip]));
        var bars = sv("g", { "clip-path": "url(#wd-plot)" });
        rows.forEach(function (row, i) {
          var y = top + i * rowH, text = row.label + (row.verb ? " (" + row.verb + ")" : "");
          svg.appendChild(sv("text", { "class": "wd-gantt-label", x: labelW - 12, y: y + 15, "text-anchor": "end" },
            [text.length > 40 ? "…" + text.slice(-39) : text]));
          var bar = sv("rect", { "class": "wd-bar-" + tone(row.result), x: r(x(row.start)), y: y + 4, width: r(Math.max(3, x(row.end) - x(row.start))), height: rowH - 8, rx: 4 });
          bar.addEventListener("pointermove", function (e) {
            tip.hidden = false;
            tip.textContent = "";
            tip.appendChild(el("b", { text: row.label }));
            tip.appendChild(el("div", {}, [chip(row.result), " " + row.task + (row.verb ? " · " + row.verb : "") + " · " + duration(row.end - row.start)]));
            tip.appendChild(el("div", { "class": "wd-muted", text: stamp(row.start) + " → " + stamp(row.end).slice(11) }));
            var box = ganttEl.getBoundingClientRect();
            tip.style.left = Math.min(box.width - 260, e.clientX - box.left + 14) + "px";
            tip.style.top = (e.clientY - box.top + 14 + ganttEl.scrollTop) + "px";
          });
          bar.addEventListener("pointerleave", function () { tip.hidden = true; });
          bars.appendChild(bar);
        });
        svg.appendChild(bars);
      }
      draw();
      svg.addEventListener("wheel", function (e) {
        e.preventDefault();
        var box = svg.getBoundingClientRect(), plot = box.width - labelW - 20, f = Math.min(1, Math.max(0, (e.clientX - box.left - labelW) / plot));
        var a = s.gantt.a, b = s.gantt.b, at = a + (b - a) * f, k = Math.exp(e.deltaY * 0.0015), w = Math.min(1, Math.max(0.002, (b - a) * k));
        s.gantt.a = Math.max(0, Math.min(1 - w, at - w * f));
        s.gantt.b = s.gantt.a + w;
        draw();
      }, { passive: false });
      var drag = null;
      svg.addEventListener("pointerdown", function (e) { drag = { x: e.clientX, a: s.gantt.a, b: s.gantt.b }; svg.setPointerCapture(e.pointerId); });
      svg.addEventListener("pointermove", function (e) {
        if (!drag) return;
        var plot = svg.getBoundingClientRect().width - labelW - 20, w = drag.b - drag.a, d = -(e.clientX - drag.x) / plot * w;
        s.gantt.a = Math.max(0, Math.min(1 - w, drag.a + d));
        s.gantt.b = s.gantt.a + w;
        draw();
      });
      svg.addEventListener("pointerup", function () { drag = null; });
      svg.addEventListener("dblclick", function () { s.gantt = { a: 0, b: 1 }; draw(); });
    }
    drawGantt();
    s.graph = s.graph || {};
    graphPanel(graphEl, g, s.graph, { colour: "readiness" });
  }

  function start(data) {
    if (typeof document === "undefined" || !data) return;
    var mount = document.getElementById("wd-app");
    if (!mount) return;
    mount.textContent = "";
    themeOf(mount);
    mount.className = "wd wd-page-" + data.page;
    try {
      ({ overview: overview, explore: explore, live: live }[data.page] || overview)(mount, data);
    } catch (error) {
      mount.appendChild(el("pre", { "class": "wd-error", text: "The dashboard could not render: " + error.message }));
    }
  }

  var api = { buildGraph: buildGraph, windowAround: windowAround, layout: layout, readiness: readiness, search: search,
    lineage: lineage, gantt: gantt, workflows: workflows, findings: findings, tone: tone, edgePath: edgePath,
    NODE_BUDGET: NODE_BUDGET, EDGE_BUDGET: EDGE_BUDGET, start: start };
  if (typeof module === "object" && module.exports) module.exports = api;
  else { root.WeaverDashboard = api; start(root.__WD_DATA); }
})(typeof window !== "undefined" ? window : this);
