// Lineage and Load: the estate graph, focused on one object at a time.

var DEPTHS = [0, 1, 2, -1];

/* One graph with its toolbar, status line and details. Lineage colours nodes
   by kind; Load colours them by their last outcome and rings derived
   readiness. */
function graphWorkspace(body, m, options) {
  var s = pageState(options.page), lay = layoutEstate(m);
  var handed = takeHandOff();
  if (handed && m.nodes.has(handed)) { s.focus = handed; s.revealed = []; s.refit = true; }
  if (s.focus && !m.nodes.has(s.focus)) s.focus = null;
  if (s.up === undefined) s.up = 1;
  if (s.down === undefined) s.down = 1;
  s.revealed = (s.revealed || []).filter(function (k) { return m.nodes.has(k); });
  var health = options.health ? assessHealth(m) : null;

  var host = el("div", { "class": "wd-graph" }), side = el("aside", { "class": "wd-side" }), line = el("div", { "class": "wd-status-line" });
  var view;
  function select(key) {
    s.focus = key; s.revealed = [];
    apply(true);
    if (key && options.onFocus) options.onFocus(key);
  }
  function reveal(marker) {
    s.revealed = s.revealed.concat(revealFrom(m, marker.keys).filter(function (k) { return s.revealed.indexOf(k) < 0; }));
    apply(false);
  }
  function apply(refit) {
    var win = s.focus ? focusWindow(m, s.focus, s.up, s.down, s.revealed) : null;
    view.setWindow(win);
    status(win);
    details(side, m, s.focus, { onOpen: select, health: health, emptyText: options.empty });
    if (refit) { if (win) view.fitWindow(true); else view.fitAll(true); }
    remember();
  }
  function status(win) {
    line.textContent = "";
    if (!win) {
      line.appendChild(document.createTextNode(fmt(m.nodes.size) + " objects in " + fmt(lay.bands.length) + " items. Search or select an object to follow its lineage."));
      return;
    }
    var parts = [win.lineage.size + " in lineage", win.context.size + " around it"];
    if (win.truncated) parts.push(el("b", { "class": "wd-warn-text", text: "lineage cut to the " + win.lineage.size + " nearest of " + fmt(win.reach) + "; lower a depth or follow a +N marker" }));
    if (win.edgesDropped) parts.push(el("b", { "class": "wd-warn-text", text: win.edgesDropped + " edges not drawn" }));
    parts.forEach(function (p, i) { if (i) line.appendChild(document.createTextNode(" · ")); line.appendChild(typeof p === "string" ? document.createTextNode(p) : p); });
  }
  var bar = el("div", { "class": "wd-toolbar" }, [
    nodeSearch(m, select),
    segmented("Up", DEPTHS, s.up, function (d) { s.up = d; s.revealed = []; apply(true); }, depthLabel),
    segmented("Down", DEPTHS, s.down, function (d) { s.down = d; s.revealed = []; apply(true); }, depthLabel),
    el("span", { "class": "wd-grow" }),
    el("div", { "class": "wd-zoom" }, [
      el("button", { "class": "wd-icon", type: "button", title: "Zoom out", "aria-label": "Zoom out", onclick: function () { view.zoomAt(0.8, view.width / 2, view.height / 2); } }, ["−"]),
      el("button", { "class": "wd-icon", type: "button", title: "Zoom in", "aria-label": "Zoom in", onclick: function () { view.zoomAt(1.25, view.width / 2, view.height / 2); } }, ["+"]),
      el("button", { "class": "wd-text-btn", type: "button", title: "Fit the focus, or the estate", onclick: function () { view.fitWindow(true); } }, ["Fit"]),
      el("button", { "class": "wd-text-btn", type: "button", title: "Show the whole estate", onclick: function () { select(null); } }, ["Estate"])
    ])
  ]);
  body.appendChild(bar);
  if (options.above) body.appendChild(options.above);
  body.appendChild(line);
  var split = el("div", { "class": "wd-split" }, [el("div", { "class": "wd-graph-col" }, [host, legend(options.mode)]), side]);
  body.appendChild(split);
  if (!m.nodes.size) { host.appendChild(empty("The catalogue records no installed objects yet. Build a project to see its graph.")); return; }
  var fresh = !s.view;
  view = new GraphView(host, m, lay, { state: s, mode: options.mode, onSelect: select, onReveal: reveal, label: options.label });
  apply(false);
  // A remembered viewport is kept unless it no longer shows the focus or the estate.
  if (fresh || s.refit || !view.showing(s.focus)) { s.refit = false; if (s.focus) view.fitWindow(false); else view.fitAll(false); }
  function remember() { saveState(); }
  return { select: select };
}

function legend(mode) {
  var entries = mode === "load" ? [
    ["swatch wd-ok", "Last load succeeded"], ["swatch wd-bad", "Failed, error or blocked"], ["swatch wd-warn", "Rejected rows or pending"],
    ["swatch wd-none", "Nothing settled, or loaded elsewhere"], ["ring wd-next", "Ready: dependencies settled"], ["ring wd-wait", "Waiting on a dependency"]
  ] : [
    ["kind wd-kind-table", "Table"], ["kind wd-kind-view", "View"], ["kind wd-kind-folder", "Folder"], ["kind wd-kind-shortcut", "Shortcut"],
    ["kind wd-kind-test", "Test"], ["kind wd-kind-assumption", "Assumption"], ["kind wd-kind-semantic-model", "Semantic model"], ["kind wd-kind-report", "Report"],
    ["line wd-up", "Upstream"], ["line wd-down", "Downstream"], ["dot wd-bad", "Needs attention"]
  ];
  return el("div", { "class": "wd-legend" }, entries.map(function (e) {
    return el("span", { "class": "wd-key" }, [el("i", { "class": "wd-key-" + e[0] }), e[1]]);
  }).concat([el("span", { "class": "wd-key" }, [el("i", { "class": "wd-key-more", text: "+3" }), "more beyond the window"])]));
}

function lineagePage(body, m) {
  graphWorkspace(body, m, { page: "lineage", mode: "kind", health: true, label: "Lineage graph",
    empty: "Search for an object, or select one in the graph, to follow its lineage." });
}

function loadPage(body, m) {
  var counts = {}, failed = [], ready = [], waiting = [];
  m.nodes.forEach(function (n) {
    if (!isLoadSubject(n)) return;
    var r = readiness(m, n), s = effectiveLoad(n);
    counts[r.state] = (counts[r.state] || 0) + 1;
    if (s && LOAD_RED.indexOf(s.result) >= 0) failed.push(n);
    if (r.state === READINESS.READY) ready.push({ n: n, r: r });
    if (r.state === READINESS.WAITING) waiting.push({ n: n, r: r });
  });
  var latest = (m.workflows || [])[0];
  var tiles = el("div", { "class": "wd-tiles" }, [
    tile("Current", counts[READINESS.CURRENT] || 0, "settled after every dependency", "ok"),
    tile("Ready", counts[READINESS.READY] || 0, "behind, with every dependency settled", "next"),
    tile("Waiting", counts[READINESS.WAITING] || 0, "a dependency has not settled", "wait"),
    tile("Last outcome failed", failed.length, "Failed, Error or Blocked", failed.length ? "bad" : "ok"),
    tile("Loaded elsewhere", counts[READINESS.ELSEWHERE] || 0, "mirrored; not read here", "none"),
    latest ? tile("Latest workflow", duration((latest.completed || 0) - (latest.started || 0)), fmt(latest.records) + " settled records · " + ago(latest.completed, m.at), latest.failed ? "bad" : "none") : null
  ]);
  var note = el("p", { "class": "wd-note", text: "Readiness is derived from the recorded state of each object's nearest loadable dependencies. It is not a queue: the catalogue records settled work only, so nothing here is shown as running." });
  graphWorkspace(body, m, { page: "load", mode: "load", health: true, label: "Load graph", above: el("div", {}, [tiles, note]),
    empty: "Select an object to see its last outcome and whether its dependencies have settled." });
  var lists = el("div", { "class": "wd-columns-2" }, [
    section("Ready · " + ready.length, [dataTable([
      { key: "object", title: "Object", value: function (r) { return r.n.label; } },
      { key: "item", title: "Item", value: function (r) { return r.n.item; } },
      { key: "outcome", title: "Last outcome", render: function (r) { var s = effectiveLoad(r.n); return chip(s ? s.result : null, s ? null : "None"); }, value: function (r) { var s = effectiveLoad(r.n); return s ? s.result : ""; } }
    ], ready, { limit: 100, sort: "object", desc: false, emptyText: "Nothing is ready to load." })]),
    section("Waiting · " + waiting.length, [dataTable([
      { key: "object", title: "Object", value: function (r) { return r.n.label; } },
      { key: "on", title: "Waiting on", value: function (r) { return r.r.waitingOn[0].label + (r.r.unsettled > 1 ? " and " + (r.r.unsettled - 1) + " more" : ""); } },
      { key: "outcome", title: "Last outcome", render: function (r) { var s = effectiveLoad(r.n); return chip(s ? s.result : null, s ? null : "None"); }, value: function (r) { var s = effectiveLoad(r.n); return s ? s.result : ""; } }
    ], waiting, { limit: 100, sort: "object", desc: false, emptyText: "Nothing is waiting on a dependency." })])
  ]);
  body.appendChild(lists);
}

function tile(label, value, note, toneName) {
  return el("div", { "class": "wd-tile wd-tile-" + (toneName || "none") }, [
    el("div", { "class": "wd-tile-label", text: label }),
    el("div", { "class": "wd-tile-value", text: typeof value === "number" ? fmt(value) : value }),
    note ? el("div", { "class": "wd-tile-note", text: note }) : null
  ]);
}
