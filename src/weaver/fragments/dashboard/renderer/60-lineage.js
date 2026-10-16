// Load orchestration: the estate graph, coloured by kind or by last outcome.

var DEPTHS = [0, 1, 2, -1];

/* One graph with its toolbar, status line and details. With nothing selected it
   shows the estate, or one item. A selection filters it to the selected objects
   and their lineage to the chosen depths, laid out on their own; a marker on a
   shown object expands the neighbours left out. Nodes can be dragged, and the
   graph can fill the page. Nodes are coloured by kind, or by last outcome with
   derived readiness ringed. */
function graphWorkspace(body, m, options) {
  var s = pageState(options.page), estate = layoutEstate(m);
  var handed = takeHandOff();
  if (handed && m.nodes.has(handed)) { s.selection = [handed]; s.expanded = []; s.moved = {}; s.item = null; s.refit = true; }
  s.selection = (s.selection || (s.focus ? [s.focus] : [])).filter(function (k) { return m.nodes.has(k); });
  if (!s.focus || s.selection.indexOf(s.focus) < 0) s.focus = s.selection[s.selection.length - 1] || null;
  if (s.up === undefined) s.up = 1;
  if (s.down === undefined) s.down = 1;
  if (s.item && !m.items.has(s.item)) s.item = null;
  s.expanded = (s.expanded || []).filter(function (k) { return m.nodes.has(k); });
  s.moved = s.moved || {};
  var health = options.health ? assessHealth(m) : null;
  s.mode = s.mode || options.mode;
  var above = options.above || null, legendHost = el("div");

  var host = el("div", { "class": "wd-graph" }), side = el("aside", { "class": "wd-side" }), line = el("div", { "class": "wd-status-line" });
  var chips = el("div", { "class": "wd-chips", "aria-label": "Selected objects" });
  var frame = el("div", { "class": "wd-workspace" });
  var view;
  function within() {
    return s.item ? new Set(m.items.get(s.item).nodes.map(function (n) { return n.key; })) : null;
  }
  /* A plain choice replaces the selection; Shift, Ctrl or ⌘ adds or removes. */
  function select(key, adding) {
    if (!key) s.selection = [];
    else if (adding) {
      var at = s.selection.indexOf(key);
      if (at >= 0) s.selection.splice(at, 1); else s.selection.push(key);
    } else s.selection = [key];
    if (key && s.item && m.nodes.get(key).item !== s.item) { s.item = null; itemPicker.value = ""; }
    s.focus = key && s.selection.indexOf(key) >= 0 ? key : s.selection[s.selection.length - 1] || null;
    s.expanded = []; s.moved = {};
    apply(true);
  }
  /* A marker expands the neighbours it counts, leaving its object where it is. */
  function expand(marker) {
    var screen = view.screenOf(marker.key);
    s.expanded = s.expanded.concat(revealFrom(m, marker.keys).filter(function (k) { return s.expanded.indexOf(k) < 0; }));
    apply(false);
    view.pin(marker.key, screen);
  }
  function moved(key, p) { s.moved[key] = { x: p.x, y: p.y }; saveState(); }
  function apply(refit) {
    var only = within();
    var win = s.selection.length || only ? focusWindow(m, s.selection, s.up, s.down, s.expanded, only) : null;
    var lay = win ? layoutEstate(subModel(m, win.shown), WINDOW_GEOMETRY) : layoutEstate(m);
    Object.keys(s.moved).forEach(function (k) { var p = lay.pos.get(k); if (p) { p.x = s.moved[k].x; p.y = s.moved[k].y; } });
    view.setWindow(win, lay);
    status(win);
    renderChips();
    details(side, m, s.focus, { onOpen: select, health: health, emptyText: options.empty });
    if (refit) { if (win) view.fitWindow(true); else view.fitAll(true); }
    saveState();
  }
  function renderChips() {
    chips.textContent = "";
    chips.hidden = !s.selection.length;
    s.selection.forEach(function (key) {
      var n = m.nodes.get(key);
      chips.appendChild(el("span", { "class": "wd-chip-select" + (key === s.focus ? " wd-current" : "") }, [
        el("button", { type: "button", "class": "wd-chip-name", title: n.item + " · " + qualified(n), onclick: function () { s.focus = key; details(side, m, key, { onOpen: select, health: health }); renderChips(); view.fitKeys([key], true); } },
          [kindMark(n.kind), n.label]),
        el("button", { type: "button", "class": "wd-chip-remove", "aria-label": "Remove " + n.label, onclick: function () { select(key, true); } }, ["×"])
      ]));
    });
    if (s.selection.length > 1) chips.appendChild(el("button", { type: "button", "class": "wd-text-btn wd-chip-clear", onclick: function () { select(null); } }, ["Clear"]));
  }
  function status(win) {
    line.textContent = "";
    if (!win) {
      line.appendChild(document.createTextNode(fmt(m.nodes.size) + " objects in " + fmt(estate.bands.length) + " items. Search or select an object to follow its lineage."));
      return;
    }
    var parts = [fmt(win.shown.size) + " of " + fmt(m.nodes.size) + " objects shown" + (win.seeds.size > 1 ? " · " + win.seeds.size + " selected" : "")];
    if (win.expanded.size) parts.push(fmt(win.expanded.size) + " expanded");
    if (win.hidden) parts.push(fmt(win.hidden) + " more beside them; select a ‹‹ or ›› marker to expand");
    if (win.truncated) parts.push(el("b", { "class": "wd-warn-text", text: "lineage cut to the " + fmt(win.lineage.size) + " nearest of " + fmt(win.reach) + "; lower a depth" }));
    if (win.edgesDropped) parts.push(el("b", { "class": "wd-warn-text", text: win.edgesDropped + " edges not drawn" }));
    parts.forEach(function (p, i) { if (i) line.appendChild(document.createTextNode(" · ")); line.appendChild(typeof p === "string" ? document.createTextNode(p) : p); });
  }
  var itemPicker = el("select", { "class": "wd-select", "aria-label": "Item", onchange: function () {
    s.item = this.value || null; s.expanded = []; s.moved = {};
    if (s.item) s.selection = s.selection.filter(function (k) { return m.nodes.get(k).item === s.item; });
    s.focus = s.selection[s.selection.length - 1] || null;
    apply(true);
  } }, [el("option", { value: "", text: "Every item" })].concat(estate.bands.map(function (b) {
    return el("option", { value: b.item.label, selected: b.item.label === s.item ? "selected" : null, text: b.item.label });
  })));
  var bar = el("div", { "class": "wd-toolbar" }, [
    nodeSearch(m, select),
    itemPicker,
    segmented("Up", DEPTHS, s.up, function (d) { s.up = d; s.expanded = []; s.moved = {}; apply(true); }, depthLabel),
    segmented("Down", DEPTHS, s.down, function (d) { s.down = d; s.expanded = []; s.moved = {}; apply(true); }, depthLabel),
    segmented("Colour", ["load", "kind"], s.mode, function (v) {
      s.mode = v; view.o.mode = v; view.draw(); legendHost.textContent = ""; legendHost.appendChild(legend(v)); saveState();
    }, function (v) { return v === "load" ? "Outcome" : "Kind"; }),
    el("span", { "class": "wd-grow" }),
    el("div", { "class": "wd-zoom" }, [
      el("button", { "class": "wd-icon", type: "button", title: "Zoom out", "aria-label": "Zoom out", onclick: function () { view.zoomAt(0.8, view.width / 2, view.height / 2); } }, ["−"]),
      el("button", { "class": "wd-icon", type: "button", title: "Zoom in", "aria-label": "Zoom in", onclick: function () { view.zoomAt(1.25, view.width / 2, view.height / 2); } }, ["+"]),
      el("button", { "class": "wd-text-btn", type: "button", title: "Fit what is shown", onclick: function () { view.fitWindow(true); } }, ["Fit"]),
      el("button", { "class": "wd-text-btn", type: "button", title: "Show the whole estate", onclick: function () {
        s.item = null; itemPicker.value = ""; select(null);
      } }, ["Estate"])
    ])
  ]);
  var maximise = el("button", { "class": "wd-icon wd-maximise", type: "button" });
  function setMaximised(on) {
    s.maximised = on;
    frame.classList.toggle("wd-maximised", on);
    maximise.textContent = on ? "⤡" : "⤢";
    maximise.title = on ? "Restore (Esc)" : "Fill the page";
    maximise.setAttribute("aria-label", maximise.title);
    if (view) view.resize();
    saveState();
  }
  maximise.addEventListener("click", function () { setMaximised(!s.maximised); });
  document.addEventListener("keydown", function (e) { if (e.key === "Escape" && s.maximised && frame.isConnected) setMaximised(false); });
  host.appendChild(maximise);
  if (above) body.appendChild(above);
  frame.appendChild(bar);
  frame.appendChild(chips);
  frame.appendChild(line);
  legendHost.appendChild(legend(s.mode));
  frame.appendChild(el("div", { "class": "wd-split" }, [el("div", { "class": "wd-graph-col" }, [host, legendHost]), side]));
  body.appendChild(frame);
  setMaximised(!!s.maximised);
  if (!m.nodes.size) { host.appendChild(empty("The catalogue records no installed objects yet. Build a project to see its graph.")); return; }
  var fresh = !s.view;
  view = new GraphView(host, m, estate, { state: s, mode: s.mode, onSelect: select, onReveal: expand, onMove: moved, label: options.label });
  apply(false);
  // A remembered viewport is kept unless it no longer shows the focus or the estate.
  if (fresh || s.refit || !view.showing(s.focus)) { s.refit = false; if (s.selection.length || s.item) view.fitWindow(false); else view.fitAll(false); }
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
  }).concat([el("span", { "class": "wd-key" }, [el("i", { "class": "wd-key-more", text: "3 ››" }), "neighbours not shown: select to expand"])]));
}

function orchestrationPage(body, m) {
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
  graphWorkspace(body, m, { page: "orchestration", mode: "load", health: true, label: "Load orchestration graph", above: el("div", { "class": "wd-summary" }, [tiles, note]),
    empty: "Search for an object, or select one in the graph, to follow its lineage, last outcome and readiness." });
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
