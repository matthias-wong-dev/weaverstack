// Overview and Health.

var KIND_ORDER = [KIND.FOLDER, KIND.SHORTCUT, KIND.TABLE, KIND.VIEW, KIND.MODEL, KIND.REPORT, KIND.TEST, KIND.ASSUMPTION];

function overviewPage(body, m) {
  var health = assessHealth(m), s = pageState("overview");
  var side = el("aside", { "class": "wd-drawer", hidden: "hidden" });
  function open(key) {
    s.open = key;
    side.hidden = !key;
    details(side, m, key, { onOpen: open, health: health });
    if (key) side.insertBefore(el("button", { "class": "wd-icon wd-close", type: "button", "aria-label": "Close", onclick: function () { open(null); } }, ["×"]), side.firstChild);
  }
  var loadRed = health.load.filter(function (f) { return f.severity === RED; }).length;
  var loadAmber = health.load.filter(function (f) { return f.severity === AMBER && !f.unobserved; }).length;
  var testRed = health.tests.filter(function (f) { return f.severity === RED; }).length;
  var testAmber = health.tests.filter(function (f) { return f.severity === AMBER; }).length;
  body.appendChild(el("div", { "class": "wd-tiles" }, [
    tile("Items", visibleItems(m).filter(function (it) { return it.nodes.length; }).length, fmt(m.nodes.size) + " installed objects", "none"),
    tile("Load", loadRed ? loadRed + " red" : loadAmber ? loadAmber + " amber" : "Green", health.counts.load.subjects + " subjects · " + loadAmber + " amber", loadRed ? "bad" : loadAmber ? "wait" : "ok"),
    tile("Tests", testRed ? testRed + " red" : testAmber ? testAmber + " amber" : "Green", health.counts.tests.subjects + " Tests and Assumptions", testRed ? "bad" : testAmber ? "wait" : "ok"),
    tile("Build", health.build.length ? health.build.length + " red" : "Consistent", "catalogue consistency only", health.build.length ? "bad" : "ok"),
    tile("Workflows, 7 days", m.workflowCount || 0, m.workflows.length ? "latest " + ago(m.workflows[0].completed, m.at) : "none in the last 7 days", "none")
  ]));

  var inventory = section("Installed items", [itemInventory(m, open)]);
  var findings = health.all.filter(function (f) { return !f.unobserved; });
  var significant = section("Significant findings", [findings.length ? el("ul", { "class": "wd-findings wd-findings-wide" }, findings.slice(0, 12).map(function (f) {
    return el("li", {}, [severityChip(f.severity), f.node ? nodeLink(m, f.node.key, open) : el("span", { "class": "wd-mono", text: f.label }),
      el("span", { "class": "wd-muted", text: " " + f.message })]);
  }).concat(findings.length > 12 ? [el("li", { "class": "wd-muted", text: (findings.length - 12) + " more on the Health page" })] : []))
    : el("div", { "class": "wd-allclear", text: "No findings. Every load and Test the catalogue records has settled, within the last 24 hours, after its dependencies." })]);
  var activity = section("Recent workflows", [workflowTable(m, 8)]);
  body.appendChild(el("div", { "class": "wd-overview" }, [el("div", { "class": "wd-overview-main" }, [inventory, significant, activity]), side]));
  if (s.open && m.nodes.has(s.open)) open(s.open);
}

/* Each item with its objects by kind and the state of what it loads. */
function itemInventory(m, open) {
  var items = visibleItems(m).filter(function (it) { return it.nodes.length || it.target; });
  items.sort(function (a, b) { return typeRank(a.type) - typeRank(b.type) || compare(a.name, b.name); });
  if (!items.length) return empty("Nothing is installed yet. Build a project to fill the catalogue.");
  var groups = [];
  items.forEach(function (it) {
    var g = groups[groups.length - 1];
    if (!g || g.type !== it.type) groups.push(g = { type: it.type, items: [] });
    g.items.push(it);
  });
  return el("div", { "class": "wd-inventory" }, groups.map(function (g) {
    return el("div", { "class": "wd-inv-group" }, [
      el("div", { "class": "wd-eyebrow", text: g.type + " · " + g.items.length }),
      el("div", { "class": "wd-inv-items" }, g.items.map(function (it) {
        var byKind = {}, tones = { ok: 0, bad: 0, warn: 0, wait: 0, none: 0 }, latest = null;
        it.nodes.forEach(function (n) {
          byKind[n.kind] = (byKind[n.kind] || 0) + 1;
          var st = isValidation(n) ? n.test : effectiveLoad(n);
          if (isLoadSubject(n) || isValidation(n)) tones[st ? tone(st.result) : "none"] += 1;
          if (st && st.result !== "Pending") latest = later(latest, st.completed);
        });
        var total = tones.ok + tones.bad + tones.warn + tones.wait + tones.none;
        var first = it.nodes[0];
        return el("button", { "class": "wd-inv-item", type: "button", onclick: function () { if (first) open(first.key); } }, [
          el("div", { "class": "wd-inv-head" }, [el("b", { text: it.name }), it.target && it.target !== it.name ? el("span", { "class": "wd-muted", text: " → " + it.target }) : null]),
          el("div", { "class": "wd-inv-kinds" }, KIND_ORDER.filter(function (k) { return byKind[k]; }).map(function (k) {
            return el("span", { "class": "wd-inv-kind", title: k }, [kindMark(k), fmt(byKind[k])]);
          })),
          total ? el("div", { "class": "wd-bar-stack", title: tones.ok + " succeeded · " + tones.bad + " failed · " + (tones.warn + tones.wait) + " pending or rejected · " + tones.none + " not settled" },
            ["ok", "warn", "wait", "bad", "none"].map(function (t) { return tones[t] ? el("i", { "class": "wd-" + t, style: "flex:" + tones[t] }) : null; })) : null,
          el("div", { "class": "wd-inv-meta", text: total ? "Last settled " + ago(latest, m.at) : it.nodes.length ? "Nothing to load" : "No objects" })
        ]);
      }))
    ]);
  }));
}

function workflowTable(m, limit) {
  if (!m.workflows.length) return empty("No workflow has settled a record in the last 7 days.");
  return dataTable([
    { key: "completed", title: "Settled", value: function (w) { return w.completed; }, render: function (w) { return stamp(w.completed); }, numeric: true },
    { key: "span", title: "Span", value: function (w) { return (w.completed || 0) - (w.started || 0); }, render: function (w) { return duration((w.completed || 0) - (w.started || 0)); }, numeric: true },
    { key: "tasks", title: "Work", value: function (w) { return w.tasks.join(", "); } },
    { key: "records", title: "Records", value: function (w) { return w.records; }, numeric: true },
    { key: "failed", title: "Failed", value: function (w) { return w.failed; }, render: function (w) { return w.failed ? el("span", { "class": "wd-chip wd-bad", text: fmt(w.failed) }) : "0"; }, numeric: true },
    { key: "id", title: "Workflow", value: function (w) { return w.id; }, mono: true }
  ], m.workflows, { limit: limit, sort: "completed" });
}

function healthPage(body, m) {
  var health = assessHealth(m), s = pageState("health");
  s.area = s.area || "all"; s.severity = s.severity || "all";
  var side = el("aside", { "class": "wd-drawer", hidden: "hidden" });
  function open(key) {
    side.hidden = !key;
    details(side, m, key, { onOpen: open, health: health });
    if (key) side.insertBefore(el("button", { "class": "wd-icon wd-close", type: "button", "aria-label": "Close", onclick: function () { open(null); } }, ["×"]), side.firstChild);
  }
  function sectionTile(label, area, list, subjects) {
    var red = list.filter(function (f) { return f.severity === RED; }).length, amber = list.filter(function (f) { return f.severity === AMBER && !f.unobserved; }).length;
    var unobserved = list.filter(function (f) { return f.unobserved; }).length, green = health.green.filter(function (f) { return f.area === area; }).length;
    var worst = red ? "bad" : amber ? "wait" : "ok";
    return tile(label, red ? red + " red" : amber ? amber + " amber" : "Green",
      fmt(subjects) + " subjects · " + fmt(green) + " green · " + amber + " amber" + (unobserved ? " · " + unobserved + " unobserved" : ""), worst);
  }
  body.appendChild(el("div", { "class": "wd-tiles" }, [
    sectionTile("Load", "load", health.load, health.counts.load.subjects),
    sectionTile("Tests", "tests", health.tests, health.counts.tests.subjects),
    sectionTile("Build", "build", health.build, health.counts.build.subjects)
  ]));
  body.appendChild(el("p", { "class": "wd-note" }, [
    "Assessed as of " + stamp(health.asOf) + ", 24 hours before the query. Build here checks what the catalogue records about itself: certification and installed validations. ",
    "Physical inventory, unresolved references and a mirrored source's loads need ", el("code", { text: "weaver health" }), "."
  ]));
  var tableHost = el("div");
  function render() {
    tableHost.textContent = "";
    var rows = health.all.concat(health.green).filter(function (f) {
      return (s.area === "all" || f.area === s.area) && (s.severity === "all" || (s.severity === "unobserved" ? f.unobserved : f.severity === s.severity && !f.unobserved));
    });
    tableHost.appendChild(dataTable([
      { key: "severity", title: "Severity", value: function (f) { return SEVERITY_RANK[f.severity] * 2 + (f.unobserved ? 0 : 1); }, render: function (f) { return severityChip(f.severity, f.unobserved); }, numeric: true },
      { key: "area", title: "Area", value: function (f) { return { load: "Load", tests: "Tests", build: "Build" }[f.area]; } },
      { key: "object", title: "Object", value: function (f) { return f.node ? f.node.label : f.label; }, render: function (f) { return f.node ? nodeLink(m, f.node.key, open) : el("span", { "class": "wd-mono", text: f.label }); } },
      { key: "item", title: "Item", value: function (f) { return f.node ? f.node.item : ""; } },
      { key: "result", title: "Status", value: function (f) { return f.result || ""; }, render: function (f) { return f.result ? chip(f.result) : "—"; } },
      { key: "message", title: "Finding", value: function (f) { return f.message; } },
      { key: "completed", title: "Settled", value: function (f) { return f.completed; }, render: function (f) { return f.completed ? stamp(f.completed) : "—"; }, numeric: true }
    ], rows, { limit: 500, sort: "severity", emptyText: "No findings match." }));
  }
  body.appendChild(el("div", { "class": "wd-toolbar" }, [
    segmented("Area", ["all", "load", "tests", "build"], s.area, function (v) { s.area = v; render(); saveState(); }, function (v) { return { all: "All", load: "Load", tests: "Tests", build: "Build" }[v]; }),
    segmented("Severity", ["all", "red", "amber", "green", "unobserved"], s.severity, function (v) { s.severity = v; render(); saveState(); }, function (v) { return { all: "All", red: "Red", amber: "Amber", green: "Green", unobserved: "Unobserved" }[v]; })
  ]));
  render();
  body.appendChild(el("div", { "class": "wd-overview" }, [el("div", { "class": "wd-overview-main" }, [tableHost]), side]));
}
