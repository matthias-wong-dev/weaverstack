// Performance, from recorded load statistics. An unrecorded count stays
// unknown; it is never shown as zero.

function perfRows(m, name) {
  return (m.tables[name] || []).map(function (r) {
    var node = m.byIdentity.get(identityKey(r.item, r.schema, r.object)) || null;
    return { item: r.item, schema: r.schema, object: r.object, node: node, label: node ? node.label : r.schema + "." + r.object,
      loads: num(r.loads) || 0, duration: num(r.duration), longest: num(r.longest), read: num(r.read), inserted: num(r.inserted),
      updated: num(r.updated), deleted: num(r.deleted), rejected: num(r.rejected), reloads: num(r.reloads) || 0, skips: num(r.skips) || 0,
      timedRead: num(r.timedread), timedDuration: num(r.timedduration), last: num(r.last) };
  });
}

function changed(r) {
  if (r.inserted === null && r.updated === null && r.deleted === null) return null;
  return (r.inserted || 0) + (r.updated || 0) + (r.deleted || 0);
}

function sumOf(rows, f) {
  var total = null;
  rows.forEach(function (r) { var v = f(r); if (v !== null && v !== undefined) total = (total || 0) + v; });
  return total;
}

function throughput(read, ms) { return read === null || !ms ? null : read / (ms / 1000); }

function performancePage(body, m) {
  var s = pageState("performance");
  s.window = s.window || "month"; s.item = s.item || "all";
  var host = el("div");
  var items = Array.from(new Set((m.tables.perftrend || []).map(function (r) { return r.item; }))).sort(compare);
  body.appendChild(el("div", { "class": "wd-toolbar" }, [
    segmented("Window", ["day", "month"], s.window, function (v) { s.window = v; render(); saveState(); }, function (v) { return v === "day" ? "Last 24 hours" : "Last 30 days"; }),
    el("select", { "class": "wd-select", "aria-label": "Item", onchange: function () { s.item = this.value; render(); saveState(); } },
      [el("option", { value: "all", text: "Every item" })].concat(items.map(function (it) { return el("option", { value: it, selected: it === s.item ? "selected" : null, text: it }); })))
  ]));
  body.appendChild(host);
  function render() {
    host.textContent = "";
    var rows = perfRows(m, s.window === "day" ? "perfday" : "perfmonth").filter(function (r) { return s.item === "all" || r.item === s.item; });
    var loads = sumOf(rows, function (r) { return r.loads; }) || 0, time = sumOf(rows, function (r) { return r.duration; });
    var read = sumOf(rows, function (r) { return r.read; }), change = sumOf(rows, changed), rejected = sumOf(rows, function (r) { return r.rejected; });
    var tp = throughput(sumOf(rows, function (r) { return r.timedRead; }), sumOf(rows, function (r) { return r.timedDuration; }));
    host.appendChild(el("div", { "class": "wd-tiles" }, [
      tile("Loads", loads, sumOf(rows, function (r) { return r.reloads; }) + " reloads · " + sumOf(rows, function (r) { return r.skips; }) + " static skips", "none"),
      tile("Load time", duration(time), "summed over loads, not elapsed", "none"),
      tile("Rows read", read === null ? "—" : fmt(read), "source rows processed", "none"),
      tile("Rows changed", change === null ? "—" : fmt(change), "inserted + updated + deleted", "none"),
      tile("Rows rejected", rejected === null ? "—" : fmt(rejected), rejected ? "see the objects below" : "", rejected ? "warn" : "none"),
      tile("Throughput", tp === null ? "—" : fmt(Math.round(tp)) + "/s", "rows read per second of load time", "none")
    ]));
    host.appendChild(section("Daily trend, last 30 days", [trendChart(m, s.item)]));
    var columns = [
      { key: "object", title: "Object", value: function (r) { return r.label; }, render: function (r) { return r.node ? el("span", {}, [kindMark(r.node.kind), r.label]) : r.label; } },
      { key: "item", title: "Item", value: function (r) { return itemName(r.item); } },
      { key: "loads", title: "Loads", value: function (r) { return r.loads; }, numeric: true },
      { key: "duration", title: "Time", value: function (r) { return r.duration; }, render: function (r) { return duration(r.duration); }, numeric: true },
      { key: "longest", title: "Longest", value: function (r) { return r.longest; }, render: function (r) { return duration(r.longest); }, numeric: true },
      { key: "read", title: "Read", value: function (r) { return r.read; }, render: function (r) { return fmt(r.read); }, numeric: true },
      { key: "changed", title: "Changed", value: changed, render: function (r) { return fmt(changed(r)); }, numeric: true },
      { key: "rejected", title: "Rejected", value: function (r) { return r.rejected; }, render: function (r) { return fmt(r.rejected); }, numeric: true },
      { key: "throughput", title: "Rows/s", value: function (r) { return throughput(r.timedRead, r.timedDuration); }, render: function (r) { var v = throughput(r.timedRead, r.timedDuration); return v === null ? "—" : fmt(Math.round(v)); }, numeric: true },
      { key: "last", title: "Last", value: function (r) { return r.last; }, render: function (r) { return ago(r.last, m.at); }, numeric: true }
    ];
    host.appendChild(section("Objects · " + fmt(rows.length), [dataTable(columns, rows, { limit: 200, sort: "duration", emptyText: "No load statistic was recorded in this window." })]));
    var single = function (name) {
      return (m.tables[name] || []).filter(function (r) { return s.item === "all" || r.item === s.item; }).map(function (r) {
        var node = m.byIdentity.get(identityKey(r.item, r.schema, r.object));
        return { label: node ? node.label : r.schema + "." + r.object, item: r.item, completed: num(r.completed), duration: num(r.duration), read: num(r.read),
          inserted: num(r.inserted), updated: num(r.updated), deleted: num(r.deleted), rejected: num(r.rejected), reload: !!r.reload, skip: !!r.skip, workflow: r.workflow };
      });
    };
    var loadColumns = [
      { key: "label", title: "Object", value: function (r) { return r.label; } },
      { key: "completed", title: "Settled", value: function (r) { return r.completed; }, render: function (r) { return stamp(r.completed); }, numeric: true },
      { key: "duration", title: "Time", value: function (r) { return r.duration; }, render: function (r) { return duration(r.duration); }, numeric: true },
      { key: "changed", title: "Changed", value: changed, render: function (r) { return fmt(changed(r)); }, numeric: true },
      { key: "flags", title: "", value: function (r) { return (r.reload ? "reload " : "") + (r.skip ? "static skip" : ""); }, sortable: false }
    ];
    host.appendChild(el("div", { "class": "wd-columns-2" }, [
      section("Slowest loads, last 7 days", [dataTable(loadColumns, single("slowest"), { limit: 50, sort: "duration", emptyText: "No load in the last 7 days." })]),
      section("Largest changes, last 7 days", [dataTable(loadColumns, single("largest"), { limit: 50, sort: "changed", emptyText: "No recorded row change in the last 7 days." })])
    ]));
  }
  render();
}

function trendChart(m, item) {
  var days = new Map();
  (m.tables.perftrend || []).forEach(function (r) {
    if (item !== "all" && r.item !== item) return;
    var d = num(r.day), e = days.get(d) || { day: d, loads: 0, duration: null, changed: null };
    e.loads += num(r.loads) || 0;
    if (num(r.duration) !== null) e.duration = (e.duration || 0) + num(r.duration);
    if (num(r.changed) !== null) e.changed = (e.changed || 0) + num(r.changed);
    days.set(d, e);
  });
  if (!days.size) return empty("No load statistic was recorded in the last 30 days.");
  var list = [], newest = Math.max.apply(null, Array.from(days.keys())), W = 900, H = 170, pad = 34;
  for (var i = 29; i >= 0; i--) { var d = newest - i * 86400000; list.push(days.get(d) || { day: d, loads: 0, duration: null, changed: null }); }
  var maxTime = Math.max(1, Math.max.apply(null, list.map(function (e) { return e.duration || 0; })));
  var bw = (W - pad * 2) / list.length;
  var svg = sv("svg", { "class": "wd-trend", viewBox: "0 0 " + W + " " + H, role: "img", "aria-label": "Load time per day" });
  [0, 0.5, 1].forEach(function (f) {
    var y = H - 24 - f * (H - 44);
    svg.appendChild(sv("line", { "class": "wd-grid", x1: pad, x2: W - pad, y1: y, y2: y }));
    svg.appendChild(sv("text", { "class": "wd-axis", x: pad - 6, y: y + 3, "text-anchor": "end" }, [duration(maxTime * f).replace(" min", "m").replace(" s", "s")]));
  });
  list.forEach(function (e, i) {
    var h = (e.duration || 0) / maxTime * (H - 44), x = pad + i * bw;
    var bar = sv("rect", { "class": "wd-trend-bar", x: x + 2, y: H - 24 - h, width: Math.max(1, bw - 4), height: Math.max(e.loads ? 1 : 0, h), rx: 2 },
      [sv("title", {}, [stamp(e.day).slice(0, 10) + ": " + fmt(e.loads) + " loads · " + duration(e.duration) + " · " + fmt(e.changed) + " rows changed"])]);
    svg.appendChild(bar);
    if (i % 5 === 4 || i === list.length - 1) svg.appendChild(sv("text", { "class": "wd-axis", x: x + bw / 2, y: H - 8, "text-anchor": "middle" }, [stamp(e.day).slice(5, 10)]));
  });
  return svg;
}
