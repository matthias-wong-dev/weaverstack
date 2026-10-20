// Performance: did each run finish in time. A run is one workflow, and its time
// is the span from its first record starting to its last completing, never a
// sum of record times, so overlapping work counts once. Statistics per object
// follow, Loads and Tests apart. An unrecorded count stays unknown; it is never
// shown as zero.

var DAY = 86400000, HOUR = 3600000;
var TASK_NAMES = { load: "Load", test: "Test" };

function taskName(task) { return TASK_NAMES[task] || (task ? String(task).charAt(0).toUpperCase() + String(task).slice(1) : ""); }

/* The object's mark: its kind where the catalogue knows it, otherwise the
   task's own, so a Test is never drawn as a Load. */
function recordKind(node, task) { return node ? node.kind : task === "test" ? KIND.TEST : null; }

function targetItems(m) {
  if (m.targetItems) return m.targetItems;
  m.targetItems = new Map();
  m.items.forEach(function (it) { if (it.target) m.targetItems.set(it.type + "|" + it.target, it.label); });
  return m.targetItems;
}

function targetItem(m, type, target) { return targetItems(m).get(type + "|" + target) || (type && target ? type + "/" + target : null); }

function perfRows(m, name) {
  return (m.tables[name] || []).filter(function (r) { return !isInternal(m, r.item, r.schema); }).map(function (r) {
    var node = m.byIdentity.get(identityKey(r.item, r.schema, r.object)) || null;
    return { item: r.item, schema: r.schema, object: r.object, node: node, label: node ? node.label : r.schema + "." + r.object,
      loads: num(r.loads) || 0, duration: num(r.duration), longest: num(r.longest), read: num(r.read), inserted: num(r.inserted),
      updated: num(r.updated), deleted: num(r.deleted), rejected: num(r.rejected), reloads: num(r.reloads) || 0, skips: num(r.skips) || 0,
      timedRead: num(r.timedread), timedDuration: num(r.timedduration), last: num(r.last) };
  });
}

function testRows(m, name) {
  return (m.tables[name] || []).map(function (r) {
    var item = targetItem(m, r.targettype, r.target);
    return { item: item, schema: r.schema, object: r.object, node: item ? m.byIdentity.get(identityKey(item, r.schema, r.object)) || null : null,
      runs: num(r.runs) || 0, duration: num(r.duration), longest: num(r.longest), failed: num(r.failed) || 0, last: num(r.last) };
  }).filter(function (r) { return !(r.item && isInternal(m, r.item, r.schema)) && !(r.object && INTERNAL_SCHEMA.test(r.schema || "")); }).map(function (r) {
    r.label = r.node ? r.node.label : r.schema + "." + r.object;
    return r;
  });
}

/* The longest object records the measure carries for the runs it lists, in the
   shape the Gantt's records have. */
function runObjectRows(m) {
  if (m.runObjects) return m.runObjects;
  m.runObjects = (m.tables.runobjects || []).map(function (r) {
    var item = targetItem(m, r.targettype, r.target), node = item ? m.byIdentity.get(identityKey(item, r.schema, r.object)) || null : null;
    var started = num(r.started), completed = num(r.completed);
    return { workflow: r.workflow, task: r.task, item: item, schema: r.schema, object: r.object, node: node, result: r.result, message: r.message,
      started: started !== null ? started : completed, completed: completed !== null ? completed : started,
      label: node ? node.label : qualified({ item: item, schema: r.schema, object: r.object }) };
  }).filter(function (r) { return !(r.item && isInternal(m, r.item, r.schema)) && r.started !== null; });
  return m.runObjects;
}

function changed(r) {
  if (r.inserted === null && r.updated === null && r.deleted === null) return null;
  return (r.inserted || 0) + (r.updated || 0) + (r.deleted || 0);
}

function throughput(read, ms) { return read === null || !ms ? null : read / (ms / 1000); }

var WINDOW_LABEL = { day: "last 24 hours", week: "last 7 days", month: "last 30 days" };
var UNKNOWN = "Not recorded";

/* The 50 largest single loads of the last 7 days, each with the record's own
   counts. A count the catalogue left blank stays null. */
function largestRows(m) {
  return (m.tables.largest || []).filter(function (r) { return !isInternal(m, r.item, r.schema); }).map(function (r) {
    var node = m.byIdentity.get(identityKey(r.item, r.schema, r.object)) || null;
    return { id: r.id, item: r.item, schema: r.schema, object: r.object, node: node, label: node ? node.label : r.schema + "." + r.object,
      workflow: r.workflow, started: num(r.started), completed: num(r.completed), duration: num(r.duration), read: num(r.read),
      inserted: num(r.inserted), updated: num(r.updated), deleted: num(r.deleted), rejected: num(r.rejected), reload: r.reload === "1", skip: r.skip === "1" };
  });
}

/* Load rank of a task, so Loads come before Tests wherever both are listed. */
function taskRank(task) { return task === "load" ? 0 : task === "test" ? 1 : 2; }

function known(value, show) { return value === null || value === undefined ? UNKNOWN : show ? show(value) : fmt(value); }
function average(total, count) { return total === null || !count ? null : total / count; }
function rate(read, ms) { var v = throughput(read, ms); return v === null ? null : v < 10 ? v.toFixed(1) : fmt(Math.round(v)); }
function plural(n, word) { return fmt(n) + " " + word + (n === 1 ? "" : "s"); }
function sameObject(a, b) { return a.item === b.item && (a.schema || "") === (b.schema || "") && (a.object || "") === (b.object || ""); }

/* One section of the card: the totals of an object's Loads over the selected
   window. They describe every load in it, never one. */
function loadTotals(m, win, r) {
  var heading = "Loads · " + WINDOW_LABEL[win];
  if (!r) return { heading: heading, note: "No load statistic was recorded for this object in this window.", facts: [] };
  return { heading: heading, note: "Totals over " + plural(r.loads, "load") + ". Row counts add up the loads that recorded them.", facts: [
    ["Loads", fmt(r.loads)],
    ["Total time", known(r.duration, duration)],
    ["Average time", known(average(r.duration, r.loads), duration)],
    ["Longest", known(r.longest, duration)],
    ["Last load", r.last === null ? UNKNOWN : stamp(r.last) + " · " + ago(r.last, m.at)],
    ["Rows read", known(r.read)],
    ["Inserted", known(r.inserted)],
    ["Updated", known(r.updated)],
    ["Deleted", known(r.deleted)],
    ["Rejected", known(r.rejected)],
    ["Rows per second", rate(r.timedRead, r.timedDuration) || UNKNOWN],
    ["Reloads", fmt(r.reloads)],
    ["Static skips", fmt(r.skips)]
  ] };
}

/* The same for Tests: executions over the window. */
function testTotals(m, win, r) {
  var heading = "Tests · " + WINDOW_LABEL[win];
  if (!r) return { heading: heading, note: "No Test was recorded for this object in this window.", facts: [] };
  return { heading: heading, note: "Totals over " + plural(r.runs, "execution") + ".", facts: [
    ["Executions", fmt(r.runs)],
    ["Failed", fmt(r.failed)],
    ["Total time", known(r.duration, duration)],
    ["Average time", known(average(r.duration, r.runs), duration)],
    ["Longest", known(r.longest, duration)],
    ["Last test", r.last === null ? UNKNOWN : stamp(r.last) + " · " + ago(r.last, m.at)]
  ] };
}

function flag(value) { return value ? "Yes" : "No"; }

/* What the Performance card says about the record a row stands for. The row is
   named by `pick`: its kind (load or test totals, one run's record, or one of
   the largest changes) and the identity that finds it again after a window
   change or a refresh. Everything comes from data the page already carries,
   so an object the catalogue holds no lineage for is as inspectable as any. */
function performanceCard(m, pick, win) {
  var node = m.byIdentity.get(identityKey(pick.item, pick.schema, pick.object)) || null;
  var task = pick.kind === "test" ? "test" : pick.kind === "record" ? pick.task : "load", kind = recordKind(node, task);
  var label = node ? node.label : (pick.schema ? pick.schema + "." : "") + pick.object;
  var sections = [], scope, row, facts;
  function totals() {
    return task === "test" ? testTotals(m, win, testRows(m, "test" + win).filter(function (r) { return sameObject(r, pick); })[0])
      : loadTotals(m, win, perfRows(m, "perf" + win).filter(function (r) { return sameObject(r, pick); })[0]);
  }
  if (pick.kind === "load" || pick.kind === "test") {
    scope = "Totals over every " + task + " of this object in the " + WINDOW_LABEL[win] + ".";
    sections.push(totals());
  } else if (pick.kind === "record") {
    scope = "One " + task + " execution, then the totals for this object in the " + WINDOW_LABEL[win] + ".";
    row = runObjectRows(m).filter(function (r) {
      return r.workflow === pick.workflow && r.task === pick.task && r.started === pick.started && sameObject(r, pick);
    })[0];
    facts = row ? [
      ["Workflow", { mono: row.workflow }],
      ["Task", taskName(row.task)],
      ["Result", { chip: row.result }],
      ["Started", stamp(row.started)],
      ["Finished", stamp(row.completed)],
      ["Time", duration(row.completed - row.started)]
    ].concat(row.message ? [["Message", { text: row.message }]] : row.result && row.result !== "Succeeded" ? [["Message", UNKNOWN]] : []) : [];
    sections.push({ heading: "This execution", note: row ? null : "This record is no longer among the longest records of the runs listed.", facts: facts });
    sections.push(totals());
  } else {
    scope = "One load, then the totals for this object in the " + WINDOW_LABEL[win] + ".";
    row = largestRows(m).filter(function (r) { return r.id === pick.id; })[0];
    sections.push({ heading: "This load", note: row ? null : "This load is no longer among the 50 largest of the last 7 days.", facts: row ? [
      ["Workflow", row.workflow ? { mono: row.workflow } : UNKNOWN],
      ["Started", known(row.started, stamp)],
      ["Finished", known(row.completed, stamp)],
      ["Time", known(row.duration, duration)],
      ["Rows read", known(row.read)],
      ["Inserted", known(row.inserted)],
      ["Updated", known(row.updated)],
      ["Deleted", known(row.deleted)],
      ["Rejected", known(row.rejected)],
      ["Reload", flag(row.reload)],
      ["Static skip", flag(row.skip)]
    ] : [] });
    sections.push(totals());
  }
  return { kind: kind, eyebrow: kind || taskName(task), title: label, line: [pick.item, node ? qualified(node) : null].filter(Boolean).join(" · "),
    detached: !node, scope: scope, sections: sections };
}

function drawPerformanceCard(panel, card, onClose) {
  panel.textContent = "";
  panel.appendChild(el("button", { "class": "wd-icon wd-close", type: "button", "aria-label": "Close", onclick: onClose }, ["×"]));
  panel.appendChild(el("div", { "class": "wd-eyebrow" }, [card.kind ? kindMark(card.kind) : null, (card.kind ? " " : "") + card.eyebrow]));
  panel.appendChild(el("h2", { "class": "wd-detail-title", text: card.title }));
  if (card.line) panel.appendChild(el("div", { "class": "wd-mono wd-muted", text: card.line }));
  if (card.detached) panel.appendChild(el("p", { "class": "wd-muted", text: "The catalogue holds no lineage for this object." }));
  panel.appendChild(el("p", { "class": "wd-desc", text: card.scope }));
  card.sections.forEach(function (section) {
    panel.appendChild(el("h3", { "class": "wd-h3", text: section.heading }));
    if (section.note) panel.appendChild(el("p", { "class": "wd-muted", text: section.note }));
    if (!section.facts.length) return;
    var list = el("dl", { "class": "wd-facts" });
    section.facts.forEach(function (f) {
      var v = f[1];
      list.appendChild(el("dt", { text: f[0] }));
      list.appendChild(el("dd", {}, [typeof v === "string" ? (v === UNKNOWN ? el("span", { "class": "wd-muted", text: v }) : v)
        : v.chip !== undefined ? chip(v.chip) : el("span", { "class": v.mono ? "wd-mono" : "wd-tip-message", text: v.mono || v.text })]));
    });
    panel.appendChild(list);
  });
}

/* Every workflow of the last 30 days, newest first, with the span each task
   type of it occupied. */
function runsOf(m) {
  if (m.runs) return m.runs;
  var tasks = new Map();
  (m.tables.runtasks || []).forEach(function (r) {
    var started = num(r.started), completed = num(r.completed);
    if (started === null) started = completed;
    if (completed === null) completed = started;
    if (started !== null) addTo(tasks, r.workflow, { task: r.task, started: started, completed: completed, records: num(r.records) || 0, failed: num(r.failed) || 0 });
  });
  m.runs = (m.tables.runs || []).map(function (r) {
    var started = num(r.started), completed = num(r.completed);
    if (started === null) started = completed;
    if (completed === null) completed = started;
    return { id: r.workflow, started: started, completed: completed, records: num(r.records) || 0, objects: num(r.objects) || 0, failed: num(r.failed) || 0, tasks: tasks.get(r.workflow) || [] };
  }).filter(function (r) { return r.started !== null; });
  m.runs.sort(function (a, b) { return b.completed - a.completed || compare(a.id, b.id); });
  return m.runs;
}

/* What one run contributes under a Task filter: the whole run, or the span of
   that task's records. Null when the run holds none. */
function runView(run, task) {
  if (task === "all") return { run: run, started: run.started, completed: run.completed, elapsed: Math.max(0, run.completed - run.started), tasks: run.tasks };
  var mine = run.tasks.filter(function (t) { return t.task === task; });
  if (!mine.length) return null;
  var a = Math.min.apply(null, mine.map(function (t) { return t.started; })), b = Math.max.apply(null, mine.map(function (t) { return t.completed; }));
  return { run: run, started: a, completed: b, elapsed: Math.max(0, b - a), tasks: mine };
}

function taskSpan(run, task) {
  var v = runView(run, task);
  return v ? v.elapsed : null;
}

var MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/* A day of a wall time, as "16 Oct". */
function shortDate(wall) { var d = new Date(wall); return d.getUTCDate() + " " + MONTHS[d.getUTCMonth()]; }

function median(values) {
  if (!values.length) return null;
  var sorted = values.slice().sort(function (a, b) { return a - b; }), mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function crossesMidnight(views) {
  return views.some(function (v) { return wallDate(wallOf(v.started)) !== wallDate(wallOf(v.completed)); });
}

/* The hour of the clock a day column starts at: midnight, unless a run crosses
   it, then the middle of the longest stretch of the day no run touches, so each
   run sits in one column. */
function dayStartHour(views) {
  if (!views.length || !crossesMidnight(views)) return 0;
  var covered = [], h;
  for (h = 0; h < 24; h++) covered.push(false);
  views.forEach(function (v) {
    if (v.completed - v.started >= DAY) { for (var i = 0; i < 24; i++) covered[i] = true; return; }
    for (var t = Math.floor(wallOf(v.started) / HOUR) * HOUR; t <= wallOf(v.completed); t += HOUR) covered[new Date(t).getUTCHours()] = true;
  });
  var best = 0, at = 0;
  for (h = 0; h < 24; h++) {
    if (covered[h] || !covered[(h + 23) % 24]) continue;
    var len = 0;
    while (len < 24 && !covered[(h + len) % 24]) len++;
    if (len > best) { best = len; at = (h + (len >> 1)) % 24; }
  }
  return best ? at : 0;
}

var WINDOW_DAYS = { day: 1, week: 7, month: 30 };

/* Runs as vertical bars: one column per day, time of day down the side, each
   run from its start to its end. Runs sharing a day stand side by side. */
function runChart(views, s, m, pick) {
  var W = 1800, H = 340, L = 64, R = 14, T = 12, B = 34, columns = WINDOW_DAYS[s.window] + (s.window === "day" ? 1 : 0);
  var cut = dayStartHour(views), cutMs = cut * HOUR, plotW = W - L - R, plotH = H - T - B, colW = plotW / columns;
  // Everything below is wall time, so a day is one DAY wide in any zone.
  var last = Math.floor((wallOf(m.at) - cutMs) / DAY), first = last - (columns - 1);
  function cycle(v) { return Math.floor((wallOf(v.started) - cutMs) / DAY); }
  function base(v) { return cycle(v) * DAY + cutMs; }
  function offset(v) { return wallOf(v.started) - base(v); }
  function endOffset(v) { return wallOf(v.completed) - base(v); }
  var shown = views.filter(function (v) { return cycle(v) >= first && cycle(v) <= last; });
  var deadline = s.deadline === "" || s.deadline === undefined ? null : ((Number(s.deadline) * HOUR - cutMs) % DAY + DAY) % DAY;
  var lo = 0, hi = 24;
  if (s.axis !== "day" && shown.length) {
    lo = Math.floor(Math.min.apply(null, shown.map(offset)) / HOUR);
    hi = Math.ceil(Math.max.apply(null, shown.map(endOffset)) / HOUR);
    if (deadline !== null) { lo = Math.min(lo, Math.floor(deadline / HOUR)); hi = Math.max(hi, Math.ceil(deadline / HOUR)); }
    if (hi - lo < 2) hi = lo + 2;
    if (hi > lo + 24) hi = lo + 24;
  }
  function y(ms) { var hours = Math.min(hi, Math.max(lo, ms / HOUR)); return T + (hours - lo) / (hi - lo) * plotH; }
  var svg = sv("svg", { "class": "wd-runs", viewBox: "0 0 " + W + " " + H, role: "img", "aria-label": "Runs by time of day" });
  var step = [0.5, 1, 2, 3, 4, 6, 12].filter(function (st) { return (hi - lo) / st <= 9; })[0] || 12;
  for (var i = 0; i < columns; i++) {
    if (i % 2) svg.appendChild(sv("rect", { "class": "wd-runs-band", x: L + i * colW, y: T, width: colW, height: plotH }));
  }
  for (var t = Math.ceil(lo / step) * step; t <= hi + 1e-9; t += step) {
    var gy = y(t * HOUR), wall = ((cut + t) % 24 + 24) % 24, hh = Math.floor(wall), mm = Math.round((wall - hh) * 60);
    svg.appendChild(sv("line", { "class": "wd-grid", x1: L, x2: W - R, y1: gy, y2: gy }));
    svg.appendChild(sv("text", { "class": "wd-axis", x: L - 8, y: gy + 3.5, "text-anchor": "end" }, [(hh < 10 ? "0" : "") + hh + ":" + (mm < 10 ? "0" : "") + mm]));
  }
  var every = Math.max(1, Math.ceil(columns / Math.floor(plotW / 72)));
  for (var c = 0; c < columns; c++) {
    if ((columns - 1 - c) % every) continue;
    svg.appendChild(sv("text", { "class": "wd-axis", x: L + c * colW + colW / 2, y: H - 12, "text-anchor": "middle" }, [shortDate((first + c) * DAY + cutMs)]));
  }
  var byColumn = new Map();
  shown.forEach(function (v) { addTo(byColumn, cycle(v) - first, v); });
  byColumn.forEach(function (list, col) {
    list.sort(function (a, b) { return a.started - b.started || compare(a.run.id, b.run.id); });
    var slot = Math.min(44, (colW - 8) / list.length), x0 = L + col * colW + (colW - slot * list.length) / 2;
    list.forEach(function (v, k) {
      var top = y(offset(v)), bottom = y(endOffset(v)), x = x0 + k * slot + 1.5, w = Math.max(2, slot - 3);
      var failed = v.run.failed > 0, picked = v.run.id === s.run;
      var title = stamp(v.started) + " → " + stamp(v.completed).slice(11) + " · " + duration(v.elapsed) + " · " + fmt(v.run.records) + " records" +
        (failed ? " · " + v.run.failed + " failed" : "") + " · " + v.run.id.slice(0, 8);
      var group = sv("g", { "class": "wd-run" + (picked ? " wd-run-picked" : ""), role: "button", tabindex: "0", "aria-label": title, onclick: function () { pick(v.run.id); },
        onkeydown: function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); pick(v.run.id); } } }, [sv("title", {}, [title])]);
      group.appendChild(sv("rect", { "class": "wd-run-hit", x: x - 1.5, y: top - 3, width: slot, height: Math.max(8, bottom - top + 6) }));
      group.appendChild(sv("rect", { "class": "wd-run-bar" + (failed ? " wd-run-failed" : ""), x: x, y: top, width: w, height: Math.max(3, bottom - top), rx: 2 }));
      v.tasks.slice().sort(function (a, b) { return a.started - b.started; }).forEach(function (task) {
        var a = y(wallOf(task.started) - base(v)), b = y(wallOf(task.completed) - base(v));
        group.appendChild(sv("rect", { "class": "wd-run-task wd-run-" + (TASK_NAMES[task.task] ? task.task : "other"), x: x + 1, y: a, width: Math.max(1, w - 2), height: Math.max(2, b - a), rx: 1.5 }));
      });
      svg.appendChild(group);
    });
  });
  if (deadline !== null && deadline / HOUR >= lo && deadline / HOUR <= hi) {
    svg.appendChild(sv("line", { "class": "wd-deadline", x1: L, x2: W - R, y1: y(deadline), y2: y(deadline) }));
    svg.appendChild(sv("text", { "class": "wd-deadline-label", x: W - R - 4, y: y(deadline) - 5, "text-anchor": "end" }, ["Deadline " + (s.deadline < 10 ? "0" : "") + s.deadline + ":00"]));
  }
  return { svg: svg, endOffset: endOffset, deadline: deadline, cut: cut, shown: shown };
}

function performancePage(body, m) {
  var s = pageState("performance"), zone = zoneLabel(m.at);
  if (WINDOW_DAYS[s.window] === undefined) s.window = "week";
  if (["all", "load", "test"].indexOf(s.task) < 0) s.task = "all";
  if (["fit", "day"].indexOf(s.axis) < 0) s.axis = "fit";
  if (s.deadline === undefined || s.deadline === null) s.deadline = "";
  s.item = s.item || "all";
  if (!s.card || typeof s.card.kind !== "string") s.card = null;
  var side = el("aside", { "class": "wd-drawer", hidden: "hidden" });
  /* The card follows the window, so it is drawn again whenever the page is. */
  function card() {
    side.hidden = !s.card;
    if (!s.card) { side.textContent = ""; return; }
    drawPerformanceCard(side, performanceCard(m, s.card, s.window), function () { pick(null); });
  }
  function pick(choice) { s.card = choice; card(); saveState(); }
  var host = el("div", { "class": "wd-overview-main" });
  var allItems = Array.from(new Set(["perfmonth", "testmonth"].reduce(function (all, name) {
    return all.concat((m.tables[name] || []).map(function (r) { return r.item || targetItem(m, r.targettype, r.target); }));
  }, []).filter(Boolean))).filter(function (it) { return !isInternal(m, it); }).sort(compare);
  var deadlines = [el("option", { value: "", selected: s.deadline === "" ? "selected" : null, text: "No deadline" })];
  for (var h = 0; h < 24; h++) deadlines.push(el("option", { value: String(h), selected: s.deadline === String(h) ? "selected" : null, text: (h < 10 ? "0" : "") + h + ":00 " + zone }));
  body.appendChild(el("div", { "class": "wd-toolbar" }, [
    segmented("Window", ["day", "week", "month"], s.window, function (v) { s.window = v; render(); saveState(); }, function (v) { return { day: "Last 24 hours", week: "Last 7 days", month: "Last 30 days" }[v]; }),
    segmented("Task", ["all", "load", "test"], s.task, function (v) { s.task = v; render(); saveState(); }, function (v) { return { all: "All", load: "Load", test: "Test" }[v]; }),
    segmented("Axis", ["fit", "day"], s.axis, function (v) { s.axis = v; render(); saveState(); }, function (v) { return v === "fit" ? "Fit runs" : "24 hours"; }),
    el("select", { "class": "wd-select", "aria-label": "Deadline", onchange: function () { s.deadline = this.value; render(); saveState(); } }, deadlines),
    el("select", { "class": "wd-select", "aria-label": "Item", onchange: function () { s.item = this.value; render(); saveState(); } },
      [el("option", { value: "all", text: "Every item" })].concat(allItems.map(function (it) { return el("option", { value: it, selected: it === s.item ? "selected" : null, text: it }); })))
  ]));
  body.appendChild(el("div", { "class": "wd-overview" }, [host, side]));
  function render() {
    host.textContent = "";
    var from = m.at - WINDOW_DAYS[s.window] * DAY;
    var views = runsOf(m).filter(function (r) { return r.completed >= from; }).map(function (r) { return runView(r, s.task); }).filter(Boolean);
    if (!views.some(function (v) { return v.run.id === s.run; })) s.run = views.length ? views[0].run.id : null;
    var elapsed = views.map(function (v) { return v.elapsed; }), latest = views[0] || null;
    var longest = views.reduce(function (a, v) { return !a || v.elapsed > a.elapsed ? v : a; }, null);
    var typical = median(elapsed), failedRuns = views.filter(function (v) { return v.run.failed > 0; }).length;
    var chart = runChart(views, s, m, function (id) { s.run = id; render(); saveState(); });
    var late = chart.deadline === null ? null : views.filter(function (v) { return chart.shown.indexOf(v) >= 0 && chart.endOffset(v) > chart.deadline; }).length;
    host.appendChild(el("div", { "class": "wd-tiles" }, [
      tile("Runs", views.length, failedRuns ? failedRuns + " with failed records" : "none with failed records", failedRuns ? "bad" : "none"),
      tile("Latest run", latest ? duration(latest.elapsed) : "—", latest ? "ended " + ago(latest.completed, m.at) + " · " + stamp(latest.completed) : "none in this window", "none"),
      tile("Typical run", typical === null ? "—" : duration(typical), "median of the runs in this window", "none"),
      tile("Longest run", longest ? duration(longest.elapsed) : "—", longest ? stamp(longest.started) : "", "none"),
      tile("Past the deadline", late === null ? "—" : late + " of " + chart.shown.length, late === null ? "set a deadline above" : "runs ending after " + (s.deadline < 10 ? "0" : "") + s.deadline + ":00 " + zone, late ? "warn" : "none")
    ]));
    var legend = el("div", { "class": "wd-legend" }, [
      el("span", {}, [el("i", { "class": "wd-swatch wd-swatch-load" }), "Load"]),
      el("span", {}, [el("i", { "class": "wd-swatch wd-swatch-test" }), "Test"]),
      el("span", {}, [el("i", { "class": "wd-swatch wd-swatch-failed" }), "A record failed"]),
      el("span", { text: "Time of day, " + zone + (chart.cut ? ". Each column runs from " + (chart.cut < 10 ? "0" : "") + chart.cut + ":00 to the same time the next day." : ".") })
    ]);
    host.appendChild(section("Runs", views.length ? [chart.svg, legend] : [empty("No workflow ran in this window.")]));

    var runColumns = [
      { key: "started", title: "Started", value: function (v) { return v.started; }, render: function (v) { return el("span", {}, [stamp(v.started), el("span", { "class": "wd-link-meta wd-mono", text: v.run.id.slice(0, 8) })]); } },
      { key: "completed", title: "Finished", value: function (v) { return v.completed; }, render: function (v) { return stamp(v.completed).slice(11); }, numeric: true },
      { key: "elapsed", title: "Elapsed", value: function (v) { return v.elapsed; }, render: function (v) { return duration(v.elapsed); }, numeric: true },
      { key: "load", title: "Load", value: function (v) { return taskSpan(v.run, "load"); }, render: function (v) { var t = taskSpan(v.run, "load"); return t === null ? "—" : duration(t); }, numeric: true },
      { key: "test", title: "Test", value: function (v) { return taskSpan(v.run, "test"); }, render: function (v) { var t = taskSpan(v.run, "test"); return t === null ? "—" : duration(t); }, numeric: true },
      { key: "records", title: "Records", value: function (v) { return v.run.records; }, render: function (v) { return fmt(v.run.records); }, numeric: true },
      { key: "failed", title: "Failed", value: function (v) { return v.run.failed; }, render: function (v) { return v.run.failed ? chip("Failed", fmt(v.run.failed)) : "0"; }, numeric: true }
    ];
    host.appendChild(section("Runs · " + fmt(views.length), [dataTable(runColumns, views, { limit: 200, sort: "started", onRow: function (v) { s.run = v.run.id; render(); saveState(); }, emptyText: "No workflow ran in this window." })]));

    var picked = views.filter(function (v) { return v.run.id === s.run; })[0] || null;
    if (picked) host.appendChild(runObjects(picked));
    function itemFilter(r) { return s.item === "all" || r.item === s.item; }
    if (s.task !== "test") {
      var loads = perfRows(m, "perf" + s.window).filter(itemFilter);
      host.appendChild(section("Loads · " + fmt(loads.length) + " objects, " + WINDOW_LABEL[s.window], [dataTable([
        { key: "object", title: "Object", value: function (r) { return r.label; }, render: function (r) { return objectCell(r.node, r.label, "load"); } },
        { key: "item", title: "Item", value: function (r) { return r.item; } },
        { key: "loads", title: "Loads", value: function (r) { return r.loads; }, numeric: true },
        { key: "duration", title: "Time", value: function (r) { return r.duration; }, render: function (r) { return duration(r.duration); }, numeric: true },
        { key: "longest", title: "Longest", value: function (r) { return r.longest; }, render: function (r) { return duration(r.longest); }, numeric: true },
        { key: "read", title: "Read", value: function (r) { return r.read; }, render: function (r) { return fmt(r.read); }, numeric: true },
        { key: "changed", title: "Changed", value: changed, render: function (r) { return fmt(changed(r)); }, numeric: true },
        { key: "rejected", title: "Rejected", value: function (r) { return r.rejected; }, render: function (r) { return fmt(r.rejected); }, numeric: true },
        { key: "throughput", title: "Rows/s", value: function (r) { return throughput(r.timedRead, r.timedDuration); }, render: function (r) { var v = throughput(r.timedRead, r.timedDuration); return v === null ? "—" : fmt(Math.round(v)); }, numeric: true },
        { key: "last", title: "Last", value: function (r) { return r.last; }, render: function (r) { return ago(r.last, m.at); }, numeric: true }
      ], loads, { limit: 200, sort: "duration", onRow: function (r) { pick({ kind: "load", item: r.item, schema: r.schema, object: r.object }); }, emptyText: "No load statistic was recorded in this window." })]));
    }
    if (s.task !== "load") {
      var tests = testRows(m, "test" + s.window).filter(itemFilter);
      host.appendChild(section("Tests · " + fmt(tests.length) + " objects, " + WINDOW_LABEL[s.window], [dataTable([
        { key: "object", title: "Object", value: function (r) { return r.label; }, render: function (r) { return objectCell(r.node, r.label, "test"); } },
        { key: "item", title: "Item", value: function (r) { return r.item; } },
        { key: "runs", title: "Runs", value: function (r) { return r.runs; }, numeric: true },
        { key: "duration", title: "Time", value: function (r) { return r.duration; }, render: function (r) { return duration(r.duration); }, numeric: true },
        { key: "longest", title: "Longest", value: function (r) { return r.longest; }, render: function (r) { return duration(r.longest); }, numeric: true },
        { key: "failed", title: "Failed", value: function (r) { return r.failed; }, render: function (r) { return r.failed ? chip("Failed", fmt(r.failed)) : "0"; }, numeric: true },
        { key: "last", title: "Last", value: function (r) { return r.last; }, render: function (r) { return ago(r.last, m.at); }, numeric: true }
      ], tests, { limit: 200, sort: "duration", onRow: function (r) { pick({ kind: "test", item: r.item, schema: r.schema, object: r.object }); }, emptyText: "No Test was recorded in this window." })]));
    }
    if (s.task !== "test") {
      var largest = largestRows(m).filter(itemFilter);
      host.appendChild(section("Largest changes, last 7 days", [dataTable([
        { key: "object", title: "Object", value: function (r) { return r.label; }, render: function (r) { return objectCell(r.node, r.label, "load"); } },
        { key: "item", title: "Item", value: function (r) { return r.item; } },
        { key: "completed", title: "Processed", value: function (r) { return r.completed; }, render: function (r) { return stamp(r.completed); }, numeric: true },
        { key: "duration", title: "Time", value: function (r) { return r.duration; }, render: function (r) { return duration(r.duration); }, numeric: true },
        { key: "changed", title: "Changed", value: changed, render: function (r) { return fmt(changed(r)); }, numeric: true },
        { key: "workflow", title: "Run", value: function (r) { return r.workflow; }, render: function (r) { return r.workflow ? String(r.workflow).slice(0, 8) : "—"; }, mono: true }
      ], largest, { limit: 50, sort: "changed", onRow: function (r) { pick({ kind: "change", id: r.id, item: r.item, schema: r.schema, object: r.object }); }, emptyText: "No recorded row change in the last 7 days." })]));
    }
    card();
  }
  /* The object cell every table shares: the object's mark, then its name. */
  function objectCell(node, label, task) {
    var kind = recordKind(node, task);
    return el("span", { "class": "wd-object" }, [kind ? kindMark(kind) : null, label]);
  }
  function runObjects(v) {
    var title = "Objects in the run of " + stamp(v.started).slice(0, 16);
    var carried = runObjectRows(m).filter(function (r) { return r.workflow === v.run.id; });
    if (!carried.length) return section(title, [empty("Objects are listed for the 8 latest runs. Select a newer run, or find this one in the Gantt.")]);
    var rows = carried.filter(function (r) { return (s.task === "all" || r.task === s.task) && (s.item === "all" || r.item === s.item); });
    var columns = [
      { key: "label", title: "Object", value: function (r) { return r.label; }, render: function (r) { return objectCell(r.node, r.label, r.task); } },
      { key: "item", title: "Item", value: function (r) { return r.item; } },
      { key: "task", title: "Task", value: function (r) { return taskName(r.task); } },
      { key: "result", title: "Result", value: function (r) { return r.result; }, render: function (r) { return chip(r.result); } },
      { key: "started", title: "Started", value: function (r) { return r.started; }, render: function (r) { return stamp(r.started).slice(11); }, numeric: true },
      { key: "duration", title: "Time", value: function (r) { return r.completed - r.started; }, render: function (r) { return duration(r.completed - r.started); }, numeric: true },
      { key: "message", title: "Message", value: function (r) { return r.message || ""; } }
    ];
    var note = v.run.objects > carried.length ? el("div", { "class": "wd-banner wd-banner-warn", text: "The " + fmt(carried.length) + " longest of this run's " + fmt(v.run.objects) + " object records. The Gantt has them all." }) : null;
    return section(title + " · " + fmt(rows.length), [note, dataTable(columns, rows, { limit: 200, sort: "duration", group: function (r) { return taskRank(r.task); },
      onRow: function (r) { pick({ kind: "record", workflow: r.workflow, task: r.task, started: r.started, item: r.item, schema: r.schema, object: r.object }); }, emptyText: "No object record matches." })]);
  }
  render();
}
