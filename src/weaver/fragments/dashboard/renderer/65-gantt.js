// The Gantt: the settled records of one workflow. A record appears when it
// settles; nothing is drawn for work the catalogue has not recorded.

var GANTT = { LABEL: 280, ROW: 20, AXIS: 28 };

function logRows(m) {
  if (m.logRows) return m.logRows;
  var byTarget = new Map(), flows = new Map((m.tables.logflows || []).map(function (r) { return [r.index, r.workflow]; }));
  m.items.forEach(function (it) { if (it.target) byTarget.set(it.type + "|" + it.target, it.label); });
  m.logRows = (m.tables.log || []).filter(function (r) {
    var item = byTarget.get(r.targettype + "|" + r.target);
    return !(item && isInternal(m, item, r.schema)) && !(r.object && INTERNAL_SCHEMA.test(r.schema || ""));
  }).map(function (r) {
    var item = byTarget.get(r.targettype + "|" + r.target) || null;
    var node = item && r.object ? m.byIdentity.get(identityKey(item, r.schema, r.object)) || null : null;
    var started = num(r.started), completed = num(r.completed);
    return { id: r.id, workflow: flows.get(r.flow) || null, task: r.task, target: r.target, schema: r.schema, object: r.object, result: r.result,
      started: started !== null ? started : completed, completed: completed !== null ? completed : started, duration: num(r.duration),
      node: node, message: r.message, envelope: !r.object,
      label: node ? node.label : r.object ? qualified({ item: item, schema: r.schema, object: r.object }) : (r.message || r.task || "") };
  });
  return m.logRows;
}

/* Elapsed span, summed task time and peak concurrency of settled records. */
function ganttSummary(rows) {
  var t0 = Infinity, t1 = -Infinity, work = 0, events = [];
  rows.forEach(function (r) {
    if (r.started === null) return;
    t0 = Math.min(t0, r.started); t1 = Math.max(t1, r.completed);
    work += Math.max(0, r.completed - r.started);
    if (!r.envelope) { events.push([r.started, 1]); events.push([r.completed, -1]); }
  });
  events.sort(function (a, b) { return a[0] - b[0] || a[1] - b[1]; });
  var running = 0, peak = 0;
  events.forEach(function (e) { running += e[1]; peak = Math.max(peak, running); });
  return { t0: t0, t1: Math.max(t1, t0 + 1000), span: t1 > t0 ? t1 - t0 : 0, work: work, peak: peak };
}

function ganttPage(body, m) {
  var s = pageState("gantt"), all = logRows(m), flows = m.workflows || [];
  var included = new Map((m.tables.logflows || []).map(function (r) { return [r.workflow, { records: num(r.records), included: num(r.included) }]; }));
  if (!flows.length) { body.appendChild(empty("No workflow has settled a record yet. Run weaver load to see its timeline.")); return; }
  if (!s.workflow || !flows.some(function (w) { return w.id === s.workflow; }) || s.follow !== false) s.workflow = flows[0].id;
  s.scope = s.scope || "objects";
  var select = el("select", { "class": "wd-select", "aria-label": "Workflow", onchange: function () {
    s.workflow = this.value; s.follow = this.selectedIndex === 0; s.zoom = null; render(); saveState();
  } }, flows.map(function (w, i) {
    return el("option", { value: w.id, selected: w.id === s.workflow ? "selected" : null,
      text: (i === 0 ? "Latest · " : "") + stamp(w.started).slice(0, 16) + " · " + duration((w.completed || 0) - (w.started || 0)) + " · " + fmt(w.records) + " records" + (w.failed ? " · " + w.failed + " failed" : "") + " · " + w.id.slice(0, 8) });
  }));
  var host = el("div");
  body.appendChild(el("div", { "class": "wd-toolbar" }, [select,
    segmented("Show", ["objects", "all"], s.scope, function (v) { s.scope = v; render(); saveState(); }, function (v) { return v === "objects" ? "Objects" : "All records"; }),
    el("span", { "class": "wd-grow" }),
    el("span", { "class": "wd-muted", text: "Ctrl or ⌘ and scroll to zoom time; drag to pan; double-click to reset" })]));
  body.appendChild(host);
  function render() {
    host.textContent = "";
    var flow = flows.filter(function (w) { return w.id === s.workflow; })[0], have = included.get(s.workflow);
    var rows = all.filter(function (r) { return r.workflow === s.workflow; });
    if (!have || !have.included) {
      host.appendChild(el("div", { "class": "wd-banner wd-banner-warn", text: "This page carries the records of the 12 latest workflows, up to 20,000 records. This workflow's " + fmt(flow.records) + " records are not among them." }));
      return;
    }
    if (have.included < have.records) {
      host.appendChild(el("div", { "class": "wd-banner wd-banner-warn", text: "Showing the first " + fmt(have.included) + " of this workflow's " + fmt(have.records) + " records, by start time. The timeline is incomplete." }));
    }
    var sum = ganttSummary(rows), failed = rows.filter(function (r) { return tone(r.result) === "bad"; }).length;
    host.appendChild(el("div", { "class": "wd-tiles" }, [
      tile("Elapsed", duration(sum.span), stamp(sum.t0) + " to " + stamp(sum.t1).slice(11), "none"),
      tile("Task time", duration(sum.work), "summed over records; overlaps count twice", "none"),
      tile("Peak concurrency", sum.peak, "object records running at once", "none"),
      tile("Records", rows.length, flow.tasks.join(", "), "none"),
      tile("Failed", failed, "Failed, Error or Blocked", failed ? "bad" : "ok")
    ]));
    var shown = rows.filter(function (r) { return s.scope === "all" || !r.envelope; }).filter(function (r) { return r.started !== null; });
    shown.sort(function (a, b) { return a.started - b.started || compare(a.label, b.label) || compare(a.id, b.id); });
    if (!shown.length) { host.appendChild(empty("No object record has settled for this workflow.")); return; }
    host.appendChild(ganttChart(shown, sum, s, m));
  }
  render();
}

function ganttChart(rows, sum, s, m) {
  var wrap = el("div", { "class": "wd-gantt", style: "height:" + (GANTT.AXIS + rows.length * GANTT.ROW + 8) + "px" });
  var canvas = el("canvas", { "class": "wd-gantt-canvas" }), tip = el("div", { "class": "wd-tip", hidden: "hidden" });
  wrap.appendChild(canvas); wrap.appendChild(tip);
  s.zoom = s.zoom || { a: 0, b: 1 };
  var P = null, width = 0, height = 0, ratio = 1, top = 0;
  function measure() {
    var box = wrap.getBoundingClientRect(), viewport = root.innerHeight || 800;
    width = Math.max(320, box.width); ratio = root.devicePixelRatio || 1;
    top = Math.max(0, -box.top); height = Math.min(viewport, box.height - top);
    canvas.style.top = top + "px"; canvas.style.width = width + "px"; canvas.style.height = Math.max(0, height) + "px";
    canvas.width = Math.round(width * ratio); canvas.height = Math.round(Math.max(1, height) * ratio);
  }
  function time(t) { var span = sum.t1 - sum.t0, a = sum.t0 + span * s.zoom.a, b = sum.t0 + span * s.zoom.b; return GANTT.LABEL + (t - a) / (b - a) * (width - GANTT.LABEL - 16); }
  function paint() {
    measure();
    if (height <= 0) return;
    P = P || paletteOf(wrap);
    var ctx = canvas.getContext("2d"), span = sum.t1 - sum.t0;
    ctx.setTransform(ratio, 0, 0, ratio, 0, -top * ratio);
    ctx.clearRect(0, top, width, height);
    var first = Math.max(0, Math.floor((top - GANTT.AXIS) / GANTT.ROW)), last = Math.min(rows.length, Math.ceil((top + height) / GANTT.ROW));
    ctx.font = "11px " + P.sans; ctx.textBaseline = "middle";
    for (var i = first; i < last; i++) {
      var r = rows[i], y = GANTT.AXIS + i * GANTT.ROW;
      if (i % 2) { ctx.fillStyle = P["paper-deep"]; ctx.globalAlpha = 0.45; ctx.fillRect(0, y, width, GANTT.ROW); ctx.globalAlpha = 1; }
      ctx.fillStyle = r.envelope ? P.muted : P.ink;
      ctx.fillText(clip(ctx, r.label + (r.envelope ? " · " + r.task : ""), GANTT.LABEL - 18, "11px " + P.sans), 8, y + GANTT.ROW / 2);
      var x1 = Math.max(GANTT.LABEL, time(r.started)), x2 = Math.min(width - 8, time(r.completed));
      if (x2 >= GANTT.LABEL && x1 <= width - 8) {
        ctx.fillStyle = toneColour(tone(r.result), P);
        ctx.globalAlpha = r.envelope ? 0.35 : 0.9;
        ctx.beginPath(); roundRect(ctx, x1, y + 4, Math.max(2.5, x2 - x1), GANTT.ROW - 8, 3); ctx.fill(); ctx.globalAlpha = 1;
      }
    }
    // The time axis stays at the top of the visible part.
    var axisY = Math.max(top, 0);
    ctx.fillStyle = P.surface; ctx.fillRect(0, axisY, width, GANTT.AXIS);
    ctx.strokeStyle = P.line; ctx.beginPath(); ctx.moveTo(0, axisY + GANTT.AXIS - 0.5); ctx.lineTo(width, axisY + GANTT.AXIS - 0.5); ctx.stroke();
    ctx.fillStyle = P.muted; ctx.font = "10.5px " + P.mono;
    for (var g = 0; g <= 6; g++) {
      var t = sum.t0 + span * (s.zoom.a + (s.zoom.b - s.zoom.a) * g / 6), gx = time(t);
      ctx.fillText("+" + duration(t - sum.t0), Math.min(width - 60, gx + 3), axisY + 14);
      ctx.globalAlpha = 0.5; ctx.beginPath(); ctx.moveTo(gx, axisY + GANTT.AXIS); ctx.lineTo(gx, top + height); ctx.stroke(); ctx.globalAlpha = 1;
    }
    ctx.fillStyle = P.muted; ctx.font = "600 10px " + P.mono; ctx.fillText("RECORD", 8, axisY + 14);
  }
  function schedule() { (root.requestAnimationFrame || setTimeout)(paint); }
  root.addEventListener("scroll", schedule, { passive: true });
  if (root.ResizeObserver) new root.ResizeObserver(schedule).observe(wrap);
  schedule();
  wrap.addEventListener("wheel", function (e) {
    if (!(e.ctrlKey || e.metaKey)) return;
    e.preventDefault();
    var box = wrap.getBoundingClientRect(), f = Math.min(1, Math.max(0, (e.clientX - box.left - GANTT.LABEL) / (width - GANTT.LABEL - 16)));
    var a = s.zoom.a, b = s.zoom.b, at = a + (b - a) * f, w = Math.min(1, Math.max(0.0005, (b - a) * Math.exp(e.deltaY * 0.002)));
    s.zoom.a = Math.max(0, Math.min(1 - w, at - w * f)); s.zoom.b = s.zoom.a + w;
    schedule();
  }, { passive: false });
  var drag = null;
  wrap.addEventListener("pointerdown", function (e) { if (e.pointerType !== "touch") drag = { x: e.clientX, a: s.zoom.a, b: s.zoom.b }; });
  wrap.addEventListener("pointermove", function (e) {
    var box = wrap.getBoundingClientRect();
    if (drag) {
      var w = drag.b - drag.a, d = -(e.clientX - drag.x) / (width - GANTT.LABEL - 16) * w;
      s.zoom.a = Math.max(0, Math.min(1 - w, drag.a + d)); s.zoom.b = s.zoom.a + w;
      schedule(); return;
    }
    var i = Math.floor((e.clientY - box.top - GANTT.AXIS) / GANTT.ROW), r = rows[i];
    if (!r || e.clientY - box.top < GANTT.AXIS) { tip.hidden = true; return; }
    tip.textContent = "";
    tip.appendChild(el("b", { text: r.label }));
    tip.appendChild(el("div", {}, [chip(r.result), " " + r.task + " · " + duration(r.completed - r.started)]));
    tip.appendChild(el("div", { "class": "wd-muted", text: stamp(r.started) + " → " + stamp(r.completed).slice(11) }));
    if (r.message && r.object) tip.appendChild(el("div", { "class": "wd-tip-message", text: r.message }));
    tip.hidden = false;
    tip.style.left = Math.min(box.width - 270, e.clientX - box.left + 14) + "px";
    tip.style.top = (e.clientY - box.top + 14) + "px";
  });
  ["pointerup", "pointercancel"].forEach(function (t) { wrap.addEventListener(t, function () { drag = null; }); });
  wrap.addEventListener("pointerleave", function () { drag = null; tip.hidden = true; });
  wrap.addEventListener("dblclick", function () { s.zoom = { a: 0, b: 1 }; schedule(); });
  return wrap;
}

function paletteOf(node) {
  var cs = root.getComputedStyle(node), P = {};
  ["ink", "muted", "faint", "line", "line-strong", "surface", "paper", "paper-deep", "blue", "blue-dark", "green", "amber", "red", "violet", "sans", "mono"]
    .forEach(function (name) { P[name] = cs.getPropertyValue("--" + name).trim(); });
  return P;
}
