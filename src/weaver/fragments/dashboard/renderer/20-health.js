// Health, as weaver.health assesses it from the catalogue alone. The codes,
// severities and rules are Python's; tests/js and the parity test hold them
// together. What only `weaver health` can observe is reported as unassessed.

var GREEN = "green", AMBER = "amber", RED = "red";
var SEVERITY_RANK = { green: 0, amber: 1, red: 2 };
var DEFAULT_AGE_MS = 24 * 3600 * 1000;
var LOAD_RED = ["Failed", "Error", "Blocked"];
var NO_DATA = ["Failed", "Error", "Blocked", "Pending"];
var ESTABLISHED = ["Succeeded", "Rejected", "Skipped"];

function isView(n) { return n.kind === KIND.VIEW; }
/* Observation, not execution: a loadable, a mirrored object and a View carry
   load state. A View settles when its build succeeds. */
function participates(n) { return n.canLoad || n.isMirrored || isView(n); }
function isLoadSubject(n) { return (n.canLoad || n.isMirrored) && !isView(n); }

/* A mirrored object's state is recorded in the catalogue it mirrors, which
   this Dashboard does not read. */
function effectiveLoad(n) { return n.isMirrored ? null : n.load; }

function establishedAt(n) {
  var s = effectiveLoad(n);
  return s && ESTABLISHED.indexOf(s.result) >= 0 ? s.completed : null;
}

/* Nodes in an order where every upstream comes first. */
function topological(m) {
  var indegree = new Map(), order = [], queue = [];
  m.nodes.forEach(function (n) { indegree.set(n.key, n.up.length); if (!n.up.length) queue.push(n.key); });
  for (var i = 0; i < queue.length; i++) {
    var n = m.nodes.get(queue[i]);
    order.push(n);
    n.down.forEach(function (d) { var k = indegree.get(d) - 1; indegree.set(d, k); if (k === 0) queue.push(d); });
  }
  if (order.length < m.nodes.size) {
    var placed = new Set(order.map(function (n) { return n.key; }));
    m.nodes.forEach(function (n) { if (!placed.has(n.key)) order.push(n); });
  }
  return order;
}

/* Over each node's lifecycle ancestors: whether one has not been processed,
   the latest instant one was processed, and the latest it was processed or built. */
function ancestry(m) {
  var out = new Map();
  topological(m).forEach(function (n) {
    var a = { unprocessed: false, latest: null, changed: null };
    n.up.forEach(function (key) {
      var p = m.nodes.get(key), pa = out.get(key) || { unprocessed: false, latest: null, changed: null };
      if (pa.unprocessed) a.unprocessed = true;
      a.latest = later(a.latest, pa.latest);
      a.changed = later(a.changed, pa.changed);
      if (participates(p)) {
        var at = establishedAt(p);
        if (at === null) a.unprocessed = true;
        a.latest = later(a.latest, at);
        a.changed = later(a.changed, later(at, p.built));
      }
    });
    out.set(n.key, a);
  });
  return out;
}

function later(a, b) { return a === null ? b : b === null ? a : Math.max(a, b); }

function ancestorsOf(m, key) {
  var seen = new Set(), stack = [key], out = [];
  while (stack.length) {
    m.nodes.get(stack.pop()).up.forEach(function (u) { if (!seen.has(u)) { seen.add(u); out.push(m.nodes.get(u)); stack.push(u); } });
  }
  return out;
}

function firstAncestor(m, key, test) {
  var found = ancestorsOf(m, key).filter(function (a) { return participates(a) && test(a); });
  found.sort(function (a, b) { return compare(a.id, b.id); });
  return found[0] || null;
}

function finding(area, code, severity, node, result, message, s) {
  return { area: area, code: code, severity: severity, node: node, result: result, message: message,
    workflow: s ? s.workflow : null, completed: s ? s.completed : null, unobserved: false };
}

function assessHealth(m, options) {
  options = options || {};
  var asOf = options.asOf !== undefined ? options.asOf : m.at - DEFAULT_AGE_MS;
  // green holds each subject an area finds nothing about, apart from the findings.
  var anc = ancestry(m), load = [], tests = [], build = [], green = [];
  var counts = { load: { subjects: 0 }, tests: { subjects: 0 }, build: { subjects: 0 } };
  m.nodes.forEach(function (n) {
    counts.build.subjects += 1;
    if (isLoadSubject(n)) {
      counts.load.subjects += 1;
      var lf = loadFindings(m, n, anc, asOf), s = effectiveLoad(n);
      load.push.apply(load, lf);
      if (!lf.length) green.push(finding("load", "load_current", GREEN, n, s.result, "the last load " + s.result + (n.isStatic ? "; loaded once" : "; current with its dependencies"), s));
    }
    if (isValidation(n)) {
      counts.tests.subjects += 1;
      var tf = testFindings(m, n, anc);
      tests.push.apply(tests, tf);
      if (!tf.length) green.push(finding("tests", "test_current", GREEN, n, n.test.result, "the last run " + n.test.result + "; current with its dependencies", n.test));
      if (!n.isInstalled) build.push(finding("build", "missing_validation_artefact", RED, n, null, "its definition or artefact is not installed; build the item again"));
      else green.push(finding("build", "build_installed", GREEN, n, null, "installed and current"));
    } else green.push(finding("build", "build_installed", GREEN, n, null, "installed and current"));
  });
  (m.tables.uncertified || []).forEach(function (r) {
    if (!m.items.has(r.item) || m.items.get(r.item).internal || !m.items.get(r.item).target) return;
    build.push({ area: "build", code: "not_installed", severity: RED, node: null, label: r.item + " " + r.schema + "." + r.object,
      item: r.item, schema: r.schema, object: r.object,
      result: null, message: "declared in the dictionary, but not certified in Registry", unobserved: false });
  });
  [load, tests, build].forEach(function (list) { list.sort(byFinding); });
  return { asOf: asOf, at: m.at, load: load, tests: tests, build: build, counts: counts,
    all: load.concat(tests, build).sort(byFinding), green: green.sort(byFinding) };
}

function byFinding(a, b) {
  return SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity] || compare(a.area, b.area) ||
    compare(a.node ? a.node.id : a.label, b.node ? b.node.id : b.label) || compare(a.code, b.code);
}

function loadFindings(m, n, anc, asOf) {
  var s = effectiveLoad(n), out = [];
  if (!s || s.result === "Pending") {
    var f = finding("load", "load_pending", AMBER, n, "Pending", n.isMirrored ?
      "loaded in the catalogue this one mirrors, which the Dashboard does not read" :
      "no load has processed this object since it was built", s);
    f.unobserved = n.isMirrored;
    return [f];
  }
  if (LOAD_RED.indexOf(s.result) >= 0) return [finding("load", "load_failed", RED, n, s.result, "the last load " + s.result, s)];
  if (s.result === "Rejected") out.push(finding("load", "load_rejected", AMBER, n, s.result, "the last load completed with rejected rows", s));
  if (NO_DATA.indexOf(s.result) >= 0 || n.isStatic) return out;
  if (s.completed !== null && s.completed < asOf) out.push(finding("load", "load_stale_time", AMBER, n, s.result, "last loaded " + stamp(s.completed), s));
  var a = anc.get(n.key);
  if (a.unprocessed) {
    var behind = firstAncestor(m, n.key, function (p) { return establishedAt(p) === null; });
    out.push(finding("load", "load_ancestor_unestablished", AMBER, n, s.result,
      (behind ? behind.label : "an upstream object") + " has not been processed since it was built", s));
  }
  var loaded = establishedAt(n);
  if (loaded !== null && a.latest !== null && a.latest > loaded) {
    var newer = firstAncestor(m, n.key, function (p) { var at = establishedAt(p); return at !== null && at > loaded; });
    out.push(finding("load", "load_stale_ancestor", AMBER, n, s.result,
      (newer ? newer.label : "an upstream object") + " has been established since this object was loaded", s));
  }
  return out;
}

function testFindings(m, n, anc) {
  var s = n.test;
  if (!s) return [finding("tests", "test_pending", AMBER, n, "Pending", "this validation has not run since it was built")];
  if (LOAD_RED.indexOf(s.result) >= 0) return [finding("tests", "test_failed", RED, n, s.result, "the last run " + s.result, s)];
  if (s.result !== "Succeeded") return [finding("tests", "test_pending", AMBER, n, s.result, "the last run " + s.result, s)];
  var at = s.completed, a = anc.get(n.key);
  if (at === null || a.changed === null || a.changed <= at) return [];
  var moved = firstAncestor(m, n.key, function (p) { return (establishedAt(p) || at) > at || (p.built || at) > at; });
  var how = moved && (establishedAt(moved) || at) > at ? "loaded" : "built";
  return [finding("tests", "test_stale_dependency", AMBER, n, s.result,
    (moved ? moved.label : "an upstream object") + " has been " + how + " since this validation passed", s)];
}

function worstOf(list) {
  return list.reduce(function (w, f) { return SEVERITY_RANK[f.severity] > SEVERITY_RANK[w] ? f.severity : w; }, GREEN);
}

/* ---- readiness: derived from recorded dependencies, never a queue -------- */

/* The nearest upstream objects that carry load state, walking through those
   that do not, such as a shortcut. */
function nearestStateful(m, n) {
  var seen = new Set(), stack = n.up.slice(), out = [];
  while (stack.length) {
    var key = stack.pop();
    if (seen.has(key)) continue;
    seen.add(key);
    var p = m.nodes.get(key);
    if (participates(p)) out.push(p);
    else stack.push.apply(stack, p.up);
  }
  return out.sort(function (a, b) { return compare(a.label, b.label); });
}

var READINESS = { CURRENT: "Current", READY: "Ready", WAITING: "Waiting", ELSEWHERE: "Loaded elsewhere", NONE: "Not loaded here" };

/* Readiness keeps apart what happened (the outcome) and whether the recorded
   dependencies would let it run again now. */
function readiness(m, n) {
  if (n.isMirrored) return { state: READINESS.ELSEWHERE, waitingOn: [], unprocessed: 0 };
  if (!n.canLoad) return { state: READINESS.NONE, waitingOn: [], unprocessed: 0 };
  var ups = nearestStateful(m, n), waiting = ups.filter(function (p) { return establishedAt(p) === null; });
  if (waiting.length) return { state: READINESS.WAITING, waitingOn: waiting, unprocessed: waiting.length };
  var loaded = establishedAt(n);
  var behind = loaded === null || ups.some(function (p) { return establishedAt(p) > loaded; });
  return { state: behind ? READINESS.READY : READINESS.CURRENT, waitingOn: [], unprocessed: 0 };
}

function tone(result) {
  switch (result) {
    case "Succeeded": case "Skipped": return "ok";
    case "Failed": case "Error": case "Blocked": return "bad";
    case "Rejected": return "warn";
    case "Pending": return "wait";
    default: return "none";
  }
}
