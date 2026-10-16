// The Catalogue Dashboard renderer's pure logic. Run with: node --test tests/js/dashboard.test.js
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { load } = require("./renderer.js");

const D = load();
const AT = Date.UTC(2026, 9, 16, 2, 0, 0);
const H = 3600 * 1000;

function table(name, columns, rows) {
  return D.TABLE + name + columns.map((c) => D.FIELD + c).join("") +
    rows.map((r) => D.ROW + r.map((v) => (v === null || v === undefined ? "" : String(v))).join(D.FIELD)).join("");
}

const NODE = ["key", "item", "schema", "object", "kind", "label", "flags", "description"];
const EDGE = ["down", "up", "kind", "through", "reference"];
const LOAD = ["item", "schema", "object", "result", "workflow", "started", "completed", "duration"];
const TEST = ["item", "schema", "object", "result", "workflow", "started", "completed", "failures"];

// Landing: Files -> Customer. Curated: shortcut to Customer; ByRegion reads
// Customer through it and Region; an Assumption checks ByRegion. Sales model
// reads ByRegion; a Report reads the model.
function estate(overrides = {}) {
  const nodes = [
    ["f", "Lakehouse/Landing", "Files/Sales", "Customers", "Folder", "Sales/Customers", 9, "Customer records as exported."],
    ["c", "Lakehouse/Landing", "Tables/Sales", "Customer", "Table", "Sales.Customer", 9, ""],
    ["s", "Warehouse/Curated", "Sales", "Customer", "Shortcut", "Sales.Customer", 8, ""],
    ["r", "Warehouse/Curated", "Sales", "Region", "Table", "Sales.Region", 9, "The regions customers are grouped into."],
    ["b", "Warehouse/Curated", "Sales", "ByRegion", "Table", "Sales.ByRegion", 9, ""],
    ["v", "Warehouse/Curated", "Sales", "ByRegionValid", "Assumption", "Sales.ByRegionValid", 8, ""],
    ["m", "SemanticModel/Sales", "", "", "Semantic model", "Sales", 9, ""],
    ["p", "Report/Sales", "", "", "Report", "Sales", 8, ""],
  ];
  const edges = [
    ["c", "f", "Dependency", "", ""],
    ["s", "c", "Shortcut", "", ""],
    ["b", "c", "Dependency", "s", ""],
    ["b", "r", "Dependency", "", ""],
    ["v", "b", "Validation", "", ""],
    ["m", "b", "Dependency", "", ""],
    ["p", "m", "Dependency", "", ""],
    ["r", "x", "External", "", "Ledger.dbo.Region"],
    ["b", "q", "Dependency", "", "Lakehouse/Landing/Tables/Sales.Gone"],
  ];
  const loads = overrides.loads || [
    ["Lakehouse/Landing", "Files/Sales", "Customers", "Succeeded", "w1", AT - 3 * H, AT - 3 * H + 5000, 5000],
    ["Lakehouse/Landing", "Tables/Sales", "Customer", "Succeeded", "w1", AT - 2 * H, AT - 2 * H + 9000, 9000],
    ["Warehouse/Curated", "Sales", "Region", "Succeeded", "w1", AT - 3 * H, AT - 3 * H + 1000, 1000],
    ["Warehouse/Curated", "Sales", "ByRegion", "Succeeded", "w0", AT - 30 * H, AT - 30 * H + 4000, 4000],
    ["SemanticModel/Sales", "", "", "Pending", "w1", null, null, null],
  ];
  const tests = overrides.tests || [["Warehouse/Curated", "Sales", "ByRegionValid", "Failed", "w1", AT - H, AT - H + 500, 3]];
  return table("page", ["name"], [[overrides.page || "lineage"]]) +
    table("meta", ["at", "format"], [[AT, 1]]) +
    table("items", ["item", "workspace", "target", "version"], [
      ["Warehouse/_weaver", "Analytics", "Weaver", "0.9.0"], ["Lakehouse/Landing", "Analytics", "Landing_LH", "0.9.0"],
      ["Warehouse/Curated", "Analytics", "Curated_WH", "0.9.0"], ["SemanticModel/Sales", "Analytics", "Sales", "0.9.0"],
      ["SemanticModel/Catalogue Dashboard", "Analytics", "Estate", "0.9.0"]]) +
    table("nodes", NODE, overrides.nodes || nodes) +
    table("edges", EDGE, edges) +
    table("internal", ["item"], [["Warehouse/_weaver"]]) +
    table("loads", LOAD, loads) +
    table("tests", TEST, tests) +
    table("built", ["item", "schema", "object", "built"], [["Warehouse/Curated", "Sales", "Region", AT - 2.5 * H]]) +
    (overrides.extra || "");
}

function model(text) {
  const parsed = D.parsePayload(text);
  return D.buildModel(parsed.tables, parsed.problems);
}

// --- payload ---------------------------------------------------------------------

test("the payload parses tables, nulls and the escaped <", () => {
  const text = table("t", ["a", "b"], [["x" + D.LESS + "/script>", ""], ["y", "2"]]);
  const parsed = D.parsePayload(text);
  assert.deepEqual(parsed.tables.t, [{ a: "x</script>", b: null }, { a: "y", b: "2" }]);
  assert.deepEqual(parsed.problems, []);
});

test("a row with the wrong number of fields is reported, not guessed", () => {
  const parsed = D.parsePayload(D.TABLE + "t" + D.FIELD + "a" + D.FIELD + "b" + D.ROW + "only");
  assert.deepEqual(parsed.tables.t, []);
  assert.equal(parsed.problems.length, 1);
});

// --- model -----------------------------------------------------------------------

test("the model names the catalogue and keeps the Dashboard's own items out", () => {
  const m = model(estate());
  assert.equal(m.catalogue.name, "Weaver");
  assert.equal(m.items.get("Warehouse/_weaver").internal, true);
  assert.equal(m.items.get("SemanticModel/Catalogue Dashboard").internal, true);
  assert.equal(m.items.get("Lakehouse/Landing").internal, false);
});

test("workflows are newest first whatever order the measure returns them in", () => {
  const W = ["workflow", "started", "completed", "records", "failed", "tasks"];
  const m = model(estate({ extra: table("workflows", W, [
    ["a", AT - 9 * H, AT - 8 * H, 2185, 0, "Load"],
    ["c", AT - 2 * H, AT - H, 2185, 0, "Load"],
    ["b", AT - 5 * H, AT - 4 * H, 9, 0, "Test"],
  ]) }));
  assert.deepEqual(m.workflows.map((w) => w.id), ["c", "b", "a"]);
});

test("a read through a shortcut is drawn from the shortcut and walked from the source", () => {
  const m = model(estate());
  const b = m.nodes.get("b");
  assert.deepEqual(b.up.sort(), ["c", "r"]);
  assert.deepEqual(b.vup.sort(), ["r", "s"]);
  // A read of an object with no lineage node is listed, not drawn; an External read is a stub.
  assert.equal(m.hiddenEdges, 1);
  assert.deepEqual(b.missing, ["Lakehouse/Landing/Tables/Sales.Gone"]);
  assert.deepEqual(m.nodes.get("r").stubs, ["Ledger.dbo.Region"]);
});

test("two nodes with one key are reported", () => {
  const nodes = [["k", "Lakehouse/A", "Tables/S", "One", "Table", "S.One", 9, ""], ["k", "Lakehouse/A", "Tables/S", "Two", "Table", "S.Two", 9, ""]];
  const m = model(estate({ nodes }));
  assert.equal(m.nodes.size, 1);
  assert.match(m.problems[0], /share the key k/);
});

// --- layout ----------------------------------------------------------------------

test("every node sits inside its item's band, shortcuts first and validations last", () => {
  const m = model(estate());
  const lay = D.layoutEstate(m);
  m.nodes.forEach((n) => {
    const p = lay.pos.get(n.key), b = p.band;
    assert.equal(b.item.label, n.item);
    assert.ok(p.x > b.x && p.x < b.x + b.width && p.y > b.y && p.y < b.y + b.height, n.key);
  });
  const curated = (key) => lay.pos.get(key).x;
  assert.ok(curated("s") < curated("r") && curated("s") < curated("b"));
  assert.ok(curated("v") > curated("b"));
  // One band per item.
  assert.equal(lay.bands.length, new Set(lay.bands.map((b) => b.item.label)).size);
});

test("cross-item edges run forward and the layout ignores the focus", () => {
  const m = model(estate());
  const lay = D.layoutEstate(m);
  m.nodes.forEach((n) => n.vup.forEach((u) => {
    if (m.nodes.get(u).item !== n.item) assert.ok(lay.pos.get(u).x < lay.pos.get(n.key).x, u + " -> " + n.key);
  }));
  const again = D.layoutEstate(model(estate()));
  lay.pos.forEach((p, key) => assert.deepEqual([again.pos.get(key).x, again.pos.get(key).y], [p.x, p.y]));
});

test("a large item wraps tall layers into sub-columns within its band", () => {
  const nodes = [];
  for (let i = 0; i < 2000; i++) nodes.push(["n" + i, "Lakehouse/Wide", "Tables/S", "T" + i, "Table", "S.T" + i, 9, ""]);
  const m = model(estate({ nodes }));
  const lay = D.layoutEstate(m);
  const band = lay.bands[0];
  assert.ok(band.height < 2000 * 24 / 4, "wrapped");
  m.nodes.forEach((n) => { const p = lay.pos.get(n.key); assert.ok(p.x < band.x + band.width && p.y < band.y + band.height); });
});

// --- the window ------------------------------------------------------------------

test("depth 0 shows only the selection, with its neighbours as markers to expand", () => {
  const m = model(estate());
  const win = D.focusWindow(m, "b", 0, 0, []);
  assert.deepEqual(Array.from(win.shown), ["b"]);
  assert.deepEqual(win.more.get("b").up.slice().sort(), ["r", "s"]);
  assert.deepEqual(win.more.get("b").down.slice().sort(), ["m", "v"]);
  const grown = D.focusWindow(m, "b", 0, 0, win.more.get("b").up);
  assert.deepEqual(Array.from(grown.shown).sort(), ["b", "r", "s"]);
  assert.deepEqual(Array.from(grown.expanded).sort(), ["r", "s"]);
  assert.ok(!grown.more.get("b").up.length);
});

test("an item on its own shows all of it, and lineage stays inside it", () => {
  const m = model(estate());
  const curated = new Set(m.items.get("Warehouse/Curated").nodes.map((n) => n.key));
  const whole = D.focusWindow(m, [], 1, 1, [], curated);
  assert.deepEqual(Array.from(whole.shown).sort(), Array.from(curated).sort());
  assert.equal(whole.focus, null);
  assert.ok(!whole.truncated);
  assert.deepEqual(whole.more.get("s").up, ["c"]);
  const walked = D.focusWindow(m, ["b"], -1, -1, [], curated);
  assert.ok(!walked.shown.has("c") && !walked.shown.has("m"));
  assert.ok(walked.shown.has("s") && walked.shown.has("v"));
});

test("a selection is laid out over its own objects, and each item keeps its full size", () => {
  const m = model(estate());
  const win = D.focusWindow(m, "b", 1, 0, []);
  const lay = D.layoutEstate(D.subModel(m, win.shown));
  assert.deepEqual(Array.from(lay.pos.keys()).sort(), ["b", "r", "s"]);
  const curated = lay.bands.find((b) => b.item.label === "Warehouse/Curated");
  assert.equal(curated.item.nodes.length, 3);
  assert.equal(curated.item.total, m.items.get("Warehouse/Curated").nodes.length);
  assert.ok(lay.pos.get("s").x < lay.pos.get("b").x);
});

test("each direction walks to its own depth", () => {
  const m = model(estate());
  const up = D.focusWindow(m, "b", 1, 0, []);
  assert.deepEqual(Array.from(up.lineage.keys()).sort(), ["b", "r", "s"]);
  const all = D.focusWindow(m, "b", -1, -1, []);
  assert.equal(all.lineage.get("f"), -3);
  assert.equal(all.lineage.get("p"), 2);
  assert.ok(all.edges.some((e) => e.u === "s" && e.d === "b" && e.side === "up"));
  assert.ok(all.edges.some((e) => e.u === "m" && e.d === "p" && e.side === "down"));
});

test("the budget keeps the nearest lineage and every cut neighbour becomes a marker", () => {
  const nodes = [["hub", "Warehouse/W", "S", "Hub", "Table", "S.Hub", 9, ""]];
  const edgeRows = [];
  for (let i = 0; i < 700; i++) { nodes.push(["c" + i, "Warehouse/W", "S", "C" + i, "Table", "S.C" + i, 9, ""]); edgeRows.push(["c" + i, "hub", "Dependency", "", ""]); }
  const text = table("page", ["name"], [["lineage"]]) + table("meta", ["at", "format"], [[AT, 1]]) + table("nodes", NODE, nodes) + table("edges", EDGE, edgeRows);
  const m = model(text);
  const win = D.focusWindow(m, "hub", 1, 1, []);
  assert.equal(win.lineage.size, D.BUDGET.NODES);
  assert.ok(win.truncated && win.reach === 701);
  assert.equal(win.more.get("hub").down.length, 701 - D.BUDGET.NODES);
  const expanded = D.focusWindow(m, "hub", 1, 1, win.more.get("hub").down.slice(0, 40));
  assert.equal(expanded.shown.size, D.BUDGET.NODES + 40);
});

// --- readiness and Health ---------------------------------------------------------

test("readiness is derived from dependencies and never replaces the outcome", () => {
  const m = model(estate({ loads: [
    ["Lakehouse/Landing", "Files/Sales", "Customers", "Succeeded", "w1", AT - 3 * H, AT - 3 * H, 1],
    ["Lakehouse/Landing", "Tables/Sales", "Customer", "Failed", "w1", AT - 2 * H, AT - 2 * H, 1],
    ["Warehouse/Curated", "Sales", "Region", "Succeeded", "w1", AT - 3 * H, AT - 3 * H, 1],
    ["Warehouse/Curated", "Sales", "ByRegion", "Succeeded", "w0", AT - 30 * H, AT - 30 * H, 1],
  ] }));
  // Customer's own load failed, but what it reads has settled: it may run again.
  const customer = D.readiness(m, m.nodes.get("c"));
  assert.equal(customer.state, "Ready");
  assert.equal(m.nodes.get("c").load.result, "Failed");
  // ByRegion reads Customer through the shortcut; Customer has settled nothing.
  const byRegion = D.readiness(m, m.nodes.get("b"));
  assert.equal(byRegion.state, "Waiting");
  assert.deepEqual(byRegion.waitingOn.map((n) => n.key), ["c"]);
  assert.equal(D.readiness(m, m.nodes.get("s")).state, "Not loaded here");
});

test("a load behind a newer upstream is Ready once every upstream has settled", () => {
  const m = model(estate());
  assert.equal(D.readiness(m, m.nodes.get("b")).state, "Ready");
  assert.equal(D.readiness(m, m.nodes.get("r")).state, "Current");
});

test("Health reports Weaver's codes for the catalogue's own state", () => {
  const extra = table("uncertified", ["item", "schema", "object"], [["Warehouse/Curated", "Sales", "Ghost"]]);
  const m = model(estate({ extra }));
  const health = D.assessHealth(m);
  const codes = (list) => list.map((f) => (f.node ? f.node.key : f.label) + ":" + f.code).sort();
  assert.deepEqual(codes(health.load), ["b:load_stale_ancestor", "b:load_stale_time", "m:load_pending"]);
  assert.deepEqual(codes(health.tests), ["v:test_failed"]);
  assert.deepEqual(codes(health.build), ["Warehouse/Curated Sales.Ghost:not_installed"]);
});

test("a validation that passed before its subject loaded again is stale", () => {
  const tests = [["Warehouse/Curated", "Sales", "ByRegionValid", "Succeeded", "w0", AT - 40 * H, AT - 40 * H, 0]];
  const m = model(estate({ tests }));
  const codes = D.assessHealth(m).tests.map((f) => f.code);
  assert.deepEqual(codes, ["test_stale_dependency"]);
});

test("a validation whose definition is not installed is a red Build finding", () => {
  const nodes = [["v", "Warehouse/Curated", "Sales", "ByRegionValid", "Assumption", "Sales.ByRegionValid", 0, ""]];
  const m = model(estate({ nodes }));
  assert.deepEqual(D.assessHealth(m).build.map((f) => f.code), ["missing_validation_artefact"]);
});

// --- metadata ----------------------------------------------------------------------

const SEMANTIC = table("models", ["item", "description"], [["SemanticModel/Sales", "Sales."]]) +
  table("smtables", ["item", "table", "ordinal", "description", "mode", "access"], [
    ["SemanticModel/Sales", "Sales", 1, "", "directLake", "sql"], ["SemanticModel/Sales", "Customer", 2, "", "directLake", "sql"],
    ["SemanticModel/Sales", "Date", 3, "", "", ""], ["SemanticModel/Sales", "Bridge", 4, "", "", ""], ["SemanticModel/Sales", "Notes", 5, "", "", ""]]) +
  table("smcolumns", ["item", "table", "column", "description", "datatype", "columntype", "sourcecolumn", "expression"], []) +
  table("smmeasures", ["item", "table", "measure", "description", "expression", "format"], []) +
  table("smrelationships", ["item", "name", "fromtable", "fromcolumn", "totable", "tocolumn", "fromcardinality", "tocardinality", "crossfilter", "active"], [
    ["SemanticModel/Sales", "r1", "Sales", "Customer", "Customer", "Customer", "", "", "oneDirection", "1"],
    ["SemanticModel/Sales", "r2", "Sales", "Order date", "Date", "Date", "many", "one", "", "1"],
    ["SemanticModel/Sales", "r3", "Sales", "Ship date", "Date", "Date", "many", "one", "", "0"],
    ["SemanticModel/Sales", "r4", "Date", "Year", "Customer", "Since", "one", "many", "", "1"],
    ["SemanticModel/Sales", "r5", "Customer", "Bridge", "Bridge", "Bridge", "many", "many", "bothDirections", "1"]]) +
  table("smsources", ["item", "table", "reference", "sourceitem", "sourceschema", "sourceobject"], [
    ["SemanticModel/Sales", "Sales", "Warehouse/Curated/Sales.ByRegion", "Warehouse/Curated", "Sales", "ByRegion"]]);

test("the bus matrix puts many sides against one sides and writes each relationship's direction", () => {
  const m = model(estate({ extra: SEMANTIC }));
  const mo = D.metadata(m).models.get("SemanticModel/Sales");
  const bus = D.busMatrix(mo);
  assert.deepEqual(Array.from(bus.facts.keys()).sort(), ["Customer", "Sales"]);
  assert.deepEqual(Array.from(bus.dims.keys()).sort(), ["Bridge", "Customer", "Date"]);
  const cell = (fact, dim) => bus.cells.get(fact + "\u0001" + dim);
  // Two relationships to Date, one of them inactive: a role-playing pair.
  assert.equal(cell("Sales", "Date").active, 1);
  assert.equal(cell("Sales", "Date").inactive, 1);
  const note = (fact, dim, factsAreRows) => bus.notation(cell(fact, dim).relationships[0], factsAreRows);
  assert.equal(note("Sales", "Date", true), "* ← 1");
  assert.equal(note("Sales", "Date", false), "1 → *");
  // Authored from the one side, it still filters towards the many side.
  assert.equal(note("Customer", "Date", true), "* ← 1");
  // A many-to-many relationship takes the sides its tables already have.
  assert.equal(note("Customer", "Bridge", true), "* ↔ *");
  assert.deepEqual(bus.mixed, ["Customer"]);
  assert.deepEqual(bus.other, []);
  assert.deepEqual(bus.isolated, ["Notes"]);
  assert.equal(D.metadata(m).models.get("SemanticModel/Sales").tables.get("Sales").sources[0].node.key, "b");
});

test("tables keep their authored order, and fall back to alphabetical without one", () => {
  const m = model(estate({ extra: SEMANTIC }));
  const mo = D.metadata(m).models.get("SemanticModel/Sales");
  assert.deepEqual(D.modelTables(mo, "authored").map((t) => t.name), ["Sales", "Customer", "Date", "Bridge", "Notes"]);
  assert.deepEqual(D.modelTables(mo, "alphabetical").map((t) => t.name), ["Bridge", "Customer", "Date", "Notes", "Sales"]);
  mo.tables.get("Notes").ordinal = null;
  assert.deepEqual(D.modelTables(mo, "authored").map((t) => t.name), ["Bridge", "Customer", "Date", "Notes", "Sales"]);
});

test("the ER diagram walks declared foreign keys through a cycle without repeating a table", () => {
  const FK = ["item", "schema", "object", "columns", "primaryitem", "primaryschema", "primaryobject", "primarycolumns"];
  const W = "Warehouse/Curated", L = "Lakehouse/Landing";
  const extra = table("foreignkeys", FK, [
    [W, "Sales", "ByRegion", "Region ID", W, "Sales", "Region", "Region ID"],
    [W, "Sales", "Region", "Manager ID", W, "Sales", "Manager", "Manager ID"],
    [W, "Sales", "Manager", "Home region ID", W, "Sales", "Region", "Region ID"],
    [L, "Tables/Sales", "Customer", "Region ID", W, "Sales", "Region", "Region ID"]]) +
    table("tables", ["item", "schema", "object", "type", "description", "lineage", "primarykey", "notnull", "identity", "incremental", "static", "prohibitrebuild"], [
      [W, "Sales", "Region", "Table", "", "", "Region ID", "", "", "", "", ""]]);
  const m = model(estate({ extra }));
  const all = D.erGraph(m, "all");
  const win = D.erWindow(all, W + "|Sales|Region", -1);
  assert.deepEqual(Array.from(win.kept.keys()).sort(), [W + "|Sales|ByRegion", W + "|Sales|Manager", W + "|Sales|Region", L + "|Tables/Sales|Customer"].sort());
  assert.equal(win.kept.get(W + "|Sales|Region"), 0);
  assert.deepEqual(all.nodes.get(W + "|Sales|Region").keys[0], { type: "Primary key", columns: ["Region ID"] });
  // Within one item, a relationship belongs when either end is there.
  assert.equal(D.erGraph(m, L).edges.length, 1);
});

test("dictionary search ranks names before descriptions and finds columns", () => {
  const extra = table("columns", ["item", "schema", "object", "column", "description", "identity"], [
    ["Warehouse/Curated", "Sales", "Region", "Region ID", "The region's key.", ""]]) + SEMANTIC;
  const m = model(estate({ extra }));
  const entries = D.dictionaryEntries(m);
  const found = D.searchEntries(entries, "region", "All").map((e) => e.kind + ":" + e.label);
  assert.equal(found[0], "Table:Sales.Region");
  assert.ok(found.includes("Column:Region ID"));
  assert.deepEqual(D.searchEntries(entries, "customer records", "All").map((e) => e.label), ["Sales/Customers"]);
});

// --- Gantt -----------------------------------------------------------------------

test("the Gantt summary measures elapsed span, summed work and peak concurrency", () => {
  const rows = [
    { started: 0, completed: 10, envelope: true },
    { started: 1, completed: 5, envelope: false },
    { started: 2, completed: 6, envelope: false },
    { started: 3, completed: 4, envelope: false },
    { started: 7, completed: 9, envelope: false },
  ];
  const s = D.ganttSummary(rows);
  assert.equal(s.span, 10);
  assert.equal(s.work, 10 + 4 + 4 + 1 + 2);
  assert.equal(s.peak, 3);
});

test("the catalogue and the Dashboard never appear as part of the estate", () => {
  const PERF = ["item", "schema", "object", "loads", "duration", "longest", "read", "inserted", "updated", "deleted", "rejected", "reloads", "skips", "timedread", "timedduration", "last"];
  const perf = (item, schema, object) => [item, schema, object, 1, 1000, 1000, 10, 1, 0, 0, 0, 0, 0, 10, 1000, AT];
  const extra = table("perfmonth", PERF, [
    perf("Warehouse/_weaver", "_", "LineageNode"), perf("Warehouse/Curated", "_", "Load"),
    perf("SemanticModel/Catalogue Dashboard", "", ""), perf("Warehouse/Curated", "Sales", "Region")]);
  const m = model(estate({ extra }));
  assert.deepEqual(D.perfRows(m, "perfmonth").map((r) => r.item + " " + r.object), ["Warehouse/Curated Region"]);
});

test("a selection of several objects shows the union of their lineage", () => {
  const m = model(estate());
  const win = D.focusWindow(m, ["r", "c"], 0, 1, []);
  assert.deepEqual(Array.from(win.seeds).sort(), ["c", "r"]);
  assert.equal(win.lineage.get("r"), 0);
  assert.equal(win.lineage.get("c"), 0);
  // One hop down from either: the shortcut from Customer, ByRegion from Region.
  assert.equal(win.lineage.get("s"), 1);
  assert.equal(win.lineage.get("b"), 1);
  assert.equal(win.focus, "c");
});

test("a certified object with no lineage node says lineage may be out of date", () => {
  const built = table("built", ["item", "schema", "object", "built"], [
    ["Warehouse/Curated", "Sales", "Region", AT], ["Warehouse/Curated", "Sales", "Unprojected", AT], ["Warehouse/_weaver", "_", "Log", AT]]);
  const text = estate().split(D.TABLE).filter((c) => !c.startsWith("built")).join(D.TABLE) + built;
  const m = model(text);
  assert.deepEqual(Array.from(m.graphGaps), [["Warehouse/Curated", 1]]);
});

test("the Gantt axis reads clock time, with the date where the day changes", () => {
  const a = Date.UTC(2026, 9, 16, 23, 58, 10), b = Date.UTC(2026, 9, 17, 0, 4, 0);
  const ticks = D.clockTicks(a, b, 660);
  assert.ok(ticks.length >= 2 && ticks.length <= 6);
  assert.ok(ticks.every((t) => t % 60000 === 0));
  assert.equal(D.clockLabel(a, null), "2026-10-16 23:58:10");
  assert.equal(D.clockLabel(Date.UTC(2026, 9, 16, 23, 59), a), "23:59:00");
  assert.equal(D.clockLabel(Date.UTC(2026, 9, 17, 0, 0), Date.UTC(2026, 9, 16, 23, 59)), "2026-10-17 00:00:00");
});
