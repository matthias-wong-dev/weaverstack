// The Catalogue Dashboard renderer's pure logic. Run with: node --test tests/js
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const dashboard = require(path.join(__dirname, "../../src/weaver/fragments/dashboard/dashboard.js"));

const AT = "2026-10-10T05:00:00Z";

function node(id, kind = "Table", item = "Warehouse/Curated", desc = "") {
  return [id, id.split("/").pop(), kind, item, id, desc];
}

// Files -> Customer -> Shortcut -> ByRegion -> Model -> Report; Region -> ByRegion; Valid checks ByRegion.
function estate() {
  return {
    at: AT,
    nodes: [
      node("L/Files.Customers", "Folder", "Lakehouse/Landing", "Customer records as exported."),
      node("L/Sales.Customer", "Table", "Lakehouse/Landing"),
      node("W/Sales.Customer", "Shortcut"),
      node("W/Sales.Region", "Table", "Warehouse/Curated", "The regions customers are grouped into."),
      node("W/Sales.ByRegion"),
      node("W/Sales.ByRegionValid", "Assumption"),
      node("M/Sales", "Semantic model", "SemanticModel/Sales"),
      node("R/Sales", "Report", "Report/Sales"),
    ],
    edges: [
      ["L/Sales.Customer", "L/Files.Customers", "Dependency"],
      ["W/Sales.Customer", "L/Sales.Customer", "Shortcut"],
      ["W/Sales.ByRegion", "W/Sales.Customer", "Dependency"],
      ["W/Sales.ByRegion", "W/Sales.Region", "Dependency"],
      ["W/Sales.ByRegionValid", "W/Sales.ByRegion", "Validation"],
      ["M/Sales", "W/Sales.ByRegion", "Dependency"],
      ["R/Sales", "M/Sales", "Dependency"],
      ["W/Sales.Region", "Sales.Elsewhere", "External"],
    ],
    loads: [
      ["L/Files.Customers", "Succeeded", "2026-10-10T04:00:00Z", "2026-10-10T04:00:05Z", 5000],
      ["L/Sales.Customer", "Succeeded", "2026-10-10T04:00:06Z", "2026-10-10T04:00:20Z", 14000],
      ["W/Sales.Region", "Succeeded", "2026-10-10T04:00:00Z", "2026-10-10T04:00:04Z", 4000],
      ["W/Sales.ByRegion", "Succeeded", "2026-10-10T03:00:00Z", "2026-10-10T03:00:04Z", 4000],
      ["M/Sales", "Pending", null, null, null],
    ],
    tests: [["W/Sales.ByRegionValid", "Failed", "2026-10-10T04:59:30Z", 3]],
    stats: [["W/Sales.Region", 3, 0, 0, 0, 0]],
    columns: [["W/Sales.Region", "Region ID", "The region's key."]],
  };
}

const ids = (win) => win.nodes.map((n) => n.id).sort();

test("the window walks each direction to its own depth", () => {
  const g = dashboard.buildGraph(estate());
  assert.deepEqual(ids(dashboard.windowAround(g, "W/Sales.ByRegion", 0, 0)), ["W/Sales.ByRegion"]);
  assert.deepEqual(ids(dashboard.windowAround(g, "W/Sales.ByRegion", 1, 0)),
    ["W/Sales.ByRegion", "W/Sales.Customer", "W/Sales.Region"]);
  assert.deepEqual(ids(dashboard.windowAround(g, "W/Sales.ByRegion", 0, 2)),
    ["M/Sales", "R/Sales", "W/Sales.ByRegion", "W/Sales.ByRegionValid"]);
  const all = dashboard.windowAround(g, "W/Sales.ByRegion", -1, -1);
  assert.equal(all.nodes.length, 8);
  assert.equal(all.nodes.find((n) => n.id === "L/Files.Customers").dist, -3);
  assert.equal(all.truncated, false);
});

test("the budget keeps the nearest nodes, upstream first, and marks the frontier", () => {
  const g = dashboard.buildGraph(estate());
  const win = dashboard.windowAround(g, "W/Sales.ByRegion", -1, -1, { nodes: 4, edges: 2 });
  assert.equal(win.nodes.length, 4);
  assert.equal(win.reach, 8);
  assert.ok(win.truncated);
  // Distance one: Customer and Region upstream before ByRegionValid downstream.
  assert.deepEqual(win.nodes.map((n) => n.id),
    ["W/Sales.ByRegion", "W/Sales.Customer", "W/Sales.Region", "M/Sales"]);
  assert.ok(win.edges.length <= 2);
  assert.deepEqual(win.more["W/Sales.Customer"], { up: 1, down: 0 });
  assert.deepEqual(win.more["M/Sales"], { up: 0, down: 1 });
  assert.equal(win.more["W/Sales.ByRegion"].down, 1);
});

test("an external read is a stub, not a node", () => {
  const g = dashboard.buildGraph(estate());
  const win = dashboard.windowAround(g, "W/Sales.Region", 0, 0);
  assert.deepEqual(win.stubs, [{ d: "W/Sales.Region", ref: "Sales.Elsewhere" }]);
});

test("the layout places columns by signed distance and keeps items together", () => {
  const g = dashboard.buildGraph(estate());
  const win = dashboard.windowAround(g, "W/Sales.ByRegion", -1, -1);
  const lay = dashboard.layout(g, win);
  const x = (id) => lay.positions.get(id).x;
  assert.ok(x("L/Files.Customers") < x("L/Sales.Customer"));
  assert.ok(x("L/Sales.Customer") < x("W/Sales.ByRegion"));
  assert.ok(x("W/Sales.ByRegion") < x("M/Sales"));
  assert.equal(x("W/Sales.ByRegion"), 0);
  for (const n of win.nodes) {
    const p = lay.positions.get(n.id);
    const box = lay.groups.find((b) => b.item === g.nodes.get(n.id).item && b.x <= p.x && p.x < b.x + b.w &&
      b.y <= p.y && p.y < b.y + b.h);
    assert.ok(box, `${n.id} sits inside its item's box`);
  }
  const ys = win.nodes.filter((n) => n.dist === -1).map((n) => lay.positions.get(n.id).y);
  assert.equal(new Set(ys).size, ys.length, "no two nodes share a row");
});

test("readiness reads only direct upstreams", () => {
  const g = dashboard.buildGraph(estate());
  // ByRegion ran before its upstreams settled again: they are all fresher.
  assert.equal(dashboard.readiness(g, "W/Sales.ByRegion"), "Candidate");
  // The model is Pending and its one upstream has settled.
  assert.equal(dashboard.readiness(g, "M/Sales"), "Candidate");
  // A root that settled is Done.
  assert.equal(dashboard.readiness(g, "L/Files.Customers"), "Done");
  // The Report has no status of its own.
  assert.equal(dashboard.readiness(g, "R/Sales"), null);
  // Waiting while an upstream is Pending.
  const data = estate();
  data.loads.push(["R/Sales", "Succeeded", AT, AT, 1]);
  assert.equal(dashboard.readiness(dashboard.buildGraph(data), "R/Sales"), "Waiting");
});

test("search ranks label matches above item and description matches", () => {
  const g = dashboard.buildGraph(estate());
  assert.equal(dashboard.search(g, "region")[0].id, "W/Sales.Region");
  assert.deepEqual(dashboard.search(g, "grouped").map((n) => n.id), ["W/Sales.Region"]);
  assert.equal(dashboard.search(g, "landing customer").length, 2);
  assert.deepEqual(dashboard.search(g, ""), []);
});

test("statuses, statistics and columns join to their node by key", () => {
  const g = dashboard.buildGraph(estate());
  const region = g.nodes.get("W/Sales.Region");
  assert.equal(region.result, "Succeeded");
  assert.equal(region.stat.read, 3);
  assert.deepEqual(region.columns, [{ name: "Region ID", note: "The region's key." }]);
  assert.equal(g.nodes.get("W/Sales.ByRegionValid").failures, 3);
});

test("lineage follows the window both ways from a node", () => {
  const g = dashboard.buildGraph(estate());
  const win = dashboard.windowAround(g, "W/Sales.ByRegion", -1, -1);
  const path = dashboard.lineage(win, "M/Sales");
  assert.ok(path.nodes.has("R/Sales") && path.nodes.has("L/Files.Customers"));
  assert.ok(!path.nodes.has("W/Sales.ByRegionValid"));
});

test("findings order errors before warnings before information", () => {
  const data = estate();
  data.loads.push(["W/Sales.Old", "Succeeded", "2026-10-08T00:00:00Z", "2026-10-08T00:00:01Z", 1000]);
  data.views = ["W/Sales.View"];
  data.loads.push(["W/Sales.View", "Succeeded", "2026-10-01T00:00:00Z", "2026-10-01T00:00:01Z", 1]);
  const list = dashboard.findings(data);
  assert.deepEqual(list.map((f) => f.severity), [1, 2, 3]);
  assert.match(list[1].text, /No successful load for \d+ hours/);
});

test("a run's timeline orders its steps and names refreshes by their node", () => {
  const log = [
    ["wf1", "load", "Lakehouse", "Landing", "Tables/Sales", "Customer", "Succeeded", "2026-10-10T04:00:06Z", "2026-10-10T04:00:20Z", ""],
    ["wf1", "load", "Lakehouse", "Landing", "", "", "Succeeded", "2026-10-10T04:00:21Z", "2026-10-10T04:00:22Z", "refresh:Lakehouse/Landing"],
    ["wf0", "load", "Warehouse", "Curated", "Sales", "Region", "Failed", "2026-10-09T04:00:00Z", "2026-10-09T04:00:04Z", ""],
  ];
  assert.deepEqual(dashboard.workflows(log).map((w) => w.id), ["wf1", "wf0"]);
  const model = dashboard.gantt(log, "wf1");
  assert.equal(model.rows.length, 2);
  assert.equal(model.rows[1].label, "Lakehouse/Landing");
  assert.equal(model.rows[1].verb, "refresh");
  assert.equal(model.t1 - model.t0, 16000);
});
