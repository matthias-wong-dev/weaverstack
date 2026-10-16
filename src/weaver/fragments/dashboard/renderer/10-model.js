// The Catalogue Dashboard renderer. Every script in this folder is concatenated
// in name order inside one function, so they share its scope. They run inside
// HTML Content (Regular) and, for their tests, under Node.

// The page measures carry delimited text: a table starts with TABLE, its name
// and column names; each row starts with ROW; FIELD separates fields. Every <
// arrives as LESS so that no catalogue text can close the data element.
var FIELD = "", ROW = "", TABLE = "", LESS = "";

var KIND = {
  TABLE: "Table", VIEW: "View", FOLDER: "Folder", SHORTCUT: "Shortcut",
  TEST: "Test", ASSUMPTION: "Assumption", MODEL: "Semantic model", REPORT: "Report"
};
var CATALOGUE_ITEM = "Warehouse/_weaver";
// The Dashboard's own logical items, whatever Fabric items they deploy to.
var DASHBOARD_ITEMS = ["SemanticModel/Catalogue Dashboard", "Report/Catalogue Dashboard"];
var ITEM_TYPES = ["Lakehouse", "Warehouse", "SQLDatabase", "SemanticModel", "Report"];
// Edge kinds travel as their first letter.
var EDGE_KINDS = { D: "Dependency", S: "Shortcut", V: "Validation", E: "External" };

function parsePayload(text) {
  var tables = {}, problems = [];
  String(text || "").split(LESS).join("<").split(TABLE).forEach(function (chunk) {
    if (!chunk) return;
    var rows = chunk.split(ROW), head = rows[0].split(FIELD), name = head[0], cols = head.slice(1), out = [];
    for (var r = 1; r < rows.length; r++) {
      var fields = rows[r].split(FIELD);
      if (fields.length !== cols.length) { problems.push(name + " row " + r + " has " + fields.length + " fields, not " + cols.length); continue; }
      var row = {};
      for (var c = 0; c < cols.length; c++) row[cols[c]] = fields[c] === "" ? null : fields[c];
      out.push(row);
    }
    tables[name] = out;
  });
  return { tables: tables, problems: problems };
}

function num(value) {
  if (value === null || value === undefined || value === "") return null;
  var n = Number(value);
  return isNaN(n) ? null : n;
}

function identityKey(item, schema, object) { return item + "|" + (schema || "") + "|" + (object || ""); }

function isValidation(node) { return node.kind === KIND.TEST || node.kind === KIND.ASSUMPTION; }

function itemType(label) { return String(label).split("/")[0]; }
function itemName(label) { var s = String(label); return s.slice(s.indexOf("/") + 1); }

function typeRank(type) { var i = ITEM_TYPES.indexOf(type); return i < 0 ? ITEM_TYPES.length : i; }

function compare(a, b) {
  a = String(a === null || a === undefined ? "" : a).toLowerCase();
  b = String(b === null || b === undefined ? "" : b).toLowerCase();
  return a < b ? -1 : a > b ? 1 : 0;
}

function addTo(map, key, value) { var list = map.get(key); if (!list) map.set(key, list = []); list.push(value); }

/* The estate one payload describes: items, nodes with their current state,
   the authoritative edges Health walks and the presentation edges the graph
   draws, where a read through a shortcut is drawn from the shortcut. */
function buildModel(tables, problems) {
  var t = function (name) { return tables[name] || []; };
  var meta = t("meta")[0] || {}, page = t("page")[0] || {};
  var m = {
    page: page.name || "overview", at: num(meta.at) || Date.now(), tables: tables,
    problems: (problems || []).slice(), items: new Map(), nodes: new Map(), byIdentity: new Map(),
    internalItems: new Set(), edgeCount: 0, hiddenEdges: 0, externalCount: 0
  };
  t("internal").forEach(function (r) { m.internalItems.add(r.item); });
  function item(label) {
    var it = m.items.get(label);
    if (!it) m.items.set(label, it = { label: label, type: itemType(label), name: itemName(label), workspace: null, target: null, version: null, nodes: [], internal: m.internalItems.has(label) || label === CATALOGUE_ITEM || DASHBOARD_ITEMS.indexOf(label) >= 0 });
    return it;
  }
  t("items").forEach(function (r) {
    var it = item(r.item);
    it.workspace = r.workspace; it.target = r.target; it.version = r.version;
  });
  var catalogue = m.items.get(CATALOGUE_ITEM);
  m.catalogue = { name: catalogue ? catalogue.target : null, workspace: catalogue ? catalogue.workspace : null, version: catalogue ? catalogue.version : null };

  t("nodes").forEach(function (r) {
    if (m.nodes.has(r.key)) { m.problems.push("Two graph nodes share the key " + r.key + "; one of them is not shown."); return; }
    var flags = num(r.flags) || 0;
    var n = {
      key: r.key, item: r.item, schema: r.schema, object: r.object, kind: r.kind, label: r.label || r.object || r.item,
      desc: r.description || "", canLoad: !!(flags & 1), isStatic: !!(flags & 2), isMirrored: !!(flags & 4), isInstalled: !!(flags & 8),
      id: identityKey(r.item, r.schema, r.object), load: null, test: null, built: null,
      up: [], down: [], vup: [], vdown: [], stubs: [], kinds: {}
    };
    m.nodes.set(n.key, n);
    m.byIdentity.set(n.id, n);
    item(n.item).nodes.push(n);
  });

  var seen = new Set(), vseen = new Set(), edges = t("edges");
  edges.forEach(function (r) {
    var d = m.nodes.get(r.down), kind = EDGE_KINDS[r.kind] || r.kind;
    if (!d) return;
    if (kind === "External") { d.stubs.push(r.reference || r.up); m.externalCount += 1; return; }
    var u = m.nodes.get(r.up);
    if (!u) { m.hiddenEdges += 1; return; }
    var key = u.key + ">" + d.key;
    if (!seen.has(key)) { seen.add(key); u.down.push(d.key); d.up.push(u.key); m.edgeCount += 1; }
    var from = r.through && m.nodes.has(r.through) && r.through !== d.key ? m.nodes.get(r.through) : u;
    var vkey = from.key + ">" + d.key;
    if (!vseen.has(vkey)) {
      vseen.add(vkey);
      from.vdown.push(d.key); d.vup.push(from.key);
      d.kinds[from.key] = from === u ? kind : "Dependency";
    }
  });

  t("loads").forEach(function (r) {
    var n = m.byIdentity.get(identityKey(r.item, r.schema, r.object));
    if (n) n.load = status(r);
  });
  t("tests").forEach(function (r) {
    var n = m.byIdentity.get(identityKey(r.item, r.schema, r.object));
    if (n) { n.test = status(r); n.test.failures = num(r.failures); }
  });
  t("built").forEach(function (r) {
    var n = m.byIdentity.get(identityKey(r.item, r.schema, r.object));
    if (n) n.built = num(r.built);
  });
  m.items.forEach(function (it) {
    it.nodes.sort(function (a, b) { return compare(a.schema, b.schema) || compare(a.label, b.label) || compare(a.key, b.key); });
  });
  m.workflows = t("workflows").map(function (r) {
    return { id: r.workflow, started: num(r.started), completed: num(r.completed), records: num(r.records) || 0, failed: num(r.failed) || 0, tasks: r.tasks ? r.tasks.split(",") : [] };
  });
  m.workflowCount = num((t("workflowcount")[0] || {}).count);
  return m;
}

function status(r) {
  return { result: r.result, workflow: r.workflow, started: num(r.started), completed: num(r.completed), duration: num(r.duration) };
}

/* The catalogue, the Dashboard and the `_` surface the catalogue presents in
   every item are Weaver's own; no page reports them as part of the estate. */
var INTERNAL_SCHEMA = /^(?:Files\/|Tables\/)?_$/;

function isInternal(m, item, schema) {
  var it = m.items.get(item);
  return !!(it && it.internal) || item === CATALOGUE_ITEM || DASHBOARD_ITEMS.indexOf(item) >= 0 || INTERNAL_SCHEMA.test(schema || "");
}

function visibleItems(m) {
  return Array.from(m.items.values()).filter(function (it) { return !it.internal; });
}

/* Shown names: a Lakehouse folder keeps its Files path. */
function qualified(node) {
  if (!node.schema && !node.object) return node.item;
  if (!node.object) return node.schema;
  return node.schema + (String(node.schema).indexOf("Files") === 0 ? "/" : ".") + node.object;
}
