// The declared metadata, indexed once, and the Dictionary page.

function metadata(m) {
  if (m.meta) return m.meta;
  var t = function (name) { return m.tables[name] || []; };
  function keep(r) { return !isInternal(m, r.item, r.schema); }
  var md = { tables: new Map(), folders: new Map(), schemas: [], columns: new Map(), keys: new Map(), validations: new Map(),
    foreignKeys: [], references: new Map(), referencedBy: new Map(), models: new Map(), hasMetadata: !!m.tables.tables, hasSemantic: !!m.tables.models };
  t("tables").filter(keep).forEach(function (r) { md.tables.set(identityKey(r.item, r.schema, r.object), r); });
  t("folders").filter(keep).forEach(function (r) { md.folders.set(identityKey(r.item, r.schema, r.object), r); });
  md.schemas = t("schemas").filter(keep);
  t("columns").filter(keep).forEach(function (r) { addTo(md.columns, identityKey(r.item, r.schema, r.object), r); });
  t("keys").filter(keep).forEach(function (r) { addTo(md.keys, identityKey(r.item, r.schema, r.object), r); });
  t("validations").filter(keep).forEach(function (r) { md.validations.set(identityKey(r.item, r.schema, r.object), r); });
  t("foreignkeys").filter(keep).forEach(function (r) {
    var fk = { from: identityKey(r.item, r.schema, r.object), to: identityKey(r.primaryitem, r.primaryschema, r.primaryobject),
      fromColumns: splitList(r.columns), toColumns: splitList(r.primarycolumns), row: r };
    md.foreignKeys.push(fk);
    addTo(md.references, fk.from, fk); addTo(md.referencedBy, fk.to, fk);
  });
  md.columns.forEach(function (list) { list.sort(function (a, b) { return compare(a.column, b.column); }); });

  function model(item) {
    var it = m.items.get(item);
    if (it && it.internal) return null;
    var mo = md.models.get(item);
    if (!mo) md.models.set(item, mo = { item: item, name: itemName(item), description: null, tables: new Map(), relationships: [], node: null });
    return mo;
  }
  function table(mo, name) {
    var tb = mo.tables.get(name);
    if (!tb) mo.tables.set(name, tb = { model: mo, name: name, ordinal: null, description: null, mode: null, access: null, columns: [], measures: [], sources: [] });
    return tb;
  }
  t("models").forEach(function (r) { var mo = model(r.item); if (mo) mo.description = r.description; });
  t("smtables").forEach(function (r) {
    var mo = model(r.item); if (!mo) return;
    var tb = table(mo, r.table);
    tb.ordinal = num(r.ordinal); tb.description = r.description; tb.mode = r.mode; tb.access = r.access;
  });
  t("smcolumns").forEach(function (r) { var mo = model(r.item); if (mo) table(mo, r.table).columns.push(r); });
  t("smmeasures").forEach(function (r) { var mo = model(r.item); if (mo) table(mo, r.table).measures.push(r); });
  t("smrelationships").forEach(function (r) {
    var mo = model(r.item); if (!mo) return;
    mo.relationships.push({ name: r.name, from: r.fromtable, fromColumn: r.fromcolumn, to: r.totable, toColumn: r.tocolumn,
      fromCardinality: (r.fromcardinality || "many").toLowerCase(), toCardinality: (r.tocardinality || "one").toLowerCase(),
      crossFilter: r.crossfilter, active: r.active !== "0" });
  });
  t("smsources").forEach(function (r) {
    var mo = model(r.item); if (!mo) return;
    table(mo, r.table).sources.push({ reference: r.reference, item: r.sourceitem, schema: r.sourceschema, object: r.sourceobject,
      node: r.sourceitem ? m.byIdentity.get(identityKey(r.sourceitem, r.sourceschema, r.sourceobject)) || null : null });
  });
  md.models.forEach(function (mo) {
    mo.tables.forEach(function (tb) {
      tb.columns.sort(function (a, b) { return compare(a.column, b.column); });
      tb.measures.sort(function (a, b) { return compare(a.measure, b.measure); });
    });
    var root = null;
    m.nodes.forEach(function (n) { if (n.item === mo.item && n.kind === KIND.MODEL) root = n; });
    mo.node = root;
  });
  m.meta = md;
  return md;
}

function splitList(text) { return text ? String(text).split(",").map(function (s) { return s.trim(); }).filter(Boolean) : []; }

/* Tables of a model in authored TMDL order, or alphabetically. */
function modelTables(mo, order) {
  var list = Array.from(mo.tables.values());
  list.sort(order === "alphabetical" || list.some(function (tb) { return tb.ordinal === null; })
    ? function (a, b) { return compare(a.name, b.name) || (a.name < b.name ? -1 : 1); }
    : function (a, b) { return a.ordinal - b.ordinal; });
  return list;
}

/* Every searchable entry: items, objects, declared columns and, for semantic
   models, tables, columns and measures. */
function dictionaryEntries(m) {
  if (m.entries) return m.entries;
  var md = metadata(m), out = [];
  visibleItems(m).forEach(function (it) {
    if (!it.nodes.length && !it.target) return;
    out.push({ kind: "Item", label: it.name, meta: it.type + (it.target && it.target !== it.name ? " · " + it.target : ""), desc: "", item: it.label, ref: it });
  });
  m.nodes.forEach(function (n) {
    out.push({ kind: n.kind, label: n.label, meta: n.kind + " · " + n.item, desc: n.desc, item: n.item, ref: n, qualified: qualified(n) });
  });
  md.columns.forEach(function (list, id) {
    var owner = m.byIdentity.get(id);
    list.forEach(function (c) {
      out.push({ kind: "Column", label: c.column, meta: "Column · " + (owner ? owner.label : c.schema + "." + c.object) + " · " + c.item, desc: c.description || "", item: c.item, ref: c, owner: owner });
    });
  });
  md.models.forEach(function (mo) {
    mo.tables.forEach(function (tb) {
      out.push({ kind: "Semantic table", label: tb.name, meta: "Semantic table · " + mo.name, desc: tb.description || "", item: mo.item, ref: tb });
      tb.columns.forEach(function (c) {
        out.push({ kind: "Semantic column", label: c.column, meta: "Semantic column · " + tb.name + " · " + mo.name, desc: c.description || "", item: mo.item, ref: c, table: tb });
      });
      tb.measures.forEach(function (c) {
        out.push({ kind: "Measure", label: c.measure, meta: "Measure · " + tb.name + " · " + mo.name, desc: c.description || "", item: mo.item, ref: c, table: tb });
      });
    });
  });
  out.forEach(function (e) {
    e.lower = e.label.toLowerCase(); e.descLower = e.desc.toLowerCase(); e.metaLower = e.meta.toLowerCase();
    e.nameLower = e.ref && e.ref.key && e.ref.object ? e.ref.object.toLowerCase() : e.lower;
  });
  m.entries = out;
  return out;
}

function searchEntries(entries, query, kind) {
  var terms = String(query || "").toLowerCase().split(/\s+/).filter(Boolean), found = [];
  entries.forEach(function (e) {
    if (kind && kind !== "All" && e.kind !== kind) return;
    var score = 0;
    for (var i = 0; i < terms.length; i++) {
      var term = terms[i], s = 0;
      if (e.lower === term || e.nameLower === term) s = 100;
      else if (e.lower.indexOf(term) === 0 || e.nameLower.indexOf(term) === 0) s = 80;
      else if ((" " + e.lower.replace(/[._/\-]/g, " ")).indexOf(" " + term) >= 0) s = 60;
      else if (e.lower.indexOf(term) >= 0) s = 40;
      else if (e.metaLower.indexOf(term) >= 0) s = 20;
      else if (e.descLower.indexOf(term) >= 0) s = 10;
      if (!s) return;
      score += s;
    }
    found.push({ e: e, score: terms.length ? score : 0 });
  });
  // With no search, items and objects come before the columns and measures inside them.
  found.sort(function (a, b) {
    return b.score - a.score || entryRank(a.e) - entryRank(b.e) || compare(a.e.label, b.e.label) || compare(a.e.meta, b.e.meta);
  });
  return found.map(function (f) { return f.e; });
}

var ENTRY_RANK = { "Item": 0, "Semantic table": 2, "Column": 3, "Semantic column": 3, "Measure": 3 };

function entryRank(e) { return e.kind in ENTRY_RANK ? ENTRY_RANK[e.kind] : 1; }

var ENTITY_KINDS = ["All", "Item", "Table", "View", "Folder", "Shortcut", "Test", "Assumption", "Column"];
var SEMANTIC_TYPES = ["SemanticModel", "Report"];

/* The meaning of the landed and curated data: the declared relationships
   between Lakehouse and Warehouse objects, and their dictionary. The Semantic
   model page is its counterpart for models. */
function entitiesPage(body, m) {
  var s = pageState("entities"), md = metadata(m);
  s.item = s.item || "all"; s.kind = s.kind || "All"; s.query = s.query || ""; s.er = s.er || {};
  var items = visibleItems(m).filter(function (it) { return SEMANTIC_TYPES.indexOf(it.type) < 0 && (it.nodes.length || it.target); })
    .sort(function (a, b) { return typeRank(a.type) - typeRank(b.type) || compare(a.name, b.name); });
  if (s.item !== "all" && !items.some(function (it) { return it.label === s.item; })) s.item = "all";
  var entries = dictionaryEntries(m).filter(function (e) {
    return ENTITY_KINDS.indexOf(e.kind) > 0 && SEMANTIC_TYPES.indexOf(itemType(e.item)) < 0 && (s.item === "all" || e.item === s.item);
  });
  var counts = {};
  entries.forEach(function (e) { counts[e.kind] = (counts[e.kind] || 0) + 1; });
  var picker = el("select", { "class": "wd-select", "aria-label": "Item", onchange: function () { s.item = this.value; s.selected = null; s.er = {}; saveState(); redraw(); } },
    [el("option", { value: "all", text: "Every Lakehouse and Warehouse" })].concat(items.map(function (it) {
      return el("option", { value: it.label, selected: it.label === s.item ? "selected" : null, text: it.type + " · " + it.name });
    })));
  var input = el("input", { "class": "wd-search wd-search-wide", type: "search", value: s.query, placeholder: "Search " + fmt(entries.length) + " entries: tables, views, folders, columns and descriptions", "aria-label": "Search the dictionary", autocomplete: "off", spellcheck: "false" });
  body.appendChild(el("div", { "class": "wd-toolbar" }, [picker, input]));

  var erHost = el("div");
  body.appendChild(section("Relationships", [erHost]));
  var er = erDiagram(erHost, m, s.er, { item: s.item, onPick: function (id) {
    var entry = entries.filter(function (e) { return e.ref && e.ref.id === id; })[0];
    if (entry) choose(entry, false);
  } });

  var facets = el("div", { "class": "wd-facets", role: "radiogroup", "aria-label": "Entry kind" });
  var list = el("div", { "class": "wd-entry-list", role: "listbox" }), detail = el("div", { "class": "wd-entry-detail" }), note = el("div", { "class": "wd-status-line" });
  body.appendChild(section("Dictionary", [facets, note, el("div", { "class": "wd-dictionary" }, [list, detail])]));
  function renderFacets() {
    facets.textContent = "";
    ENTITY_KINDS.forEach(function (k) {
      if (k !== "All" && !counts[k]) return;
      facets.appendChild(el("button", { type: "button", role: "radio", "class": "wd-facet", "aria-checked": String(s.kind === k),
        onclick: function () { s.kind = k; renderFacets(); renderList(); saveState(); } },
        [k === "All" ? null : kindMark(k), k + " ", el("span", { "class": "wd-facet-count", text: fmt(k === "All" ? entries.length : counts[k]) })]));
    });
  }
  function renderList() {
    var found = searchEntries(entries, s.query, s.kind), shown = found.slice(0, 100);
    list.textContent = "";
    note.textContent = found.length > 100 ? "Showing the first 100 of " + fmt(found.length) + " matches. Refine the search to narrow them." : fmt(found.length) + (found.length === 1 ? " match" : " matches");
    shown.forEach(function (e) {
      list.appendChild(el("button", { type: "button", role: "option", "class": "wd-entry", "aria-selected": String(s.selected === entryId(e)),
        onclick: function () { choose(e, true); } }, [
        el("span", { "class": "wd-entry-label" }, [kindMark(e.kind), e.label]),
        el("span", { "class": "wd-entry-meta", text: e.meta }),
        e.desc ? el("span", { "class": "wd-entry-desc", text: e.desc }) : null
      ]));
    });
    if (!shown.length) list.appendChild(empty("Nothing matches. Try fewer words, or another kind."));
  }
  /* An entry chosen here shows in the diagram too, when it has relationships. */
  function choose(e, focusDiagram) {
    s.selected = entryId(e);
    renderList();
    renderDetail(e);
    if (focusDiagram && e.ref && e.ref.id && er.has && er.has(e.ref.id)) er.focus(e.ref.id);
    saveState();
  }
  function renderDetail(e) { entryDetail(detail, m, md, e, function (other) { choose(other, true); }); }
  input.addEventListener("input", function () { s.query = input.value; renderList(); });
  renderFacets(); renderList();
  var current = entries.filter(function (e) { return entryId(e) === s.selected; })[0];
  if (current) renderDetail(current);
  else detail.appendChild(empty("Select an entry to see its definition, keys, columns and lineage."));
}

function entryId(e) {
  if (e.kind === "Item") return "item|" + e.item;
  if (e.ref && e.ref.key) return "node|" + e.ref.key;
  if (e.kind === "Column") return "col|" + identityKey(e.ref.item, e.ref.schema, e.ref.object) + "|" + e.ref.column;
  if (e.kind === "Semantic table") return "smt|" + e.item + "|" + e.ref.name;
  return e.kind + "|" + e.item + "|" + (e.table ? e.table.name : "") + "|" + e.label;
}

function entryDetail(panel, m, md, e, select) {
  panel.textContent = "";
  var entries = dictionaryEntries(m);
  function find(kind, test) { return entries.filter(function (x) { return x.kind === kind && test(x); })[0]; }
  function openNode(key) {
    var x = entries.filter(function (y) { return y.ref && y.ref.key === key; })[0];
    if (x) select(x);
  }
  if (e.kind === "Item") {
    var it = e.ref;
    panel.appendChild(el("div", { "class": "wd-eyebrow", text: it.type }));
    panel.appendChild(el("h2", { "class": "wd-detail-title", text: it.name }));
    var facts = el("dl", { "class": "wd-facts" });
    [["Fabric item", it.target], ["Workspace", it.workspace], ["Weaver version", it.version], ["Objects", fmt(it.nodes.length)]].forEach(function (f) {
      if (f[1]) { facts.appendChild(el("dt", { text: f[0] })); facts.appendChild(el("dd", { text: f[1] })); }
    });
    panel.appendChild(facts);
    var schemas = md.schemas.filter(function (r) { return r.item === it.label; });
    if (schemas.length) {
      panel.appendChild(el("h3", { "class": "wd-h3", text: "Schemas" }));
      panel.appendChild(el("dl", { "class": "wd-columns" }, schemas.reduce(function (a, r) { return a.concat([el("dt", { text: r.schema }), el("dd", { text: r.description || "" })]); }, [])));
    }
    panel.appendChild(el("h3", { "class": "wd-h3", text: "Objects" }));
    panel.appendChild(el("ul", { "class": "wd-list" }, it.nodes.slice(0, 200).map(function (n) { return el("li", {}, [nodeLink(m, n.key, openNode)]); })));
    return;
  }
  if (e.ref && e.ref.key) {
    var n = e.ref;
    details(panel, m, n.key, { onOpen: openNode, columns: md.columns });
    var declared = md.tables.get(n.id) || md.folders.get(n.id), validation = md.validations.get(n.id);
    if (declared) {
      var facts2 = el("dl", { "class": "wd-facts" });
      [["Primary key", declared.primarykey], ["Not null", declared.notnull], ["Identity column", declared.identity], ["File key", declared.filekey],
        ["Incremental", declared.incremental ? "Yes" : null], ["Prohibit rebuild", declared.prohibitrebuild ? "Yes" : null]].forEach(function (f) {
        if (f[1]) { facts2.appendChild(el("dt", { text: f[0] })); facts2.appendChild(el("dd", { "class": "wd-mono", text: f[1] })); }
      });
      panel.insertBefore(facts2, panel.querySelector(".wd-h3"));
      if (declared.lineage) panel.insertBefore(el("p", { "class": "wd-lineage-text" }, [el("b", { text: "Lineage. " }), declared.lineage]), facts2.nextSibling);
    }
    if (validation && validation.primarykey) panel.appendChild(el("p", { "class": "wd-muted", text: "Correlation key: " + validation.primarykey }));
    var keys = md.keys.get(n.id) || [], refs = md.references.get(n.id) || [], back = md.referencedBy.get(n.id) || [];
    if (keys.length || refs.length || back.length) {
      panel.appendChild(el("h3", { "class": "wd-h3", text: "Keys and relationships" }));
      panel.appendChild(el("ul", { "class": "wd-list wd-mono" }, keys.map(function (k) { return el("li", { text: k.type + " (" + k.columns + ")" }); })
        .concat(refs.map(function (r) { return el("li", { text: "(" + r.fromColumns.join(", ") + ") → " + r.to.split("|").slice(1).join(".") + " (" + r.toColumns.join(", ") + ")" }); }))
        .concat(back.map(function (r) { return el("li", { text: r.from.split("|").slice(1).join(".") + " (" + r.fromColumns.join(", ") + ") → (" + r.toColumns.join(", ") + ")" }); }))));
    }
    if (n.kind === KIND.MODEL) {
      var mo = md.models.get(n.item);
      if (mo) semanticSummary(panel, m, mo, function (tb) { var x = find("Semantic table", function (y) { return y.ref === tb; }); if (x) select(x); });
    }
    return;
  }
  if (e.kind === "Column") {
    panel.appendChild(el("div", { "class": "wd-eyebrow", text: "Declared column" }));
    panel.appendChild(el("h2", { "class": "wd-detail-title", text: e.ref.column }));
    panel.appendChild(el("p", { "class": e.ref.description ? "wd-desc" : "wd-desc wd-muted", text: e.ref.description || "No description is declared." }));
    if (e.ref.identity) panel.appendChild(el("p", { "class": "wd-muted", text: "Weaver's managed surrogate column." }));
    if (e.owner) panel.appendChild(el("p", {}, ["Of ", nodeLink(m, e.owner.key, openNode)]));
    panel.appendChild(el("p", { "class": "wd-note", text: "The dictionary describes declared columns; it does not inventory every physical column." }));
    return;
  }
  var tb = e.kind === "Semantic table" ? e.ref : e.table;
  if (e.kind === "Semantic column" || e.kind === "Measure") {
    panel.appendChild(el("div", { "class": "wd-eyebrow", text: e.kind }));
    panel.appendChild(el("h2", { "class": "wd-detail-title", text: e.label }));
    panel.appendChild(el("div", { "class": "wd-muted", text: tb.name + " · " + tb.model.name }));
    panel.appendChild(el("p", { "class": e.ref.description ? "wd-desc" : "wd-desc wd-muted", text: e.ref.description || "No description." }));
    var f3 = el("dl", { "class": "wd-facts" });
    [["Data type", e.ref.datatype], ["Kind", e.ref.columntype], ["Source column", e.ref.sourcecolumn], ["Format", e.ref.format]].forEach(function (f) {
      if (f[1]) { f3.appendChild(el("dt", { text: f[0] })); f3.appendChild(el("dd", { text: f[1] })); }
    });
    panel.appendChild(f3);
    if (e.ref.expression) panel.appendChild(el("pre", { "class": "wd-code", text: e.ref.expression }));
    return;
  }
  semanticTableDetail(panel, m, tb, openNode);
}
