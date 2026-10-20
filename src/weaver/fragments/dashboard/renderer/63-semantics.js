// Semantic models and the bus matrix.

function modelList(m) {
  return Array.from(metadata(m).models.values()).sort(function (a, b) { return compare(a.name, b.name); });
}

function semanticSummary(panel, m, mo, onTable) {
  panel.appendChild(el("h3", { "class": "wd-h3", text: "Tables · " + mo.tables.size }));
  panel.appendChild(el("ul", { "class": "wd-list" }, modelTables(mo, "authored").map(function (tb) {
    return el("li", {}, [el("button", { "class": "wd-link", type: "button", onclick: function () { onTable(tb); } }, [kindMark("Semantic table"), tb.name]),
      el("span", { "class": "wd-grow" }), el("span", { "class": "wd-muted", text: tb.mode || "" })]);
  })));
}

function semanticTableDetail(panel, m, tb, openNode) {
  panel.appendChild(el("div", { "class": "wd-eyebrow" }, [kindMark("Semantic table"), " Semantic table"]));
  panel.appendChild(el("h2", { "class": "wd-detail-title", text: tb.name }));
  panel.appendChild(el("div", { "class": "wd-muted", text: tb.model.name + (tb.ordinal !== null ? " · table " + tb.ordinal + " in authored order" : "") }));
  panel.appendChild(el("p", { "class": tb.description ? "wd-desc" : "wd-desc wd-muted", text: tb.description || "No description." }));
  var facts = el("dl", { "class": "wd-facts" });
  [["Storage mode", tb.mode], ["Source access", tb.access]].forEach(function (f) {
    if (f[1]) { facts.appendChild(el("dt", { text: f[0] })); facts.appendChild(el("dd", { text: f[1] })); }
  });
  panel.appendChild(facts);
  if (tb.sources.length) {
    panel.appendChild(el("h3", { "class": "wd-h3", text: "Reads" }));
    panel.appendChild(el("ul", { "class": "wd-list" }, tb.sources.map(function (src) {
      return el("li", {}, [src.node && openNode ? nodeLink(m, src.node.key, openNode) : el("span", { "class": "wd-mono", text: src.reference })]);
    })));
  }
  var rels = tb.model.relationships.filter(function (r) { return r.from === tb.name || r.to === tb.name; });
  if (rels.length) {
    panel.appendChild(el("h3", { "class": "wd-h3", text: "Relationships · " + rels.length }));
    panel.appendChild(el("ul", { "class": "wd-list wd-mono" }, rels.map(relationshipLine)));
  }
  if (tb.columns.length) {
    panel.appendChild(el("h3", { "class": "wd-h3", text: "Columns · " + tb.columns.length }));
    panel.appendChild(dataTable([
      { key: "column", title: "Column", value: function (c) { return c.column; } },
      { key: "datatype", title: "Type", value: function (c) { return c.datatype || ""; } },
      { key: "description", title: "Description", value: function (c) { return c.description || ""; } }
    ], tb.columns, { limit: 300, sort: null }));
  }
  if (tb.measures.length) {
    panel.appendChild(el("h3", { "class": "wd-h3", text: "Measures · " + tb.measures.length }));
    tb.measures.forEach(function (ms) {
      panel.appendChild(el("details", { "class": "wd-measure" }, [
        el("summary", {}, [el("b", { text: ms.measure }), ms.description ? el("span", { "class": "wd-muted", text: " · " + ms.description }) : null]),
        ms.expression ? el("pre", { "class": "wd-code", text: ms.expression }) : null,
        ms.format ? el("div", { "class": "wd-muted", text: "Format " + ms.format }) : null
      ]));
    });
  }
}

function relationshipLine(r) {
  return el("li", { "class": r.active ? null : "wd-inactive" }, [
    r.from + "[" + (r.fromColumn || "") + "]  " + notation({ r: r, factIsFrom: true }, true) + "  " + r.to + "[" + (r.toColumn || "") + "]" + (r.active ? "" : "  inactive")
  ]);
}

/* ---- bus matrix -------------------------------------------------------------- */

/* Fact-side tables against dimension-side tables, from a model's direct
   relationships. The one side of a one-to-many relationship is a dimension and
   its many side a fact. A one-to-one or many-to-many relationship takes the
   roles its tables already have, or its from and to tables when they have none;
   it is listed apart only when both its tables are on the same side. */
function busMatrix(mo) {
  var facts = new Map(), dims = new Map(), cells = new Map(), other = [], later = [];
  function place(fact, dim, r) {
    facts.set(fact, true); dims.set(dim, true);
    var key = fact + "\u0001" + dim, cell = cells.get(key);
    if (!cell) cells.set(key, cell = { active: 0, inactive: 0, relationships: [] });
    cell[r.active ? "active" : "inactive"] += 1;
    cell.relationships.push({ r: r, factIsFrom: r.from === fact });
  }
  mo.relationships.forEach(function (r) {
    if (r.fromCardinality === "many" && r.toCardinality === "one") place(r.from, r.to, r);
    else if (r.fromCardinality === "one" && r.toCardinality === "many") place(r.to, r.from, r);
    else later.push(r);
  });
  function canBeFact(name) { return facts.has(name) || !dims.has(name); }
  function canBeDim(name) { return dims.has(name) || !facts.has(name); }
  later.forEach(function (r) {
    if (canBeFact(r.from) && canBeDim(r.to)) place(r.from, r.to, r);
    else if (canBeFact(r.to) && canBeDim(r.from)) place(r.to, r.from, r);
    else other.push(r);
  });
  var related = new Set();
  mo.relationships.forEach(function (r) { related.add(r.from); related.add(r.to); });
  var isolated = Array.from(mo.tables.keys()).filter(function (name) { return !related.has(name); });
  return { facts: facts, dims: dims, cells: cells, other: other, isolated: isolated, notation: notation,
    mixed: Array.from(facts.keys()).filter(function (name) { return dims.has(name); }) };
}

/* A relationship as the matrix writes it, read from the row table to the
   column table: each side's cardinality, and the arrow the filter travels. */
function notation(entry, rowIsFact) {
  var r = entry.r, rowIsFrom = entry.factIsFrom === rowIsFact, row = rowIsFrom ? r.from : r.to;
  var card = function (c) { return c === "many" ? "*" : "1"; };
  var left = card(rowIsFrom ? r.fromCardinality : r.toCardinality), right = card(rowIsFrom ? r.toCardinality : r.fromCardinality);
  // One way, a filter runs from the one side to the many side; between two
  // tables of equal cardinality, from the to table to the from table.
  var filtered = r.fromCardinality !== r.toCardinality ? (r.fromCardinality === "many" ? r.from : r.to) : r.from;
  var arrow = /both/i.test(r.crossFilter || "") ? "↔" : filtered === row ? "←" : "→";
  return left + " " + arrow + " " + right;
}

/* The fact × dimension matrix of one model, read from row to column. */
function busMatrixView(mo, s) {
  var host = el("div"), bus = busMatrix(mo), order = modelTables(mo, s.order).map(function (tb) { return tb.name; });
  function sorted(map) { return order.filter(function (n) { return map.has(n); }).concat(Array.from(map.keys()).filter(function (n) { return order.indexOf(n) < 0; }).sort(compare)); }
  var factsAreRows = s.dims === "columns";
  var rows = sorted(factsAreRows ? bus.facts : bus.dims), cols = sorted(factsAreRows ? bus.dims : bus.facts);
  if (!rows.length) { host.appendChild(empty("This model records no relationship.")); }
  else {
    var mixedMark = function (name) { return bus.mixed.indexOf(name) >= 0 ? el("i", { "class": "wd-bus-mixed", title: "On the fact side of one relationship and the dimension side of another", text: " ◐" }) : null; };
    host.appendChild(el("div", { "class": "wd-table-wrap wd-bus-wrap" }, [el("table", { "class": "wd-bus" }, [
      el("thead", {}, [el("tr", {}, [el("th", { "class": "wd-bus-corner", text: factsAreRows ? "Fact ↓   Dimension →" : "Dimension ↓   Fact →" })].concat(cols.map(function (c) {
        return el("th", { scope: "col", "class": "wd-bus-dim" }, [el("span", {}, [c, mixedMark(c)])]);
      })))]),
      el("tbody", {}, rows.map(function (row) {
        return el("tr", {}, [el("th", { scope: "row" }, [row, mixedMark(row)])].concat(cols.map(function (col) {
          var cell = bus.cells.get(factsAreRows ? row + "\u0001" + col : col + "\u0001" + row);
          if (!cell) return el("td", { "class": "wd-bus-empty" });
          var shown = cell.relationships.filter(function (e) { return e.r.active; })[0] || cell.relationships[0], count = cell.relationships.length;
          return el("td", { "class": "wd-bus-cell" + (cell.active ? "" : " wd-bus-inactive"),
            title: cell.relationships.map(function (e) { return notation(e, factsAreRows) + "  " + e.r.from + "[" + e.r.fromColumn + "] → " + e.r.to + "[" + e.r.toColumn + "]" + (e.r.active ? "" : " (inactive)"); }).join("\n") },
            [notation(shown, factsAreRows), count > 1 ? el("sup", { text: String(count) }) : null]);
        })));
      }))
    ])]));
    host.appendChild(el("div", { "class": "wd-legend" }, [
      el("span", { "class": "wd-key", text: "* ← 1  many to one" }), el("span", { "class": "wd-key", text: "* ← *  many to many" }),
      el("span", { "class": "wd-key", text: "↔  filters both ways" }), el("span", { "class": "wd-key wd-muted", text: "grey: inactive only" }),
      el("span", { "class": "wd-key", text: "²  relationships between the pair" }), el("span", { "class": "wd-key", text: "◐ on both sides" }),
      el("span", { "class": "wd-key", text: "Read from the row table to the column table. Cells show direct relationships, not filter propagation; an empty cell records none." })
    ]));
  }
  if (bus.other.length) {
    host.appendChild(el("h3", { "class": "wd-h3", text: "Between two tables on the same side · " + bus.other.length }));
    host.appendChild(el("ul", { "class": "wd-list wd-mono" }, bus.other.map(relationshipLine)));
  }
  if (bus.isolated.length) {
    host.appendChild(el("h3", { "class": "wd-h3", text: "Tables with no relationship · " + bus.isolated.length }));
    host.appendChild(el("p", { "class": "wd-muted", text: order.filter(function (n) { return bus.isolated.indexOf(n) >= 0; }).join(", ") }));
  }
  if (mo.relationships.length) {
    host.appendChild(el("details", { "class": "wd-more-detail" }, [el("summary", { text: "Every relationship · " + mo.relationships.length }),
      el("ul", { "class": "wd-list wd-mono" }, mo.relationships.slice().sort(function (a, b) { return compare(a.from, b.from) || compare(a.to, b.to); }).map(relationshipLine))]));
  }
  return host;
}

/* One model, chosen at the top: what it is, how its tables relate, its tables,
   columns and measures, and the managed sources they read. The Entity
   dictionary page is its counterpart for Lakehouses and Warehouses. */
function semanticPage(body, m) {
  var s = pageState("semantic"), models = modelList(m);
  s.order = s.order || "authored"; s.dims = s.dims || "columns"; s.query = s.query || "";
  if (!models.length) { body.appendChild(empty("No semantic model is recorded in the catalogue.")); return; }
  if (!s.model || !models.some(function (mo) { return mo.item === s.model; })) s.model = models[0].item;
  var mo = metadata(m).models.get(s.model);
  var picker = el("select", { "class": "wd-select wd-select-strong", "aria-label": "Semantic model", onchange: function () { s.model = this.value; s.table = null; saveState(); redraw(); } },
    models.map(function (x) { return el("option", { value: x.item, selected: x.item === s.model ? "selected" : null, text: x.name }); }));
  var input = el("input", { "class": "wd-search", type: "search", value: s.query, placeholder: "Search this model's tables, columns and measures", "aria-label": "Search the model", autocomplete: "off", spellcheck: "false" });
  body.appendChild(el("div", { "class": "wd-toolbar" }, [picker, el("div", { "class": "wd-searchbox" }, [input])]));

  // What the model is.
  var refresh = mo.node ? effectiveLoad(mo.node) : null, it = m.items.get(mo.item), measureCount = 0, columnCount = 0;
  mo.tables.forEach(function (tb) { measureCount += tb.measures.length; columnCount += tb.columns.length; });
  var facts = el("dl", { "class": "wd-facts wd-facts-wide" });
  function fact(label, value) { facts.appendChild(el("dt", { text: label })); facts.appendChild(el("dd", {}, [value])); }
  fact("Last refresh", refresh ? el("span", {}, [chip(refresh.result), " " + stamp(refresh.completed)]) : el("span", { "class": "wd-muted", text: "Nothing has been processed since it was built" }));
  if (mo.node && mo.node.built) fact("Built", stamp(mo.node.built));
  if (it && it.target) fact("Fabric item", it.target);
  fact("Contents", fmt(mo.tables.size) + " tables · " + fmt(columnCount) + " columns · " + fmt(measureCount) + " measures · " + fmt(mo.relationships.length) + " relationships");
  var validations = it ? it.nodes.filter(isValidation) : [];
  body.appendChild(el("div", { "class": "wd-model-head" }, [
    el("div", {}, [el("div", { "class": "wd-eyebrow", text: "Semantic model" }), el("h2", { "class": "wd-detail-title", text: mo.name }),
      el("p", { "class": mo.description ? "wd-desc" : "wd-desc wd-muted", text: mo.description || "No description." }), facts]),
    validations.length ? el("div", {}, [el("h3", { "class": "wd-h3", text: "Tests and Assumptions · " + validations.length }),
      el("ul", { "class": "wd-list" }, validations.map(function (v) {
        return el("li", {}, [kindMark(v.kind), v.label, el("span", { "class": "wd-grow" }), chip(v.test ? v.test.result : null, v.test ? null : "Not run"),
          v.test && v.test.failures ? el("span", { "class": "wd-muted", text: " " + fmt(v.test.failures) + " rows" }) : null]);
      }))]) : null
  ]));

  // How its tables relate.
  var busHost = el("div");
  function renderBus() { busHost.textContent = ""; busHost.appendChild(busMatrixView(mo, s)); }
  body.appendChild(section("Relationships", [el("div", { "class": "wd-toolbar" }, [
    segmented("Dimensions", ["columns", "rows"], s.dims, function (v) { s.dims = v; renderBus(); saveState(); }, function (v) { return v === "columns" ? "Columns" : "Rows"; }),
    segmented("Order", ["authored", "alphabetical"], s.order, function (v) { s.order = v; renderBus(); renderTables(); saveState(); }, function (v) { return v === "authored" ? "Authored" : "A–Z"; })
  ]), busHost]));

  // Its tables and their columns.
  var tablesHost = el("div"), detail = el("div", { "class": "wd-entry-detail" }), measuresHost = el("div");
  function matches(text) { var q = s.query.toLowerCase().trim(); return !q || String(text || "").toLowerCase().indexOf(q) >= 0; }
  function tableMatches(tb) {
    return matches(tb.name) || matches(tb.description) || tb.columns.some(function (c) { return matches(c.column) || matches(c.description); }) ||
      tb.measures.some(function (x) { return matches(x.measure) || matches(x.description); });
  }
  function renderTables() {
    tablesHost.textContent = "";
    var tables = modelTables(mo, s.order).filter(tableMatches);
    if (s.order === "authored" && tables.some(function (tb) { return tb.ordinal === null; })) {
      tablesHost.appendChild(el("p", { "class": "wd-note", text: "This model was read back without an authored table order, so its tables are listed alphabetically. Build it again to record the order." }));
    }
    tablesHost.appendChild(dataTable([
      { key: "ordinal", title: "#", value: function (tb) { return tb.ordinal; }, numeric: true, sortable: false },
      { key: "name", title: "Table", value: function (tb) { return tb.name; }, sortable: false },
      { key: "mode", title: "Mode", value: function (tb) { return tb.mode || ""; }, sortable: false },
      { key: "columns", title: "Columns", value: function (tb) { return tb.columns.length; }, numeric: true, sortable: false },
      { key: "measures", title: "Measures", value: function (tb) { return tb.measures.length; }, numeric: true, sortable: false },
      { key: "reads", title: "Reads", value: function (tb) { return tb.sources.map(function (src) { return src.node ? src.node.label : src.reference; }).join(", "); }, sortable: false }
    ], tables, { limit: 500, onRow: function (tb) { s.table = tb.name; showTable(tb); saveState(); }, emptyText: "No table matches." }));
  }
  function showTable(tb) { detail.textContent = ""; semanticTableDetail(detail, m, tb, null); }
  body.appendChild(section("Tables", [el("div", { "class": "wd-semantic-grid" }, [tablesHost, detail])]));
  var chosen = s.table && mo.tables.get(s.table);
  if (chosen) showTable(chosen); else detail.appendChild(empty("Select a table to see its columns, measures and relationships."));

  // Every measure, wherever it lives.
  function renderMeasures() {
    measuresHost.textContent = "";
    var all = [];
    mo.tables.forEach(function (tb) { tb.measures.forEach(function (x) { if (matches(x.measure) || matches(x.description) || matches(tb.name)) all.push({ table: tb.name, x: x }); }); });
    if (!all.length) { measuresHost.appendChild(empty(measureCount ? "No measure matches." : "This model has no measures.")); return; }
    all.sort(function (a, b) { return compare(a.x.measure, b.x.measure); });
    measuresHost.appendChild(el("div", { "class": "wd-measures" }, all.map(function (e) {
      return el("details", { "class": "wd-measure" }, [
        el("summary", {}, [el("b", { text: e.x.measure }), el("span", { "class": "wd-muted", text: " · " + e.table }),
          e.x.description ? el("span", { "class": "wd-measure-desc", text: e.x.description }) : null]),
        e.x.expression ? el("pre", { "class": "wd-code", text: e.x.expression }) : null,
        e.x.format ? el("div", { "class": "wd-muted", text: "Format " + e.x.format }) : null
      ]);
    })));
  }
  body.appendChild(section("Measures · " + fmt(measureCount), [measuresHost]));

  // The managed sources the model's tables read, and which other models read them.
  body.appendChild(section("Sources", [sourceReuse(m, models, false, mo.item)]));
  input.addEventListener("input", function () { s.query = input.value; renderTables(); renderMeasures(); });
  renderBus(); renderTables(); renderMeasures();
}

/* Logical sources against the models whose tables read them. */
function sourceReuse(m, models, modelsAsRows, only) {
  var sources = new Map();
  models.forEach(function (mo) {
    mo.tables.forEach(function (tb) {
      tb.sources.forEach(function (src) {
        var key = src.node ? src.node.key : src.reference, entry = sources.get(key);
        if (!entry) sources.set(key, entry = { label: src.node ? src.node.label : src.reference, item: src.item || "", by: new Map() });
        addTo(entry.by, mo.item, tb.name);
      });
    });
  });
  if (!sources.size) return empty("No semantic table records a managed source.");
  // With a model named, only the sources that model reads.
  var list = Array.from(sources.values()).filter(function (src) { return !only || src.by.has(only); })
    .sort(function (a, b) { return compare(a.item, b.item) || compare(a.label, b.label); });
  if (!list.length) return empty("No table of this model records a managed source.");
  var rows = modelsAsRows ? models : list, cols = modelsAsRows ? list : models;
  function name(x) { return x.name !== undefined ? [x.name] : [x.label, el("span", { "class": "wd-muted", text: " " + itemName(x.item) })]; }
  function tables(row, col) { var src = modelsAsRows ? col : row, mo = modelsAsRows ? row : col; return src.by.get(mo.item); }
  return el("div", { "class": "wd-table-wrap wd-bus-wrap" }, [el("table", { "class": "wd-bus" }, [
    el("thead", {}, [el("tr", {}, [el("th", { "class": "wd-bus-corner", text: modelsAsRows ? "Model ↓   Source →" : "Source ↓   Model →" })].concat(cols.map(function (c) {
      return el("th", { scope: "col", "class": "wd-bus-dim" }, [el("span", {}, name(c))]);
    })))]),
    el("tbody", {}, rows.map(function (row) {
      return el("tr", {}, [el("th", { scope: "row" }, name(row))].concat(cols.map(function (col) {
        var found = tables(row, col);
        return found ? el("td", { "class": "wd-bus-cell", title: found.join(", ") }, [found.length > 1 ? String(found.length) : "✓"]) : el("td", { "class": "wd-bus-empty" });
      })));
    }))
  ])]);
}
