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
  var card = (r.fromCardinality === "many" ? "*" : "1") + ":" + (r.toCardinality === "many" ? "*" : "1");
  return el("li", { "class": r.active ? null : "wd-inactive" }, [
    r.from + "[" + (r.fromColumn || "") + "] → " + r.to + "[" + (r.toColumn || "") + "]  " + card +
    (r.crossFilter && /both/i.test(r.crossFilter) ? "  both directions" : "") + (r.active ? "" : "  inactive")
  ]);
}

function semanticsPage(body, m) {
  var s = pageState("semantics"), models = modelList(m);
  s.order = s.order || "authored";
  if (!models.length) { body.appendChild(empty("No semantic model is recorded in the catalogue.")); return; }
  if (!s.model || !models.some(function (mo) { return mo.item === s.model; })) s.model = models[0].item;
  var nav = el("nav", { "class": "wd-model-nav", "aria-label": "Semantic models" }), main = el("div", { "class": "wd-model-main" });
  function renderNav() {
    nav.textContent = "";
    models.forEach(function (mo) {
      var st = mo.node ? effectiveLoad(mo.node) : null;
      nav.appendChild(el("button", { type: "button", "class": "wd-model-link", "aria-current": mo.item === s.model ? "true" : null,
        onclick: function () { s.model = mo.item; s.table = null; renderNav(); render(); saveState(); } }, [
        kindMark(KIND.MODEL), el("span", { "class": "wd-grow", text: mo.name }), st ? el("i", { "class": "wd-dot wd-" + tone(st.result), title: st.result }) : null
      ]));
    });
  }
  function render() {
    main.textContent = "";
    var mo = metadata(m).models.get(s.model), refresh = mo.node ? effectiveLoad(mo.node) : null;
    main.appendChild(el("div", { "class": "wd-eyebrow", text: "Semantic model" }));
    main.appendChild(el("h2", { "class": "wd-detail-title", text: mo.name }));
    main.appendChild(el("p", { "class": mo.description ? "wd-desc" : "wd-desc wd-muted", text: mo.description || "No description." }));
    var facts = el("dl", { "class": "wd-facts" });
    function fact(label, value) { facts.appendChild(el("dt", { text: label })); facts.appendChild(el("dd", {}, [value])); }
    fact("Last refresh", refresh ? el("span", {}, [chip(refresh.result), " " + stamp(refresh.completed)]) : el("span", { "class": "wd-muted", text: "Nothing has settled since it was built" }));
    if (mo.node && mo.node.built) fact("Built", stamp(mo.node.built));
    var it = m.items.get(mo.item);
    if (it && it.target) fact("Fabric item", it.target);
    main.appendChild(facts);
    var validations = mo.node ? m.items.get(mo.item).nodes.filter(isValidation) : [];
    if (validations.length) {
      main.appendChild(el("h3", { "class": "wd-h3", text: "Tests and Assumptions · " + validations.length }));
      main.appendChild(el("ul", { "class": "wd-list" }, validations.map(function (v) {
        return el("li", {}, [kindMark(v.kind), v.label, el("span", { "class": "wd-grow" }), chip(v.test ? v.test.result : null, v.test ? null : "Not run"),
          v.test && v.test.failures ? el("span", { "class": "wd-muted", text: " " + fmt(v.test.failures) + " rows" }) : null]);
      })));
    }
    main.appendChild(el("div", { "class": "wd-toolbar" }, [
      el("h3", { "class": "wd-h3 wd-inline", text: "Tables · " + mo.tables.size }),
      el("span", { "class": "wd-grow" }),
      segmented("Order", ["authored", "alphabetical"], s.order, function (v) { s.order = v; render(); saveState(); }, function (v) { return v === "authored" ? "Authored" : "A–Z"; })
    ]));
    var tables = modelTables(mo, s.order);
    if (s.order === "authored" && tables.some(function (tb) { return tb.ordinal === null; })) {
      main.appendChild(el("p", { "class": "wd-note", text: "This model was read back without an authored table order, so its tables are listed alphabetically. Build it again to record the order." }));
    }
    var grid = el("div", { "class": "wd-semantic-grid" }), detail = el("div", { "class": "wd-entry-detail" });
    grid.appendChild(dataTable([
      { key: "ordinal", title: "#", value: function (tb) { return tb.ordinal; }, numeric: true, sortable: false },
      { key: "name", title: "Table", value: function (tb) { return tb.name; }, sortable: false },
      { key: "mode", title: "Mode", value: function (tb) { return tb.mode || ""; }, sortable: false },
      { key: "columns", title: "Columns", value: function (tb) { return tb.columns.length; }, numeric: true, sortable: false },
      { key: "measures", title: "Measures", value: function (tb) { return tb.measures.length; }, numeric: true, sortable: false },
      { key: "reads", title: "Reads", value: function (tb) { return tb.sources.map(function (src) { return src.node ? src.node.label : src.reference; }).join(", "); }, sortable: false }
    ], tables, { limit: 500, onRow: function (tb) { s.table = tb.name; showTable(tb); saveState(); } }));
    grid.appendChild(detail);
    main.appendChild(grid);
    function showTable(tb) { detail.textContent = ""; semanticTableDetail(detail, m, tb, null); }
    var chosen = s.table && mo.tables.get(s.table);
    if (chosen) showTable(chosen); else detail.appendChild(empty("Select a table to see its columns, measures and relationships."));
    if (mo.relationships.length) {
      main.appendChild(el("h3", { "class": "wd-h3", text: "Relationships · " + mo.relationships.length }));
      main.appendChild(el("ul", { "class": "wd-list wd-mono" }, mo.relationships.slice().sort(function (a, b) { return compare(a.from, b.from) || compare(a.to, b.to); }).map(relationshipLine)));
    }
  }
  body.appendChild(el("div", { "class": "wd-semantics" }, [nav, main]));
  renderNav(); render();
}

/* ---- bus matrix -------------------------------------------------------------- */

/* Fact-side tables against dimension-side tables, from a model's direct
   relationships. A many-to-one relationship supplies one of each; one-to-one
   and many-to-many relationships are listed as such, never given a role. */
function busMatrix(mo) {
  var facts = new Map(), dims = new Map(), cells = new Map(), other = [];
  mo.relationships.forEach(function (r) {
    var fact, dim;
    if (r.fromCardinality === "many" && r.toCardinality === "one") { fact = r.from; dim = r.to; }
    else if (r.fromCardinality === "one" && r.toCardinality === "many") { fact = r.to; dim = r.from; }
    else { other.push(r); return; }
    facts.set(fact, true); dims.set(dim, true);
    var key = fact + "\u0001" + dim, cell = cells.get(key);
    if (!cell) cells.set(key, cell = { active: 0, inactive: 0, relationships: [] });
    cell[r.active ? "active" : "inactive"] += 1;
    cell.relationships.push(r);
  });
  var related = new Set();
  mo.relationships.forEach(function (r) { related.add(r.from); related.add(r.to); });
  var isolated = Array.from(mo.tables.keys()).filter(function (name) { return !related.has(name); });
  return { facts: facts, dims: dims, cells: cells, other: other, isolated: isolated,
    mixed: Array.from(facts.keys()).filter(function (name) { return dims.has(name); }) };
}

function busPage(body, m) {
  var s = pageState("bus"), models = modelList(m);
  s.order = s.order || "authored"; s.tab = s.tab || "matrix";
  if (!models.length) { body.appendChild(empty("No semantic model is recorded in the catalogue.")); return; }
  if (!s.model || !models.some(function (mo) { return mo.item === s.model; })) s.model = models[0].item;
  var host = el("div");
  var picker = el("select", { "class": "wd-select", "aria-label": "Semantic model", onchange: function () { s.model = this.value; render(); saveState(); } },
    models.map(function (mo) { return el("option", { value: mo.item, selected: mo.item === s.model ? "selected" : null, text: mo.name }); }));
  body.appendChild(el("div", { "class": "wd-toolbar" }, [
    picker,
    segmented("View", ["matrix", "reuse"], s.tab, function (v) { s.tab = v; render(); saveState(); }, function (v) { return v === "matrix" ? "Fact × dimension" : "Source reuse"; }),
    segmented("Order", ["authored", "alphabetical"], s.order, function (v) { s.order = v; render(); saveState(); }, function (v) { return v === "authored" ? "Authored" : "A–Z"; })
  ]));
  body.appendChild(host);
  function render() {
    host.textContent = "";
    if (s.tab === "reuse") { host.appendChild(sourceReuse(m, models)); return; }
    var mo = metadata(m).models.get(s.model), bus = busMatrix(mo), order = modelTables(mo, s.order).map(function (tb) { return tb.name; });
    function sorted(map) { return order.filter(function (n) { return map.has(n); }).concat(Array.from(map.keys()).filter(function (n) { return order.indexOf(n) < 0; }).sort(compare)); }
    var facts = sorted(bus.facts), dims = sorted(bus.dims);
    if (!facts.length) { host.appendChild(empty("This model has no many-to-one relationships, so no table takes a fact or dimension side.")); }
    else {
      var table = el("table", { "class": "wd-bus" }, [
        el("thead", {}, [el("tr", {}, [el("th", { "class": "wd-bus-corner", text: "Fact side ↓   Dimension side →" })].concat(dims.map(function (d) {
          return el("th", { scope: "col", "class": "wd-bus-dim" }, [el("span", { text: d }), bus.mixed.indexOf(d) >= 0 ? el("i", { "class": "wd-bus-mixed", title: "Also on the fact side of another relationship", text: "◐" }) : null]);
        })))]),
        el("tbody", {}, facts.map(function (f) {
          return el("tr", {}, [el("th", { scope: "row" }, [f, bus.mixed.indexOf(f) >= 0 ? el("i", { "class": "wd-bus-mixed", title: "Also on the dimension side of another relationship", text: " ◐" }) : null])].concat(dims.map(function (d) {
            var cell = bus.cells.get(f + "\u0001" + d);
            if (!cell) return el("td", { "class": "wd-bus-empty" });
            var text = cell.active ? "●" : "○", count = cell.active + cell.inactive;
            return el("td", { "class": "wd-bus-cell" + (cell.active ? "" : " wd-bus-inactive"),
              title: cell.relationships.map(function (r) { return r.from + "[" + r.fromColumn + "] → " + r.to + "[" + r.toColumn + "]" + (r.active ? "" : " (inactive)"); }).join("\n") },
              [text, count > 1 ? el("sup", { text: String(count) }) : null]);
          })));
        }))
      ]);
      host.appendChild(el("div", { "class": "wd-table-wrap wd-bus-wrap" }, [table]));
      host.appendChild(el("div", { "class": "wd-legend" }, [
        el("span", { "class": "wd-key", text: "● active relationship" }), el("span", { "class": "wd-key", text: "○ inactive only" }),
        el("span", { "class": "wd-key", text: "² relationships between the pair" }), el("span", { "class": "wd-key", text: "◐ on both sides" }),
        el("span", { "class": "wd-key", text: "Cells show direct relationships, not filter propagation; an empty cell records none." })
      ]));
    }
    if (bus.other.length) {
      host.appendChild(el("h3", { "class": "wd-h3", text: "One-to-one and many-to-many · " + bus.other.length }));
      host.appendChild(el("ul", { "class": "wd-list wd-mono" }, bus.other.map(relationshipLine)));
    }
    if (bus.isolated.length) {
      host.appendChild(el("h3", { "class": "wd-h3", text: "Tables with no relationship · " + bus.isolated.length }));
      host.appendChild(el("p", { "class": "wd-muted", text: order.filter(function (n) { return bus.isolated.indexOf(n) >= 0; }).join(", ") }));
    }
  }
  render();
}

/* Logical sources against the models whose tables read them. */
function sourceReuse(m, models) {
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
  var list = Array.from(sources.values()).sort(function (a, b) { return compare(a.item, b.item) || compare(a.label, b.label); });
  return el("div", { "class": "wd-table-wrap wd-bus-wrap" }, [el("table", { "class": "wd-bus" }, [
    el("thead", {}, [el("tr", {}, [el("th", { "class": "wd-bus-corner", text: "Source ↓   Model →" })].concat(models.map(function (mo) { return el("th", { scope: "col", "class": "wd-bus-dim" }, [el("span", { text: mo.name })]); })))]),
    el("tbody", {}, list.map(function (src) {
      return el("tr", {}, [el("th", { scope: "row" }, [src.label, el("span", { "class": "wd-muted", text: " " + itemName(src.item) })])].concat(models.map(function (mo) {
        var tables = src.by.get(mo.item);
        return tables ? el("td", { "class": "wd-bus-cell", title: tables.join(", ") }, ["●", tables.length > 1 ? el("sup", { text: String(tables.length) }) : null]) : el("td", { "class": "wd-bus-empty" });
      })));
    }))
  ])]);
}
