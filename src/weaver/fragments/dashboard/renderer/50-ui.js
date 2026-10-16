// Shared interface: DOM helpers, formatting, the app shell, search and the
// object details every page opens.

function el(tag, attrs, children) {
  var e = document.createElement(tag);
  setAttrs(e, attrs);
  (children || []).forEach(function (c) { if (c !== null && c !== undefined && c !== false) e.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
  return e;
}

function setAttrs(e, attrs) {
  Object.keys(attrs || {}).forEach(function (k) {
    var v = attrs[k];
    if (v === null || v === undefined || v === false) return;
    if (k.slice(0, 2) === "on") e.addEventListener(k.slice(2), v);
    else if (k === "text") e.textContent = v;
    else e.setAttribute(k, v);
  });
}

function fmt(n) { return n === null || n === undefined ? "—" : Number(n).toLocaleString("en-AU"); }

function duration(ms) {
  if (ms === null || ms === undefined) return "—";
  var s = ms / 1000;
  if (s < 60) return (s < 10 ? s.toFixed(1) : Math.round(s)) + " s";
  var mins = Math.floor(s / 60);
  if (mins < 60) return mins + " min " + Math.round(s % 60) + " s";
  var h = Math.floor(mins / 60);
  return h < 48 ? h + " h " + (mins % 60) + " min" : Math.floor(h / 24) + " d " + (h % 24) + " h";
}

function stamp(t) {
  if (t === null || t === undefined) return "—";
  return new Date(t).toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

function ago(t, now) {
  if (t === null || t === undefined) return "never";
  var s = Math.max(0, (now - t) / 1000);
  if (s < 90) return Math.round(s) + " s ago";
  if (s < 5400) return Math.round(s / 60) + " min ago";
  if (s < 172800) return Math.round(s / 3600) + " h ago";
  return Math.round(s / 86400) + " days ago";
}

function chip(result, label) {
  return el("span", { "class": "wd-chip wd-" + tone(result) }, [label || result || "No status"]);
}

function severityChip(severity, unobserved) {
  if (unobserved) return el("span", { "class": "wd-chip wd-none", text: "Unobserved" });
  return el("span", { "class": "wd-chip wd-" + ({ red: "bad", amber: "warn", green: "ok" }[severity]), text: { red: "Red", amber: "Amber", green: "Green" }[severity] });
}

function kindMark(kind) { return el("i", { "class": "wd-kind wd-kind-" + String(kind).toLowerCase().replace(/\s+/g, "-"), "aria-hidden": "true" }); }

function state() {
  root.__WD_STATE = root.__WD_STATE || {};
  return root.__WD_STATE;
}

function pageState(name) { var s = state(); s[name] = s[name] || {}; return s[name]; }

/* A request from one page to another, through storage the visuals may share. */
function handOff(key) {
  try { root.localStorage.setItem("weaver-dashboard-focus", JSON.stringify({ key: key, at: Date.now() })); } catch (e) { /* unavailable */ }
}

function takeHandOff() {
  try {
    var raw = root.localStorage.getItem("weaver-dashboard-focus");
    if (!raw) return null;
    root.localStorage.removeItem("weaver-dashboard-focus");
    var v = JSON.parse(raw);
    return Date.now() - v.at < 10 * 60 * 1000 ? v.key : null;
  } catch (e) { return null; }
}

function weaveMark() { return el("span", { "class": "wd-mark", "aria-hidden": "true" }, [el("i"), el("i"), el("i"), el("i")]); }

var PAGE_TITLES = {
  overview: "Overview", lineage: "Lineage", dictionary: "Dictionary", er: "ER diagram", semantics: "Semantics",
  bus: "Bus matrix", load: "Load", gantt: "Gantt", health: "Health", performance: "Performance"
};

/* The header every page shares: the catalogue's own name, where it lives, and
   the two times a reader needs apart, when this was queried and when work
   last settled. */
function shell(mount, m) {
  var s = state();
  var title = m.catalogue.name || "Weaver catalogue";
  var context = [m.catalogue.workspace ? "Workspace " + m.catalogue.workspace : null,
    m.catalogue.version ? "Weaver " + m.catalogue.version : null].filter(Boolean).join(" · ");
  var settled = latestSettled(m);
  var themeButton = el("button", { "class": "wd-icon", type: "button", title: "Switch light and dark", "aria-label": "Switch light and dark",
    onclick: function () { s.theme = mount.getAttribute("data-theme") === "dark" ? "light" : "dark"; mount.setAttribute("data-theme", s.theme); redraw(); } },
    [el("span", { "aria-hidden": "true", text: "◐" })]);
  var head = el("header", { "class": "wd-top" }, [
    el("div", { "class": "wd-brand" }, [weaveMark(), el("div", {}, [
      el("div", { "class": "wd-eyebrow", text: "Catalogue · " + (PAGE_TITLES[m.page] || m.page) }),
      el("h1", { text: title }),
      context ? el("div", { "class": "wd-context", text: context }) : null
    ])]),
    el("span", { "class": "wd-grow" }),
    el("dl", { "class": "wd-times" }, [
      el("dt", { text: "Queried" }), el("dd", { text: stamp(m.at) }),
      settled === undefined ? null : el("dt", { text: "Last settled" }),
      settled === undefined ? null : el("dd", { text: settled ? stamp(settled) + " · " + ago(settled, m.at) : "nothing has settled" })
    ]),
    themeButton
  ]);
  mount.appendChild(head);
  if (m.problems.length) {
    mount.appendChild(el("div", { "class": "wd-banner wd-banner-bad" }, [
      el("b", { text: "Some catalogue data could not be read. " }),
      m.problems.slice(0, 3).join(" ") + (m.problems.length > 3 ? " (" + (m.problems.length - 3) + " more)" : "")
    ]));
  }
  var body = el("main", { "class": "wd-body" });
  mount.appendChild(body);
  return body;
}

/* The latest settled load, test or workflow, or undefined when the page
   carries none of them. */
function latestSettled(m) {
  if (!m.tables.loads && !m.tables.tests && !m.tables.workflows) return undefined;
  var latest = null;
  m.nodes.forEach(function (n) {
    [n.load, n.test].forEach(function (s) { if (s && s.result !== "Pending" && s.completed !== null) latest = later(latest, s.completed); });
  });
  (m.workflows || []).forEach(function (w) { latest = later(latest, w.completed); });
  return latest;
}

function redraw() { if (root.__WD_REDRAW) root.__WD_REDRAW(); }

function section(title, children, attrs) {
  return el("section", Object.assign({ "class": "wd-section" }, attrs || {}), [title ? el("h2", { "class": "wd-h", text: title }) : null].concat(children || []));
}

function empty(text) { return el("div", { "class": "wd-empty", text: text }); }

/* ---- search ----------------------------------------------------------------- */

function searchNodes(m, query, limit) {
  var terms = String(query || "").toLowerCase().split(/\s+/).filter(Boolean);
  if (!terms.length) return [];
  var found = [];
  m.nodes.forEach(function (n) {
    var label = n.label.toLowerCase(), item = n.item.toLowerCase(), desc = n.desc.toLowerCase(),
      full = (n.schema + "." + n.object).toLowerCase(), score = 0;
    for (var i = 0; i < terms.length; i++) {
      var t = terms[i], s = 0;
      if (label === t || n.object && n.object.toLowerCase() === t) s = 100;
      else if (label.indexOf(t) === 0 || (n.object || "").toLowerCase().indexOf(t) === 0) s = 80;
      else if ((" " + label.replace(/[._/\-]/g, " ")).indexOf(" " + t) >= 0) s = 60;
      else if (label.indexOf(t) >= 0 || full.indexOf(t) >= 0) s = 40;
      else if (item.indexOf(t) >= 0) s = 25;
      else if (n.kind.toLowerCase() === t) s = 20;
      else if (desc.indexOf(t) >= 0) s = 10;
      if (!s) return;
      score += s;
    }
    found.push({ node: n, score: score });
  });
  found.sort(function (a, b) {
    return b.score - a.score || a.node.label.length - b.node.label.length || compare(a.node.label, b.node.label) || compare(a.node.key, b.node.key);
  });
  return found.slice(0, limit || 12).map(function (f) { return f.node; });
}

function searchBox(options) {
  var input = el("input", { "class": "wd-search", type: "search", placeholder: options.placeholder || "Search", "aria-label": options.placeholder || "Search", autocomplete: "off", spellcheck: "false" });
  var list = el("ul", { "class": "wd-results", role: "listbox", hidden: "hidden" }), results = [], active = 0;
  function render() {
    list.textContent = "";
    list.hidden = !results.length;
    results.forEach(function (r, i) {
      list.appendChild(el("li", { role: "option", "aria-selected": String(i === active), "class": "wd-result",
        onmousedown: function (e) { e.preventDefault(); choose(r); } },
        [el("span", { "class": "wd-result-label" }, [r.kind ? kindMark(r.kind) : null, r.label]),
          el("span", { "class": "wd-result-meta", text: r.meta }),
          r.desc ? el("span", { "class": "wd-result-desc", text: r.desc }) : null]));
    });
  }
  function choose(r) { input.value = ""; results = []; render(); input.blur(); options.onPick(r); }
  input.addEventListener("input", function () { results = options.find(input.value); active = 0; render(); });
  input.addEventListener("keydown", function (e) {
    if (e.key === "ArrowDown") { active = Math.min(results.length - 1, active + 1); render(); e.preventDefault(); }
    else if (e.key === "ArrowUp") { active = Math.max(0, active - 1); render(); e.preventDefault(); }
    else if (e.key === "Enter" && results[active]) choose(results[active]);
    else if (e.key === "Escape") { input.value = ""; results = []; render(); }
  });
  input.addEventListener("blur", function () { setTimeout(function () { list.hidden = true; }, 150); });
  input.addEventListener("focus", function () { if (results.length) list.hidden = false; });
  return el("div", { "class": "wd-searchbox" }, [input, list]);
}

function nodeSearch(m, onPick) {
  return searchBox({
    placeholder: "Search " + fmt(m.nodes.size) + " objects by name, item or description",
    find: function (q) {
      return searchNodes(m, q, 12).map(function (n) { return { key: n.key, kind: n.kind, label: n.label, meta: n.kind + " · " + n.item, desc: n.desc }; });
    },
    onPick: function (r) { onPick(r.key); }
  });
}

function segmented(label, values, value, onChange, display) {
  var group = el("div", { "class": "wd-seg", role: "radiogroup", "aria-label": label }, [el("span", { "class": "wd-seg-label", text: label })]);
  values.forEach(function (v) {
    group.appendChild(el("button", { "class": "wd-seg-btn", type: "button", role: "radio", "aria-checked": String(v === value),
      onclick: function () {
        Array.prototype.forEach.call(group.querySelectorAll("button"), function (b) { b.setAttribute("aria-checked", "false"); });
        this.setAttribute("aria-checked", "true");
        onChange(v);
      } }, [display ? display(v) : String(v)]));
  });
  return group;
}

function depthLabel(d) { return d < 0 ? "Max" : String(d); }

/* ---- object details ----------------------------------------------------------- */

function nodeLink(m, key, onOpen) {
  var n = m.nodes.get(key);
  return el("button", { "class": "wd-link", type: "button", title: n.item + " · " + qualified(n), onclick: function () { onOpen(key); } },
    [kindMark(n.kind), n.label, el("span", { "class": "wd-link-meta", text: itemName(n.item) })]);
}

function details(panel, m, key, options) {
  options = options || {};
  panel.textContent = "";
  var n = m.nodes.get(key);
  if (!n) { panel.appendChild(empty(options.emptyText || "Select an object to see its details.")); return; }
  var open = options.onOpen || function () {};
  panel.appendChild(el("div", { "class": "wd-eyebrow" }, [kindMark(n.kind), " " + n.kind]));
  panel.appendChild(el("h2", { "class": "wd-detail-title", text: n.label }));
  panel.appendChild(el("div", { "class": "wd-mono wd-muted", text: n.item + " · " + qualified(n) }));
  panel.appendChild(el("p", { "class": n.desc ? "wd-desc" : "wd-desc wd-muted", text: n.desc || "No description is declared." }));

  var s = isValidation(n) ? n.test : effectiveLoad(n), carries = !!m.tables.loads;
  var facts = el("dl", { "class": "wd-facts" });
  function fact(label, value) { facts.appendChild(el("dt", { text: label })); facts.appendChild(el("dd", {}, [value])); }
  // A page that carries no status says nothing about loads.
  if (carries && n.isMirrored) fact("Load", el("span", { "class": "wd-muted", text: "Recorded in the catalogue this one mirrors" }));
  else if (carries && s) {
    fact(isValidation(n) ? "Last run" : "Last load", el("span", {}, [chip(s.result), " " + stamp(s.completed)]));
    if (s.duration !== null && s.duration !== undefined) fact("Duration", duration(s.duration));
    if (isValidation(n) && s.failures) fact("Failing rows", fmt(s.failures));
    if (s.workflow) fact("Workflow", el("span", { "class": "wd-mono", text: s.workflow }));
  } else if (carries && (participates(n) || isValidation(n))) fact(isValidation(n) ? "Last run" : "Last load", el("span", { "class": "wd-muted", text: "Nothing has settled since it was built" }));
  if (carries && isLoadSubject(n)) {
    var ready = readiness(m, n);
    fact("Readiness", el("span", {}, [el("span", { "class": "wd-chip wd-" + readinessTone(ready.state), text: ready.state }),
      ready.waitingOn.length ? " waiting on " + ready.waitingOn.slice(0, 3).map(function (w) { return w.label; }).join(", ") + (ready.waitingOn.length > 3 ? " and " + (ready.waitingOn.length - 3) + " more" : "") : ""]));
  }
  if (n.built) fact("Built", stamp(n.built));
  if (n.isStatic) fact("Static", "Loaded once");
  panel.appendChild(facts);

  if (options.health) {
    var mine = options.health.all.filter(function (f) { return f.node === n; });
    if (mine.length) {
      panel.appendChild(el("h3", { "class": "wd-h3", text: "Health" }));
      panel.appendChild(el("ul", { "class": "wd-findings" }, mine.map(function (f) {
        return el("li", {}, [severityChip(f.severity, f.unobserved), " " + f.message]);
      })));
    }
  }
  var validations = n.vdown.filter(function (d) { return isValidation(m.nodes.get(d)); });
  if (validations.length) {
    panel.appendChild(el("h3", { "class": "wd-h3", text: "Tests and Assumptions" }));
    panel.appendChild(el("ul", { "class": "wd-list" }, validations.map(function (d) {
      var t = m.nodes.get(d);
      return el("li", {}, [nodeLink(m, d, open), el("span", { "class": "wd-grow" }), carries ? chip(t.test ? t.test.result : null, t.test ? null : "Not run") : null]);
    })));
  }
  [["Reads", n.vup.filter(function (u) { return !isValidation(m.nodes.get(u)); })],
    ["Read by", n.vdown.filter(function (d) { return !isValidation(m.nodes.get(d)); })]].forEach(function (pair) {
    if (!pair[1].length) return;
    var sorted = pair[1].slice().sort(function (a, b) { return compare(m.nodes.get(a).label, m.nodes.get(b).label); });
    panel.appendChild(el("h3", { "class": "wd-h3", text: pair[0] + " · " + pair[1].length }));
    panel.appendChild(el("ul", { "class": "wd-list" }, sorted.slice(0, 40).map(function (k) { return el("li", {}, [nodeLink(m, k, open)]); })
      .concat(sorted.length > 40 ? [el("li", { "class": "wd-muted", text: (sorted.length - 40) + " more" })] : [])));
  });
  if (n.stubs.length) {
    panel.appendChild(el("h3", { "class": "wd-h3", text: "Reads outside Weaver · " + n.stubs.length }));
    panel.appendChild(el("ul", { "class": "wd-list wd-mono" }, n.stubs.map(function (ref) { return el("li", { text: ref }); })));
  }
  if (options.columns) {
    var cols = options.columns.get(n.id) || [];
    if (cols.length) {
      panel.appendChild(el("h3", { "class": "wd-h3", text: "Described columns · " + cols.length }));
      panel.appendChild(el("dl", { "class": "wd-columns" }, cols.reduce(function (all, c) {
        return all.concat([el("dt", { text: c.column }), el("dd", { text: c.description || "" })]);
      }, [])));
    }
  }
  if (options.lineageLink) {
    panel.appendChild(el("button", { "class": "wd-text-btn wd-handoff", type: "button", onclick: function () { handOff(n.key); this.textContent = "Open the Lineage page to see it"; } },
      ["Show in Lineage"]));
  }
}

function readinessTone(state) {
  return { Current: "ok", Ready: "next", Waiting: "wait", "Loaded elsewhere": "none", "Not loaded here": "none" }[state] || "none";
}

/* A table for a list of records, with optional sorting and a row limit. */
function dataTable(columns, rows, options) {
  options = options || {};
  var limit = options.limit || 200, sortBy = options.sort || null, desc = options.desc !== false;
  var wrap = el("div", { "class": "wd-table-wrap" });
  function render() {
    wrap.textContent = "";
    var sorted = rows.slice();
    if (sortBy) {
      var col = columns.filter(function (c) { return c.key === sortBy; })[0];
      sorted.sort(function (a, b) {
        var x = col.sort ? col.sort(a) : col.value(a), y = col.sort ? col.sort(b) : col.value(b);
        var r = typeof x === "number" || typeof y === "number" ? (x === null ? -Infinity : x) - (y === null ? -Infinity : y) : compare(x, y);
        return desc ? -r : r;
      });
    }
    var table = el("table", { "class": "wd-table" }, [
      el("thead", {}, [el("tr", {}, columns.map(function (c) {
        return el("th", { "class": c.numeric ? "wd-num" : null, scope: "col", "aria-sort": c.key === sortBy ? (desc ? "descending" : "ascending") : null },
          [c.sortable === false ? c.title : el("button", { type: "button", "class": "wd-sort", onclick: function () {
            if (sortBy === c.key) desc = !desc; else { sortBy = c.key; desc = !!c.numeric; }
            render();
          } }, [c.title])]);
      }))]),
      el("tbody", {}, sorted.slice(0, limit).map(function (row) {
        return el("tr", { "class": options.onRow ? "wd-clickable" : null, onclick: options.onRow ? function () { options.onRow(row); } : null },
          columns.map(function (c) {
            var v = c.render ? c.render(row) : c.value(row);
            return el("td", { "class": c.numeric ? "wd-num" : c.mono ? "wd-mono" : null }, [v === null || v === undefined ? "—" : typeof v === "object" ? v : String(v)]);
          }));
      }))
    ]);
    wrap.appendChild(table);
    if (sorted.length > limit) wrap.appendChild(el("div", { "class": "wd-more-rows", text: "Showing " + fmt(limit) + " of " + fmt(sorted.length) + ". Search or sort to find the rest." }));
    if (!sorted.length) wrap.appendChild(empty(options.emptyText || "Nothing to show."));
  }
  render();
  return wrap;
}
