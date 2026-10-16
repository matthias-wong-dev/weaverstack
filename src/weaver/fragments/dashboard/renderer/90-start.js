// Start-up. Power BI renders the visual again on every refresh; view state
// lives on the window and, where the host allows it, in session storage, so a
// refresh keeps the focus, the depths and the viewport.

var PAGES = {
  overview: overviewPage, lineage: lineagePage, dictionary: dictionaryPage, er: erPage, semantics: semanticsPage,
  bus: busPage, load: loadPage, gantt: ganttPage, health: healthPage, performance: performancePage
};
var STORE = "weaver-dashboard-state";

function saveState() {
  try { root.sessionStorage.setItem(STORE, JSON.stringify(state())); } catch (e) { /* unavailable */ }
}

function restoreState() {
  if (root.__WD_STATE) return;
  try {
    var raw = root.sessionStorage.getItem(STORE);
    if (raw) root.__WD_STATE = JSON.parse(raw);
  } catch (e) { /* unavailable */ }
}

function start() {
  if (typeof document === "undefined") return;
  var mount = document.getElementById("wd-app"), source = document.getElementById("wd-data");
  if (!mount) return;
  restoreState();
  var s = state();
  if (!s.theme) s.theme = root.matchMedia && root.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  var parsed = parsePayload(source ? source.textContent : ""), m = buildModel(parsed.tables, parsed.problems);
  if (!source) m.problems.push("The page carries no catalogue data.");
  function render() {
    var scroll = root.scrollY || 0;
    mount.textContent = "";
    mount.className = "wd wd-page-" + m.page;
    mount.setAttribute("data-theme", s.theme);
    var body = shell(mount, m);
    try {
      (PAGES[m.page] || overviewPage)(body, m);
    } catch (error) {
      body.appendChild(el("pre", { "class": "wd-error", text: "This page could not be drawn: " + (error && error.message) }));
    }
    if (scroll && root.scrollTo) root.scrollTo(0, scroll);
    saveState();
  }
  root.__WD_REDRAW = render;
  render();
  root.addEventListener("pagehide", saveState);
}

var api = {
  parsePayload: parsePayload, buildModel: buildModel, assessHealth: assessHealth, readiness: readiness,
  layoutEstate: layoutEstate, focusWindow: focusWindow, searchNodes: searchNodes, dictionaryEntries: dictionaryEntries,
  searchEntries: searchEntries, metadata: metadata, modelTables: modelTables, busMatrix: busMatrix, erGraph: erGraph,
  erWindow: erWindow, ganttSummary: ganttSummary, logRows: logRows, perfRows: perfRows, tone: tone, BUDGET: BUDGET,
  FIELD: FIELD, ROW: ROW, TABLE: TABLE, LESS: LESS
};
if (typeof module === "object" && module && module.exports) module.exports = api;
else { root.WeaverDashboard = api; start(); }
