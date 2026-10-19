// Time. The catalogue holds UTC instants. A page shows them in the viewer's
// time zone, or in UTC when the viewer asks, and the data never changes. Day
// and clock arithmetic runs on wall time, an instant moved by its zone's
// offset, where a day is 24 hours wide whatever the zone does to the clock.

var UTC_ZONES = /^(?:UTC|Etc\/UTC|GMT|Etc\/GMT|Z)$/;

function showUtc() { return !!(root.__WD_STATE && root.__WD_STATE.utc); }

/* True when the viewer's own zone is UTC all year, so there is nothing to choose. */
function localIsUtc() {
  try { if (UTC_ZONES.test(Intl.DateTimeFormat().resolvedOptions().timeZone)) return true; } catch (e) { /* no Intl */ }
  return new Date(Date.UTC(2026, 0, 1)).getTimezoneOffset() === 0 && new Date(Date.UTC(2026, 6, 1)).getTimezoneOffset() === 0;
}

function offsetAt(t) { return showUtc() ? 0 : -new Date(t).getTimezoneOffset() * 60000; }

function wallOf(t) { return t + offsetAt(t); }

/* The instant a wall time names. An hour the clock repeats resolves to its first. */
function instantOf(wall) {
  if (showUtc()) return wall;
  return wall - offsetAt(wall - offsetAt(wall));
}

/* The zone's short name at t: "AEDT", "UTC", or an offset where the browser has no name. */
function zoneLabel(t, utc) {
  if (utc === undefined ? showUtc() : utc) return "UTC";
  try {
    var parts = new Intl.DateTimeFormat("en-AU", { timeZoneName: "short" }).formatToParts(new Date(t));
    for (var i = 0; i < parts.length; i++) if (parts[i].type === "timeZoneName") return parts[i].value;
  } catch (e) { /* no Intl */ }
  var minutes = -new Date(t).getTimezoneOffset();
  return "UTC" + (minutes < 0 ? "-" : "+") + Math.floor(Math.abs(minutes) / 60);
}

function pad2(n) { return n < 10 ? "0" + n : String(n); }

/* yyyy-mm-dd and hh:mm:ss of a wall time. */
function wallDate(wall) { var d = new Date(wall); return d.getUTCFullYear() + "-" + pad2(d.getUTCMonth() + 1) + "-" + pad2(d.getUTCDate()); }
function wallClock(wall) { var d = new Date(wall); return pad2(d.getUTCHours()) + ":" + pad2(d.getUTCMinutes()) + ":" + pad2(d.getUTCSeconds()); }

/* The time control every page header shares: the viewer's zone or UTC. */
function zoneControl(m) {
  if (localIsUtc()) return null;
  return segmented("Times", ["local", "utc"], showUtc() ? "utc" : "local", function (v) {
    state().utc = v === "utc";
    // A deadline names an hour of the zone it was set in.
    if (state().performance) state().performance.deadline = "";
    saveState();
    if (root.__WD_REDRAW) root.__WD_REDRAW();
  }, function (v) { return v === "utc" ? "UTC" : "Local · " + zoneLabel(m.at, false); });
}
