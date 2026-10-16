// The graph view, drawn on a canvas so the whole estate stays responsive. A
// single-finger vertical swipe scrolls the page; one-finger horizontal drags,
// mouse drags and two-finger gestures move the graph.

var CELL = 64;

function GraphView(host, m, lay, options) {
  var self = this;
  this.m = m; this.lay = lay; this.o = options; this.s = options.state;
  this.host = host;
  this.canvas = el("canvas", { "class": "wd-graph-canvas", tabindex: "0", role: "img", "aria-label": options.label || "Lineage graph" });
  this.tip = el("div", { "class": "wd-tip", hidden: "hidden" });
  this.hint = el("div", { "class": "wd-hint", hidden: "hidden", text: "Hold Ctrl or ⌘ and scroll to zoom" });
  host.appendChild(this.canvas); host.appendChild(this.tip); host.appendChild(this.hint);
  this.index(lay);
  this.markers = [];
  this.win = null;
  this.hover = null;
  this.bind();
  if (root.ResizeObserver) new root.ResizeObserver(function () { self.resize(); }).observe(host);
  else root.addEventListener("resize", function () { self.resize(); });
  this.resize();
}

/* A spatial index of the layout's nodes, for hit testing. */
GraphView.prototype.index = function (lay) {
  var grid = new Map();
  lay.pos.forEach(function (p, key) {
    var cell = Math.floor(p.x / CELL) + "|" + Math.floor(p.y / CELL);
    var list = grid.get(cell);
    if (!list) grid.set(cell, list = []);
    list.push(key);
  });
  this.lay = lay; this.grid = grid;
};

GraphView.prototype.resize = function () {
  var box = this.host.getBoundingClientRect(), ratio = root.devicePixelRatio || 1;
  this.width = Math.max(1, box.width); this.height = Math.max(1, box.height);
  this.canvas.width = Math.round(this.width * ratio); this.canvas.height = Math.round(this.height * ratio);
  this.canvas.style.width = this.width + "px"; this.canvas.style.height = this.height + "px";
  this.ratio = ratio;
  if (!this.s.view) this.fitAll(false);
  this.draw();
};

GraphView.prototype.palette = function () {
  var cs = root.getComputedStyle(this.host), v = function (name) { return cs.getPropertyValue(name).trim(); };
  var P = {};
  ["ink", "ink-soft", "muted", "faint", "line", "line-strong", "surface", "paper", "paper-deep", "blue", "blue-dark", "blue-soft",
    "green", "green-soft", "amber", "amber-soft", "red", "red-soft", "violet", "violet-soft", "up", "down", "band", "band-line", "sans", "mono"]
    .forEach(function (name) { P[name] = v("--" + name); });
  return P;
};

function kindColour(kind, P) {
  switch (kind) {
    case KIND.FOLDER: return P.green;
    case KIND.SHORTCUT: return P.amber;
    case KIND.TEST: case KIND.ASSUMPTION: return P.violet;
    case KIND.MODEL: case KIND.REPORT: return P["blue-dark"];
    default: return P.blue;
  }
}

function toneColour(t, P) {
  return { ok: P.green, bad: P.red, warn: P.amber, wait: P.amber, none: P.faint }[t] || P.faint;
}

/* One glyph per kind, about 12 px across. */
function glyph(ctx, kind, x, y, colour, P) {
  ctx.lineWidth = 1.6;
  ctx.strokeStyle = colour; ctx.fillStyle = colour;
  ctx.beginPath();
  switch (kind) {
    case KIND.TABLE: ctx.arc(x, y, 5, 0, 7); ctx.fill(); break;
    case KIND.VIEW:
      ctx.fillStyle = P.surface; ctx.arc(x, y, 5, 0, 7); ctx.fill(); ctx.stroke();
      ctx.beginPath(); ctx.fillStyle = colour; ctx.arc(x, y, 1.8, 0, 7); ctx.fill(); break;
    case KIND.FOLDER:
      ctx.moveTo(x - 6, y - 4); ctx.lineTo(x - 2, y - 4); ctx.lineTo(x - 0.5, y - 2.5); ctx.lineTo(x + 6, y - 2.5);
      ctx.lineTo(x + 6, y + 4.5); ctx.lineTo(x - 6, y + 4.5); ctx.closePath(); ctx.fill(); break;
    case KIND.SHORTCUT:
      ctx.fillStyle = P.surface; ctx.moveTo(x, y - 6); ctx.lineTo(x + 6, y); ctx.lineTo(x, y + 6); ctx.lineTo(x - 6, y); ctx.closePath();
      ctx.fill(); ctx.stroke(); break;
    case KIND.TEST: ctx.moveTo(x, y - 6); ctx.lineTo(x + 6, y + 4.5); ctx.lineTo(x - 6, y + 4.5); ctx.closePath(); ctx.fill(); break;
    case KIND.ASSUMPTION:
      ctx.fillStyle = P.surface; ctx.moveTo(x, y - 6); ctx.lineTo(x + 6, y + 4.5); ctx.lineTo(x - 6, y + 4.5); ctx.closePath();
      ctx.fill(); ctx.stroke(); break;
    case KIND.MODEL:
      for (var i = 0; i < 6; i++) { var a = Math.PI / 3 * i + Math.PI / 6; ctx[i ? "lineTo" : "moveTo"](x + 6 * Math.cos(a), y + 6 * Math.sin(a)); }
      ctx.closePath(); ctx.fill(); break;
    case KIND.REPORT:
      ctx.fillStyle = P.surface; ctx.rect(x - 5.5, y - 5.5, 11, 11); ctx.fill(); ctx.stroke();
      ctx.beginPath(); ctx.fillStyle = colour; ctx.rect(x - 3, y, 1.6, 3); ctx.rect(x - 0.8, y - 3, 1.6, 6); ctx.rect(x + 1.4, y - 1.5, 1.6, 4.5); ctx.fill(); break;
    default: ctx.fillStyle = P.surface; ctx.arc(x, y, 4.5, 0, 7); ctx.fill(); ctx.stroke();
  }
}

function curve(ctx, a, b) {
  var x1 = a.x + 7, y1 = a.y, x2 = b.x - 8, y2 = b.y, c = Math.max(36, Math.abs(x2 - x1) * 0.45);
  ctx.moveTo(x1, y1);
  ctx.bezierCurveTo(x1 + c, y1, x2 - c, y2, x2, y2);
}

function arrow(ctx, b) {
  ctx.moveTo(b.x - 7, b.y); ctx.lineTo(b.x - 12, b.y - 3); ctx.lineTo(b.x - 12, b.y + 3); ctx.closePath();
}

/* Whether the viewport shows the node, or with no node any part of the estate. */
GraphView.prototype.showing = function (key) {
  var r = this.visibleRect(), p = key ? this.lay.pos.get(key) : null;
  if (p) return p.x > r.x && p.x < r.x + r.w && p.y > r.y && p.y < r.y + r.h;
  return r.x < this.lay.width && r.x + r.w > 0 && r.y < this.lay.height && r.y + r.h > 0;
};

/* Show a window over its own layout, or with no window the whole estate. */
GraphView.prototype.setWindow = function (win, lay) { this.win = win; this.index(lay); this.draw(); };

GraphView.prototype.visibleRect = function () {
  var v = this.s.view;
  return { x: -v.x / v.k, y: -v.y / v.k, w: this.width / v.k, h: this.height / v.k };
};

GraphView.prototype.draw = function () {
  var self = this;
  if (this.pending) return;
  this.pending = true;
  (root.requestAnimationFrame || function (f) { return setTimeout(f, 16); })(function () { self.pending = false; self.paint(); });
};

GraphView.prototype.paint = function () {
  var ctx = this.canvas.getContext("2d"), P = this.palette(), v = this.s.view, m = this.m, lay = this.lay, win = this.win;
  var mode = this.o.mode || "kind", self = this;
  ctx.setTransform(this.ratio, 0, 0, this.ratio, 0, 0);
  ctx.clearRect(0, 0, this.width, this.height);
  ctx.translate(v.x, v.y); ctx.scale(v.k, v.k);
  var r = this.visibleRect(), pad = 240;
  function inView(x, y) { return x > r.x - pad && x < r.x + r.w + pad && y > r.y - 40 && y < r.y + r.h + 40; }
  var k = v.k, labels = k >= 0.5, sans = P.sans || "sans-serif", mono = P.mono || "monospace";

  // Item bands, with the shortcut and validation sub-bands.
  lay.bands.forEach(function (b) {
    if (b.x > r.x + r.w || b.x + b.width < r.x || b.y > r.y + r.h || b.y + b.height < r.y) return;
    ctx.beginPath(); roundRect(ctx, b.x, b.y, b.width, b.height, 12);
    ctx.fillStyle = P.band; ctx.fill(); ctx.lineWidth = 1 / k; ctx.strokeStyle = P["band-line"]; ctx.stroke();
    b.columns.forEach(function (c) {
      if (c.kind === "objects") return;
      ctx.beginPath(); roundRect(ctx, c.x - 12, b.y + GEOMETRY.HEAD - 13, c.width - 6, b.height - GEOMETRY.HEAD + 5, 9);
      ctx.fillStyle = c.kind === "shortcuts" ? P["amber-soft"] : P["violet-soft"]; ctx.fill();
      if (k > 0.45) {
        ctx.fillStyle = c.kind === "shortcuts" ? P.amber : P.violet;
        ctx.font = "700 9px " + mono;
        ctx.fillText(c.kind === "shortcuts" ? "SHORTCUTS" : "TESTS", c.x - 4, b.y + GEOMETRY.HEAD - 2);
      }
    });
    // The header stays readable at any zoom: below 0.9 it grows as the view zooms out.
    var grow = Math.max(1, 0.9 / k), eyebrow = 9.5 * grow, name = Math.min(13 * grow, b.width / 7);
    ctx.fillStyle = P.blue; ctx.font = "700 " + eyebrow + "px " + mono;
    ctx.fillText(b.item.type.toUpperCase(), b.x + 16, b.y + 8 + eyebrow);
    ctx.fillStyle = P.ink; ctx.font = "700 " + name + "px " + sans;
    var title = clip(ctx, b.item.name + (b.item.target && b.item.target !== b.item.name ? " · " + b.item.target : ""), b.width - 32 - (k > 0.3 ? 90 : 0), "700 " + name + "px " + sans);
    ctx.fillText(title, b.x + 16, b.y + 12 + eyebrow + name);
    if (k > 0.3) {
      ctx.fillStyle = P.muted; ctx.font = "11px " + sans;
      var total = b.item.total || b.item.nodes.length;
      var meta = (total > b.item.nodes.length ? fmt(b.item.nodes.length) + " of " : "") + fmt(total) + (total === 1 ? " object" : " objects");
      ctx.fillText(meta, b.x + b.width - 16 - ctx.measureText(meta).width, b.y + 8 + eyebrow);
    }
  });

  var shown = win ? win.shown : null;
  // Edges: everything faintly without a focus; with one, the window's edges.
  ctx.lineWidth = 1 / Math.max(k, 0.5);
  if (!win) {
    ctx.strokeStyle = P["line-strong"]; ctx.globalAlpha = k < 0.3 ? 0.35 : 0.6; ctx.beginPath();
    m.nodes.forEach(function (n) {
      var b = lay.pos.get(n.key);
      n.vup.forEach(function (u) {
        var a = lay.pos.get(u);
        if (a && (inView(a.x, a.y) || inView(b.x, b.y))) curve(ctx, a, b);
      });
    });
    ctx.stroke(); ctx.globalAlpha = 1;
  } else {
    var hl = this.hover && shown.has(this.hover) ? this.hover : null;
    [null, "up", "down"].forEach(function (side) {
      ctx.beginPath();
      var heads = [];
      win.edges.forEach(function (e) {
        if (e.side !== side) return;
        var a = lay.pos.get(e.u), b = lay.pos.get(e.d);
        curve(ctx, a, b); heads.push(b);
      });
      if (side === null) { ctx.strokeStyle = P["line-strong"]; ctx.lineWidth = 1 / Math.max(k, 0.6); ctx.globalAlpha = 0.75; }
      else { ctx.strokeStyle = side === "up" ? P.up : P.down; ctx.lineWidth = 1.8 / Math.max(k, 0.7); ctx.globalAlpha = 1; }
      ctx.stroke();
      if (side !== null && k > 0.4) { ctx.beginPath(); heads.forEach(function (b) { arrow(ctx, b); }); ctx.fillStyle = ctx.strokeStyle; ctx.fill(); }
      ctx.globalAlpha = 1;
    });
    if (hl) {
      ctx.beginPath();
      win.edges.forEach(function (e) { if (e.u === hl || e.d === hl) curve(ctx, lay.pos.get(e.u), lay.pos.get(e.d)); });
      ctx.strokeStyle = P.ink; ctx.lineWidth = 1.6 / Math.max(k, 0.7); ctx.stroke();
    }
  }

  // Nodes.
  var font = "11px " + sans, fontStrong = "600 11px " + sans;
  this.labelEnds = new Map();
  m.nodes.forEach(function (n) {
    var p = lay.pos.get(n.key);
    if (!p || !inView(p.x, p.y)) return;
    var colour = self.nodeColour(n, P);
    if (win && win.seeds.has(n.key)) {
      ctx.beginPath(); ctx.arc(p.x, p.y, 10, 0, 7); ctx.fillStyle = P["blue-soft"]; ctx.fill();
      ctx.lineWidth = 2 / Math.max(k, 0.8); ctx.strokeStyle = P.ink; ctx.stroke();
    }
    glyph(ctx, n.kind, p.x, p.y, colour, P);
    self.marks(ctx, n, p, P);
    if (labels || (win && k > 0.3)) {
      var seed = win && win.seeds.has(n.key);
      var text = clip(ctx, n.label, GEOMETRY.SUB - 30, seed ? fontStrong : font);
      ctx.font = seed ? fontStrong : font;
      // A halo in the background colour keeps labels legible over edges.
      ctx.lineJoin = "round"; ctx.lineWidth = 3; ctx.strokeStyle = P.band; ctx.strokeText(text, p.x + 11, p.y + 4);
      ctx.fillStyle = P.ink;
      ctx.fillText(text, p.x + 11, p.y + 4);
      self.labelEnds.set(n.key, p.x + 11 + ctx.measureText(text).width);
    }
  });

  // Expansion markers: neighbours outside the window, upstream on the left.
  this.markers = [];
  if (win) {
    ctx.font = "600 9.5px " + mono;
    win.more.forEach(function (more, key) {
      var p = lay.pos.get(key);
      if (!inView(p.x, p.y)) return;
      if (more.up.length) self.marker(ctx, P, key, "up", more.up, p.x - 46, p.y, p.x - 8);
      if (more.down.length) {
        var end = self.labelEnds.get(key) || p.x + 10;
        self.marker(ctx, P, key, "down", more.down, end + 16, p.y, end + 4);
      }
    });
  }
};

GraphView.prototype.nodeColour = function (n, P) {
  if (this.o.mode !== "load") return kindColour(n.kind, P);
  var s = isValidation(n) ? n.test : effectiveLoad(n);
  if (n.isMirrored) return P.faint;
  return s ? toneColour(tone(s.result), P) : participates(n) ? P.faint : kindColour(n.kind, P);
};

/* Small status marks: a dot for a load or test that needs attention, and on
   the Load page a ring for derived readiness. */
GraphView.prototype.marks = function (ctx, n, p, P) {
  if (this.o.mode === "load") {
    var ready = readiness(this.m, n);
    if (ready.state === READINESS.READY || ready.state === READINESS.WAITING) {
      ctx.beginPath(); ctx.arc(p.x, p.y, 8.5, 0, 7); ctx.lineWidth = 1.5;
      ctx.setLineDash(ready.state === READINESS.WAITING ? [2, 2] : []);
      ctx.strokeStyle = ready.state === READINESS.READY ? P.blue : P.amber; ctx.stroke(); ctx.setLineDash([]);
    }
    return;
  }
  var s = isValidation(n) ? n.test : effectiveLoad(n), t = s ? tone(s.result) : null;
  if (t === "bad" || t === "warn" || t === "wait") {
    ctx.beginPath(); ctx.arc(p.x + 6, p.y - 6, 2.8, 0, 7); ctx.fillStyle = toneColour(t, P); ctx.fill();
    ctx.lineWidth = 1; ctx.strokeStyle = P.surface; ctx.stroke();
  }
};

/* An expansion marker: ‹‹ N for upstream neighbours left out, N ›› downstream. */
GraphView.prototype.marker = function (ctx, P, key, side, keys, x, y, join) {
  var text = side === "up" ? "‹‹ " + keys.length : keys.length + " ››", w = ctx.measureText(text).width + 12, left = side === "up" ? x - w / 2 + 14 : x;
  ctx.beginPath(); ctx.setLineDash([2, 2]);
  if (side === "up") { ctx.moveTo(left + w, y); ctx.lineTo(join, y); } else { ctx.moveTo(join, y); ctx.lineTo(left, y); }
  ctx.strokeStyle = P["line-strong"]; ctx.lineWidth = 1; ctx.stroke(); ctx.setLineDash([]);
  ctx.beginPath(); roundRect(ctx, left, y - 7.5, w, 15, 7.5);
  ctx.fillStyle = P["paper-deep"]; ctx.fill(); ctx.strokeStyle = P["line-strong"]; ctx.stroke();
  ctx.fillStyle = P.muted; ctx.fillText(text, left + 6, y + 3.4);
  this.markers.push({ key: key, side: side, keys: keys, x: left, y: y - 7.5, w: w, h: 15 });
};

function roundRect(ctx, x, y, w, h, radius) {
  var rr = Math.min(radius, w / 2, h / 2);
  ctx.moveTo(x + rr, y); ctx.arcTo(x + w, y, x + w, y + h, rr); ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr); ctx.arcTo(x, y, x + w, y, rr); ctx.closePath();
}

function clip(ctx, text, width, font) {
  ctx.font = font;
  if (ctx.measureText(text).width <= width) return text;
  var lo = 0, hi = text.length;
  while (lo < hi) { var mid = (lo + hi + 1) >> 1; if (ctx.measureText(text.slice(0, mid) + "…").width <= width) lo = mid; else hi = mid - 1; }
  return text.slice(0, lo) + "…";
}

/* ---- view changes --------------------------------------------------------- */

GraphView.prototype.fitRect = function (rect, maxK, animate) {
  var k = Math.min(maxK || 1.2, Math.max(0.05, Math.min(this.width / (rect.w + 160), this.height / (rect.h + 120))));
  var target = { k: k, x: this.width / 2 - (rect.x + rect.w / 2) * k, y: this.height / 2 - (rect.y + rect.h / 2) * k };
  this.go(target, animate);
};

GraphView.prototype.fitAll = function (animate) {
  this.fitRect({ x: 0, y: 0, w: this.lay.width, h: this.lay.height }, 1.1, animate);
};

/* Zoom to what is shown: the window's own layout, or the estate. */
GraphView.prototype.fitWindow = function (animate) {
  if (!this.win) return this.fitAll(animate);
  this.fitKeys(Array.from(this.win.shown), animate);
};

/* Keep a node where it is on screen across a change of layout. */
GraphView.prototype.pin = function (key, screen) {
  var p = this.lay.pos.get(key), v = this.s.view;
  if (!p || !screen) return;
  this.s.view = { k: v.k, x: screen.x - p.x * v.k, y: screen.y - p.y * v.k };
  this.draw();
};

GraphView.prototype.screenOf = function (key) {
  var p = this.lay.pos.get(key), v = this.s.view;
  return p ? { x: p.x * v.k + v.x, y: p.y * v.k + v.y } : null;
};

GraphView.prototype.fitKeys = function (keys, animate) {
  var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity, lay = this.lay;
  keys.forEach(function (key) {
    var p = lay.pos.get(key);
    x0 = Math.min(x0, p.x - 60); y0 = Math.min(y0, p.y - 20); x1 = Math.max(x1, p.x + 190); y1 = Math.max(y1, p.y + 20);
  });
  var rect = { x: x0, y: y0, w: x1 - x0, h: y1 - y0 }, k = Math.min(1.15, Math.min(this.width / (rect.w + 160), this.height / (rect.h + 120)));
  if (k < 0.2 && this.win) {
    // Too spread to read at once: keep the focus legible and in the middle.
    var p = lay.pos.get(this.win.focus);
    return this.go({ k: 0.75, x: this.width / 2 - (p.x + 60) * 0.75, y: this.height / 2 - p.y * 0.75 }, animate);
  }
  this.fitRect(rect, 1.15, animate);
};

GraphView.prototype.go = function (target, animate) {
  var self = this, from = this.s.view;
  if (!animate || !from || !root.requestAnimationFrame) { this.s.view = target; this.draw(); return; }
  var start = null;
  function frame(now) {
    if (start === null) start = now;
    var t = Math.min(1, (now - start) / 380), e = t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    self.s.view = { k: from.k + (target.k - from.k) * e, x: from.x + (target.x - from.x) * e, y: from.y + (target.y - from.y) * e };
    self.paint();
    if (t < 1) root.requestAnimationFrame(frame);
  }
  root.requestAnimationFrame(frame);
};

GraphView.prototype.zoomAt = function (factor, px, py) {
  var v = this.s.view, k = Math.min(3, Math.max(0.04, v.k * factor)), f = k / v.k;
  this.s.view = { k: k, x: px - (px - v.x) * f, y: py - (py - v.y) * f };
  this.draw();
};

GraphView.prototype.panBy = function (dx, dy) {
  var v = this.s.view;
  this.s.view = { k: v.k, x: v.x + dx, y: v.y + dy };
  this.draw();
};

/* ---- hit testing and input ------------------------------------------------ */

GraphView.prototype.world = function (cx, cy) {
  var box = this.canvas.getBoundingClientRect(), v = this.s.view;
  return { x: (cx - box.left - v.x) / v.k, y: (cy - box.top - v.y) / v.k, sx: cx - box.left, sy: cy - box.top };
};

GraphView.prototype.hit = function (cx, cy) {
  var w = this.world(cx, cy), self = this, best = null, bestD = Infinity, slop = 6 / this.s.view.k;
  for (var i = this.markers.length - 1; i >= 0; i--) {
    var mk = this.markers[i];
    if (w.x >= mk.x - slop && w.x <= mk.x + mk.w + slop && w.y >= mk.y - slop && w.y <= mk.y + mk.h + slop) return { marker: mk };
  }
  var gx = Math.floor(w.x / CELL), gy = Math.floor(w.y / CELL);
  for (var dx = -4; dx <= 1; dx++) for (var dy = -1; dy <= 1; dy++) {
    (self.grid.get((gx + dx) + "|" + (gy + dy)) || []).forEach(function (key) {
      var p = self.lay.pos.get(key), end = self.labelEnds && self.labelEnds.get(key);
      var near = Math.hypot(w.x - p.x, w.y - p.y);
      var onLabel = end && w.x >= p.x && w.x <= end && Math.abs(w.y - p.y) <= 8;
      var d = onLabel ? 0.5 : near;
      if ((near <= 9 + slop || onLabel) && d < bestD) { best = key; bestD = d; }
    });
  }
  return best ? { node: best } : null;
};

GraphView.prototype.bind = function () {
  var self = this, c = this.canvas, pointers = new Map(), drag = null, pinch = null;
  c.addEventListener("pointerdown", function (e) {
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (e.pointerType === "mouse") c.setPointerCapture(e.pointerId);
    if (pointers.size === 1) drag = { x: e.clientX, y: e.clientY, view: self.s.view, moved: false, touch: e.pointerType === "touch" };
    else if (pointers.size === 2) {
      var pts = Array.from(pointers.values());
      pinch = { d: Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y), cx: (pts[0].x + pts[1].x) / 2, cy: (pts[0].y + pts[1].y) / 2, view: self.s.view };
      drag = null;
    }
  });
  c.addEventListener("pointermove", function (e) {
    if (pointers.has(e.pointerId)) pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pinch && pointers.size === 2) {
      var pts = Array.from(pointers.values()), box = c.getBoundingClientRect();
      var d = Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y), cx = (pts[0].x + pts[1].x) / 2, cy = (pts[0].y + pts[1].y) / 2;
      var k = Math.min(3, Math.max(0.04, pinch.view.k * d / Math.max(1, pinch.d))), f = k / pinch.view.k;
      var px = pinch.cx - box.left, py = pinch.cy - box.top;
      self.s.view = { k: k, x: px - (px - pinch.view.x) * f + (cx - pinch.cx), y: py - (py - pinch.view.y) * f + (cy - pinch.cy) };
      self.draw();
      return;
    }
    if (drag) {
      var dx = e.clientX - drag.x, dy = e.clientY - drag.y;
      if (!drag.moved && Math.hypot(dx, dy) < 4) return;
      drag.moved = true;
      self.hideTip();
      // A touch moves the graph sideways; the page owns vertical swipes.
      self.s.view = { k: drag.view.k, x: drag.view.x + dx, y: drag.view.y + (drag.touch ? 0 : dy) };
      c.classList.add("wd-dragging");
      self.draw();
      return;
    }
    if (e.pointerType === "mouse") self.hoverAt(e.clientX, e.clientY);
  });
  function end(e) {
    var wasClick = drag && !drag.moved && pointers.size === 1;
    pointers.delete(e.pointerId);
    c.classList.remove("wd-dragging");
    if (pointers.size < 2) pinch = null;
    if (wasClick && e.type === "pointerup") self.click(e.clientX, e.clientY, e.shiftKey || e.metaKey || e.ctrlKey);
    if (!pointers.size) drag = null;
  }
  c.addEventListener("pointerup", end);
  c.addEventListener("pointercancel", end);
  c.addEventListener("pointerleave", function (e) { if (e.pointerType === "mouse") { self.hover = null; self.hideTip(); self.draw(); } });
  c.addEventListener("wheel", function (e) {
    var box = c.getBoundingClientRect();
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      self.zoomAt(Math.exp(-e.deltaY * (e.deltaMode ? 0.05 : 0.0025)), e.clientX - box.left, e.clientY - box.top);
    } else if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
      e.preventDefault(); self.panBy(-e.deltaX, 0);
    } else if (!pageScrolls()) {
      e.preventDefault();
      self.zoomAt(Math.exp(-e.deltaY * (e.deltaMode ? 0.05 : 0.0025)), e.clientX - box.left, e.clientY - box.top);
    } else self.flashHint();
  }, { passive: false });
  c.addEventListener("keydown", function (e) {
    var step = 80, handled = true;
    if (e.key === "ArrowLeft") self.panBy(step, 0);
    else if (e.key === "ArrowRight") self.panBy(-step, 0);
    else if (e.key === "ArrowUp") self.panBy(0, step);
    else if (e.key === "ArrowDown") self.panBy(0, -step);
    else if (e.key === "+" || e.key === "=") self.zoomAt(1.25, self.width / 2, self.height / 2);
    else if (e.key === "-") self.zoomAt(0.8, self.width / 2, self.height / 2);
    else if (e.key === "0") self.fitWindow(true);
    else if (e.key === "Escape" && self.o.onSelect) self.o.onSelect(null);
    else handled = false;
    if (handled) e.preventDefault();
  });
};

function pageScrolls() {
  var s = document.scrollingElement || document.documentElement;
  return s.scrollHeight > s.clientHeight + 4;
}

GraphView.prototype.flashHint = function () {
  var hint = this.hint;
  hint.hidden = false;
  clearTimeout(this.hintTimer);
  this.hintTimer = setTimeout(function () { hint.hidden = true; }, 1400);
};

GraphView.prototype.click = function (cx, cy, adding) {
  var h = this.hit(cx, cy);
  if (!h) return;
  if (h.marker && this.o.onReveal) this.o.onReveal(h.marker);
  else if (h.node && this.o.onSelect) this.o.onSelect(h.node, adding);
};

GraphView.prototype.hoverAt = function (cx, cy) {
  var h = this.hit(cx, cy), key = h && h.node ? h.node : null;
  this.canvas.style.cursor = h ? "pointer" : "grab";
  if (h && h.marker) {
    this.showTip(cx, cy, [el("b", { text: h.marker.keys.length + (h.marker.side === "up" ? " more upstream" : " more downstream") }),
      el("div", { "class": "wd-muted", text: "Select to show " + (h.marker.keys.length > BUDGET.REVEAL ? "the first " + BUDGET.REVEAL : h.marker.keys.length === 1 ? "it" : "them") })]);
  } else if (key) {
    var n = this.m.nodes.get(key), s = isValidation(n) ? n.test : effectiveLoad(n);
    this.showTip(cx, cy, [el("b", { text: n.label }), el("div", { "class": "wd-muted", text: n.kind + " · " + n.item }),
      s ? el("div", {}, [chip(s.result), " " + (s.completed ? stamp(s.completed) : "")]) : null]);
  } else this.hideTip();
  if (key !== this.hover) { this.hover = key; this.draw(); }
};

GraphView.prototype.showTip = function (cx, cy, children) {
  var box = this.host.getBoundingClientRect(), tip = this.tip;
  tip.textContent = "";
  children.forEach(function (c) { if (c) tip.appendChild(c); });
  tip.hidden = false;
  tip.style.left = Math.min(box.width - 250, cx - box.left + 14) + "px";
  tip.style.top = Math.min(box.height - 70, cy - box.top + 14) + "px";
};

GraphView.prototype.hideTip = function () { this.tip.hidden = true; };
