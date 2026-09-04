/* Lineage highlighting for the scale graph.

   The SVG is already laid out and rendered by tools/build_scale_dag.py, so the
   page shows the whole estate with this file absent. All this adds is: click or
   focus a node to trace everything upstream and downstream of it, and a panel
   saying what the node is. */

(function () {
  "use strict";

  var svg = document.querySelector(".scale-dag");
  var panel = document.getElementById("dag-panel");
  if (!svg || !panel) return;

  var nodes = Array.prototype.slice.call(svg.querySelectorAll(".n"));
  var edges = Array.prototype.slice.call(svg.querySelectorAll(".e"));
  var search = document.getElementById("dag-search");
  var clear = document.getElementById("dag-clear");
  var counter = document.getElementById("dag-count");
  var fit = document.getElementById("dag-fit");
  var frame = svg.closest(".dag-frame");
  var data = null;
  var selected = -1;

  fetch("/assets/scale-dag.json")
    .then(function (response) { return response.json(); })
    .then(function (payload) {
      data = payload;
      svg.classList.add("interactive");
      if (search) search.disabled = false;
      if (fit) fit.hidden = false;
    })
    .catch(function () { /* the static picture is still the whole point */ });

  function reach(start, direction) {
    // Everything the node depends on, or everything that depends on it.
    var seen = {};
    var queue = [start];
    while (queue.length) {
      var current = queue.shift();
      (data[direction][current] || []).forEach(function (next) {
        if (!seen[next]) { seen[next] = true; queue.push(next); }
      });
    }
    return seen;
  }

  function render(index) {
    selected = index;

    if (index < 0) {
      svg.classList.remove("focused");
      nodes.forEach(function (n) { n.classList.remove("is-on", "is-up", "is-down", "is-self"); });
      edges.forEach(function (e) { e.classList.remove("is-on"); });
      panel.innerHTML = '<p class="hint">Select any object to trace what it reads and what reads it.</p>';
      if (clear) clear.hidden = true;
      return;
    }

    var up = reach(index, "up");
    var down = reach(index, "down");
    var lit = {};
    lit[index] = true;
    Object.keys(up).forEach(function (k) { lit[k] = true; });
    Object.keys(down).forEach(function (k) { lit[k] = true; });

    svg.classList.add("focused");
    nodes.forEach(function (node, position) {
      node.classList.toggle("is-self", position === index);
      node.classList.toggle("is-up", !!up[position]);
      node.classList.toggle("is-down", !!down[position]);
      node.classList.toggle("is-on", !!lit[position]);
    });
    edges.forEach(function (edge) {
      var u = +edge.getAttribute("data-u");
      var d = +edge.getAttribute("data-d");
      edge.classList.toggle("is-on", !!lit[u] && !!lit[d]);
    });

    var node = data.nodes[index];
    var direct = function (list) {
      return list.length
        ? list.slice(0, 8).map(function (i) {
            return "<li>" + escapeHtml(data.nodes[i].schema + "." + data.nodes[i].name) + "</li>";
          }).join("") + (list.length > 8 ? "<li>and " + (list.length - 8) + " more</li>" : "")
        : "<li class=\"none\">none</li>";
    };

    panel.innerHTML =
      '<h3>' + escapeHtml(node.name) + "</h3>" +
      '<dl>' +
      "<dt>Logical id</dt><dd>" + escapeHtml(node.id) + "</dd>" +
      "<dt>Fabric item</dt><dd>" + escapeHtml(node.item) + "</dd>" +
      "<dt>Kind</dt><dd>" + escapeHtml(node.type || node.role) +
        (node.loadable ? ", loadable" : "") + "</dd>" +
      "<dt>Layer</dt><dd>" + node.layer + "</dd>" +
      "</dl>" +
      '<div class="lineage">' +
      "<div><h4>Reads directly</h4><ul>" + direct(data.up[index]) + "</ul></div>" +
      "<div><h4>Read directly by</h4><ul>" + direct(data.down[index]) + "</ul></div>" +
      "</div>" +
      '<p class="totals">' + Object.keys(up).length + " objects upstream in total, " +
        Object.keys(down).length + " downstream.</p>";

    if (clear) clear.hidden = false;
  }

  function escapeHtml(text) {
    return String(text).replace(/[&<>"]/g, function (character) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[character];
    });
  }

  nodes.forEach(function (node, index) {
    function pick(event) {
      if (!data) return;
      event.preventDefault();
      render(selected === index ? -1 : index);
    }
    node.addEventListener("click", pick);
    node.addEventListener("keydown", function (event) {
      if (event.key === "Enter" || event.key === " ") pick(event);
      if (event.key === "Escape") render(-1);
    });
  });

  if (fit && frame) {
    fit.addEventListener("click", function () {
      // Trades label legibility for seeing the whole shape at once.
      var on = frame.classList.toggle("fit");
      fit.setAttribute("aria-pressed", on ? "true" : "false");
      fit.textContent = on ? "Actual size" : "Fit whole estate";
    });
  }

  if (clear) {
    clear.addEventListener("click", function () {
      render(-1);
      if (search) { search.value = ""; filter(""); }
    });
  }

  function filter(term) {
    var needle = term.trim().toLowerCase();
    if (!data) return;
    if (!needle) {
      svg.classList.remove("filtered");
      nodes.forEach(function (n) { n.classList.remove("is-match"); });
      if (counter) counter.textContent = "";
      return;
    }
    var hits = 0;
    svg.classList.add("filtered");
    nodes.forEach(function (node, index) {
      var record = data.nodes[index];
      var match = (record.schema + "." + record.name).toLowerCase().indexOf(needle) > -1;
      node.classList.toggle("is-match", match);
      if (match) hits += 1;
    });
    if (counter) counter.textContent = hits + (hits === 1 ? " match" : " matches");
  }

  if (search) {
    search.addEventListener("input", function () { filter(this.value); });
    search.addEventListener("keydown", function (event) {
      if (event.key !== "Enter" || !data) return;
      event.preventDefault();
      var first = nodes.findIndex(function (n) { return n.classList.contains("is-match"); });
      if (first > -1) {
        render(first);
        nodes[first].scrollIntoView({ block: "center", inline: "center" });
      }
    });
  }

  render(-1);
})();
