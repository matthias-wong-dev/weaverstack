/* Progressive enhancement only. Every page reads and works with this file absent:
   the tab panels are all visible, and code blocks are selectable text. */

(function () {
  "use strict";

  // --- mobile navigation ---------------------------------------------------

  var toggle = document.querySelector(".nav-toggle");
  var links = document.querySelector(".nav-links");
  if (toggle && links) {
    toggle.hidden = false;
    toggle.addEventListener("click", function () {
      var open = links.classList.toggle("open");
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
    });
  }

  // --- copy buttons --------------------------------------------------------

  document.querySelectorAll(".code").forEach(function (block) {
    var bar = block.querySelector(".code-bar");
    var pre = block.querySelector("pre");
    if (!bar || !pre || !navigator.clipboard) return;

    var button = document.createElement("button");
    button.type = "button";
    button.className = "copy";
    button.textContent = "Copy";
    bar.appendChild(button);

    button.addEventListener("click", function () {
      navigator.clipboard.writeText(pre.innerText.replace(/\s+$/, "")).then(function () {
        button.textContent = "Copied";
        button.setAttribute("data-copied", "");
        setTimeout(function () {
          button.textContent = "Copy";
          button.removeAttribute("data-copied");
        }, 1600);
      });
    });
  });

  // --- code tabs -----------------------------------------------------------

  document.querySelectorAll(".tabs").forEach(function (tabs) {
    var strip = tabs.querySelector(".tab-strip");
    var panels = Array.prototype.slice.call(tabs.querySelectorAll(".tab-panel"));
    if (!strip || panels.length < 2) return;

    // The strip is hidden until here, so a page without JavaScript shows every
    // panel rather than a row of buttons that do nothing.
    tabs.classList.remove("no-js");
    var buttons = Array.prototype.slice.call(strip.querySelectorAll("button"));

    function select(index) {
      buttons.forEach(function (button, position) {
        button.setAttribute("aria-selected", position === index ? "true" : "false");
        button.tabIndex = position === index ? 0 : -1;
      });
      panels.forEach(function (panel, position) {
        panel.hidden = position !== index;
      });
    }

    buttons.forEach(function (button, index) {
      button.addEventListener("click", function () { select(index); });
      button.addEventListener("keydown", function (event) {
        var step = event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
        if (!step) return;
        event.preventDefault();
        var next = (index + step + buttons.length) % buttons.length;
        select(next);
        buttons[next].focus();
      });
    });

    select(0);
  });
})();
