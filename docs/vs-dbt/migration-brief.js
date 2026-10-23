/* Copy the agent brief from its authoritative Markdown source. */
(function () {
  "use strict";

  var button = document.querySelector("[data-copy-markdown]");
  if (!button) return;

  var status = document.getElementById(button.getAttribute("aria-describedby"));
  var markdown = null;
  var originalLabel = button.textContent;

  function report(message) {
    if (status) status.textContent = message;
  }

  function fallbackCopy(value) {
    var field = document.createElement("textarea");
    field.value = value;
    field.setAttribute("readonly", "");
    field.style.position = "fixed";
    field.style.opacity = "0";
    document.body.appendChild(field);
    field.select();
    var copied = document.execCommand("copy");
    field.remove();
    return copied ? Promise.resolve() : Promise.reject(new Error("Copy failed"));
  }

  function copyMarkdown() {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(markdown);
    }
    return fallbackCopy(markdown);
  }

  fetch(button.getAttribute("data-copy-markdown"), { credentials: "same-origin" })
    .then(function (response) {
      if (!response.ok) throw new Error("Brief returned " + response.status);
      return response.text();
    })
    .then(function (source) {
      markdown = source;
      button.disabled = false;
      report("");
    })
    .catch(function () {
      report("The copyable brief could not be loaded. Open the Markdown version instead.");
    });

  button.addEventListener("click", function () {
    if (markdown === null) {
      report("The copyable brief is still loading. Try again in a moment.");
      return;
    }

    button.disabled = true;
    copyMarkdown()
      .then(function () {
        button.textContent = "Copied";
        button.setAttribute("data-copied", "");
        report("Agent brief copied as Markdown.");
        window.setTimeout(function () {
          button.textContent = originalLabel;
          button.removeAttribute("data-copied");
          button.disabled = false;
        }, 1800);
      })
      .catch(function () {
        button.disabled = false;
        report("The brief could not be copied. Open the Markdown version instead.");
      });
  });
})();
