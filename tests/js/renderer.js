// Loads the Catalogue Dashboard renderer as weaver.catalogue_dashboard composes
// it: every script in name order, inside one function.
"use strict";
const fs = require("node:fs");
const path = require("node:path");

const folder = path.join(__dirname, "../../src/weaver/fragments/dashboard/renderer");

function load() {
  const body = fs.readdirSync(folder).filter((name) => name.endsWith(".js")).sort()
    .map((name) => fs.readFileSync(path.join(folder, name), "utf8")).join("\n");
  const module = { exports: {} };
  new Function("root", "module", '"use strict";\n' + body)({}, module);
  return module.exports;
}

module.exports = { load };
