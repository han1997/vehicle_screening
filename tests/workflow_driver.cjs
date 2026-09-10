"use strict";
const { count } = require("./support/assertions.cjs");
const suite = process.argv.find((value) => ["core", "api", "ui"].includes(value));
if (!suite) throw new Error("Expected core, api or ui");
require(`./specs/${suite}.cjs`)()
  .then(() => {
    console.log(`PASS ${suite}: ${count()} assertions`);
    if (suite === "ui") require("electron").app.exit(0);
  })
  .catch((error) => {
    console.error(error.stack || error);
    if (suite === "ui") require("electron").app.exit(1);
    else process.exitCode = 1;
  });
