"use strict";
const assert = require("assert").strict;
let assertions = 0;
function check(value, message) {
  assert.ok(value, message);
  assertions += 1;
}
function equal(actual, expected, message) {
  assert.deepEqual(
    actual,
    expected,
    `${message}\nExpected: ${JSON.stringify(expected)}\nActual: ${JSON.stringify(actual)}`
  );
  assertions += 1;
}
module.exports = { assert, check, equal, count: () => assertions };
