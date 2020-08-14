console.log(require("./src/elt_hero"));
console.log("\nversion: ", require("./package.json").version, "\n");
const { start, waitForManifest } = require("./src/ingestor");

start(waitForManifest);
