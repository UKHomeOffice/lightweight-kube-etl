console.log(require('./src/elt_hero'));
console.log('\nversion: ', require('./package.json').version, '\n');
const { start } = require('./src/ingestor');
start();
