const { start } = require('./src/ingestor');
console.log(require('./src/elt_hero'));
console.log('\nversion: ', require('./package.json').version, '\n');
start();
