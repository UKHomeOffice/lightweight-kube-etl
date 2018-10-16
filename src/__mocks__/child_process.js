const child_process = jest.genMockFromModule('child_process');

function exec (cmd, callback) {
  console.log(cmd);
  const stdout = new Buffer(JSON.stringify({items: [{metaname: 'job-1'}]}));
  const stderr = new Buffer([]);
  callback(null, stdout, stderr);
}

child_process.exec = exec;

module.exports = child_process;