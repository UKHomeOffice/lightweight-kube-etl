const child_process = jest.genMockFromModule('child_process');
const {
  complete_job,
  running_job,
  pod_status_ready,
  pod_status_not_ready,
  pod_not_ready
} = require('../helpers.spec');

const get_jobs_to_delete = jest.fn()
  .mockReturnValueOnce(new Error('kubectl error'))
  .mockReturnValue({
    items: [
      {
        "metadata": {
          "name": "elastic-bulk-1538055000"
        }
      },
      {
        "metadata": {
          "name": "neo4j-bulk-1538055000"
        }
      },
      {
        "metadata": {
          "name": "elastic-delta-1537362006"
        }
      },
      {
        "metadata": {
          "name": "elastic-delta-1537362006"
        }
      },
      {
        "metadata": {
          "name": "some-other-important-job"
        }
      }
    ]
  });

function getResponse (cmd) {
  switch(cmd) {
    case "kubectl --context acp-notprod_DACC -n dacc-entitysearch --token MOCK_TOKEN get jobs -o json":
      return get_jobs_to_delete();
  }
}

function exec (cmd, callback) {
  const response = getResponse(cmd);

  if (response instanceof Error) {
    callback(response);
  } else {
    const stdout = JSON.stringify(response);
    const stderr = JSON.stringify([]);
    callback(null, stdout, stderr);
  }
}

function spawn (cmd, callback) {
  callback(null, new Buffer("hello you"));
}

child_process.exec = jest.fn().mockImplementation(exec);
child_process.spawn = jest.fn().mockImplementation(spawn);

module.exports = child_process;