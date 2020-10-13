const child_process = jest.genMockFromModule("child_process");
const EventEmitter = require("events");
class Spawn extends EventEmitter {}
const {
  complete_job,
  running_job,
  pod_status_ready,
  pod_status_not_ready,
  pod_not_ready
} = require("../helpers.spec");

const get_jobs_to_delete = jest
  .fn()
  .mockReturnValueOnce(new Error("kubectl error"))
  .mockReturnValue({
    items: [
      {
        metadata: {
          name: "elastic-bulk-1538055000"
        }
      },
      {
        metadata: {
          name: "neo4j-bulk-1538055000"
        }
      },
      {
        metadata: {
          name: "elastic-delta-1537362006"
        }
      },
      {
        metadata: {
          name: "elastic-delta-1537362006"
        }
      },
      {
        metadata: {
          name: "some-other-important-job"
        }
      }
    ]
  });

const get_pod_status = jest
  .fn()
  .mockReturnValueOnce(new Error("kubectl get pods error"))
  .mockReturnValueOnce(pod_not_ready)
  .mockReturnValueOnce(pod_status_not_ready)
  .mockReturnValue(pod_status_ready);

const get_job_status = jest
  .fn()
  .mockReturnValueOnce(new Error("kubectl get jobs error"))
  .mockReturnValueOnce("")
  .mockReturnValueOnce(running_job)
  .mockReturnValue(complete_job);

function getOutput(command) {
  const cmd = command.replace(
    "kubectl --context acp-notprod_DACC -n dacc-entitysearch --token MOCK_TOKEN ",
    ""
  );
  switch (cmd) {
    case "get jobs -o json":
      return { stdout: get_jobs_to_delete(), stderr: null };
    case "get pods neo4j-0 -o json":
      return { stdout: get_pod_status(), stderr: null };
    case "get pods neo4j-1 -o json":
      return { stdout: get_pod_status(), stderr: null };
    case "get pods elasticsearch-0 -o json":
      return { stdout: pod_status_ready, stderr: null };
    case "get pods elasticsearch-1 -o json":
      return { stdout: pod_status_ready, stderr: null };
    case "get jobs neo4j-delta-1538055240 -o json":
      return { stdout: get_job_status(), stderr: null };
    case "get jobs neo4j-bulk-1538055555 -o json":
      return { stdout: complete_job, stderr: null };
    case "get jobs elastic-bulk-1538055555 -o json":
      return { stdout: complete_job, stderr: null };
    case "get jobs neo4j-delta-1538055555 -o json":
      return { stdout: complete_job, stderr: null };
    case "get jobs elastic-delta-1538055555 -o json":
      return { stdout: complete_job, stderr: null };
    case "create job neo4j-bulk-1538055555 --from cronjob/neo4j-bulk":
      return {
        exitcode: jest
          .fn()
          .mockReturnValueOnce(1)
          .mockReturnValue(0)
      };
    case "create job elastic-bulk-1538055555 --from cronjob/elastic-bulk":
      return { exitcode: 0 };
    case "create job neo4j-delta-1538055555 --from cronjob/neo4j-delta":
      return {
        exitcode: jest
          .fn()
          .mockReturnValueOnce(1)
          .mockReturnValue(0)
      };
    case "create job elastic-delta-1538055555 --from cronjob/elastic-delta":
      return { exitcode: 0 };
    case "create job neo4j-delta-1538022222 --from cronjob/neo4j-delta":
      return { exitcode: 1 };
    case "create job neo4j-delta-1538055240 --from cronjob/neo4j-delta":
      return { exitcode: 0 };
    case "create job elastic-delta-1538055240 --from cronjob/elastic-delta":
      return { exitcode: 0 };
    default:
      return { exitcode: 0 };
  }
}

function exec(command, opts = {}, callback) {
  const { stdout, stderr } = getOutput(command);

  if (stdout instanceof Error) {
    callback(stdout);
  } else {
    callback(null, JSON.stringify(stdout), stderr ? JSON.stringify(stderr) : null);
  }
}

function spawn(cmd, cmd_args) {
  const _spawn = new Spawn();
  const { exitcode } = getOutput(cmd + " " + cmd_args.join(" "));
  setTimeout(() => _spawn.emit("exit", exitcode), 10);
  return _spawn;
}

child_process.exec = jest.fn().mockImplementation(exec);
child_process.spawn = jest.fn().mockImplementation(spawn);

module.exports = child_process;
