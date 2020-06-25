jest.mock("child_process");
jest.mock("./s3-client");
jest.mock("./mongodb");
const events = require("events");
const moment = require("moment");
const child_process = require("child_process");
const { insert: mongoClient } = require("./mongodb");
const ingestor = require("./ingestor");
const { getIngestFiles } = require("./helpers");
const { s3_samples } = require("./__mocks__/s3-client");
const noop = () => {};
const {
  getOldJobs,
  deleteOldJobs,
  checkPodStatus,
  checkJobStatus,
  checkRollingStatus,
  waitForPods,
  runJob,
  createBulkJobs,
  createDeltaJobs,
  enterErrorState,
  waitForCompletion,
  start,
} = ingestor;

const pod_status_ready = {
  status: {
    containerStatuses: [
      {
        name: "build",
        ready: true,
        restartCount: 0,
        state: {
          running: {
            startedAt: "2020-10-10T10:10:00Z",
          },
        },
      },
      {
        name: "neo4j",
        ready: true,
        restartCount: 0,
        state: {
          running: {
            startedAt: "2020-10-10T10:11:00Z",
          },
        },
      },
    ],
  },
};

const complete_job = {
  status: {
    conditions: [
      {
        type: "Complete",
        status: "True",
        lastProbeTime: "2016-09-22T13:59:03Z",
        lastTransitionTime: "2016-09-22T13:59:03Z",
      },
    ],
    startTime: "2016-09-22T13:56:42Z",
    completionTime: "2016-09-22T13:59:03Z",
    succeeded: 1,
  },
};

const mockExec = (count = 1, data = {}) => {
  for (var i = 0; i < count; i++) {
    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(false, JSON.stringify(data))
    );
  }
};

describe("Kubectl - getOldJobs", () => {
  it("should, on error, enter an error state", (done) => {
    const errorMessage = "kubectl error";
    
    const spy = jest.spyOn(console, "error").mockImplementation(noop);

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(errorMessage)
    );
      
    getOldJobs({}, noop, () => {
      expect(spy.mock.calls[0][0]).toBe(errorMessage);
      console.error.mockRestore();
      done();
    });
  });

  it("should get jobs to delete for a bulk", (done) => {
    const ingestParams = { ingestName: "1538055000", ingestType: "bulk" };

    const testJSON = {
      items: [
        {
          metadata: {
            name: "elastic-bulk-1538055000",
          },
        },
        {
          metadata: {
            name: "neo4j-bulk-1538055000",
          },
        },
      ],
    };

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(testJSON))
    );

    getOldJobs(ingestParams, (params, jobs) => {
      expect(jobs.length).toBe(2);
      expect(jobs).toEqual(
        expect.arrayContaining([
          "elastic-bulk-1538055000",
          "neo4j-bulk-1538055000",
        ])
      );
      expect(params).toEqual(ingestParams);

      done();
    });
  });

  it("should get jobs to delete for a bulk with suffix", (done) => {
    const ingestParams = { ingestName: "1538055000_nam", ingestType: "bulk" };

    const testJSON = {
      items: [
        {
          metadata: {
            name: "elastic-bulk-1538055000_nam",
          },
        },
        {
          metadata: {
            name: "neo4j-bulk-1538055000_nam",
          },
        },
      ],
    };

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(testJSON))
    );

    getOldJobs(ingestParams, (params, jobs) => {
      expect(jobs.length).toBe(2);
      expect(jobs).toEqual(
        expect.arrayContaining([
          "elastic-bulk-1538055000_nam",
          "neo4j-bulk-1538055000_nam",
        ])
      );
      expect(params).toEqual(ingestParams);
      
      done();
    });
  });

  it("should get jobs to delete for a delta", (done) => {
    const ingestParams = {
      ingestName: "1537362006",
      ingestType: "incremental",
    };

    const testJSON = {
      items: [
        {
          metadata: {
            name: "elastic-delta-1537362006",
          },
        },
        {
          metadata: {
            name: "neo4j-delta-1537362006",
          },
        },
      ],
    };

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(testJSON))
    );
    getOldJobs(ingestParams, (params, jobs) => {
      expect(jobs.length).toBe(2);
      expect(jobs).toEqual(
        expect.arrayContaining([
          "elastic-delta-1537362006",
          "elastic-delta-1537362006",
        ])
      );
      expect(jobs).not.toEqual(
        expect.arrayContaining(["some-other-important-job"])
      );
      expect(params).toEqual(ingestParams);

      done();
    });
  });

  it("should get jobs to delete for a delta with suffix", (done) => {
    const ingestParams = {
      ingestName: "1537362006_nam",
      ingestType: "incremental",
    };

    const testJSON = {
      items: [
        {
          metadata: {
            name: "elastic-delta-1537362006_nam",
          },
        },
        {
          metadata: {
            name: "neo4j-delta-1537362006_nam",
          },
        },
      ],
    };

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(testJSON))
    );

    getOldJobs(ingestParams, (params, jobs) => {
      expect(jobs.length).toBe(2);
      expect(jobs).toEqual(
        expect.arrayContaining([
          "elastic-delta-1537362006_nam",
          "elastic-delta-1537362006_nam",
        ])
      );
      expect(jobs).not.toEqual(
        expect.arrayContaining(["some-other-important-job"])
      );
      expect(params).toEqual(ingestParams);
      
      done();
    });
  });
});

describe("Kubectl - deleteOldJobs", () => {
  it("should call createBulkJobs when the ingest is a bulk", (done) => {
    const ingestParams = { ingestName: "1538055240", ingestType: "bulk" };
    const jobsToDelete = ["elastic-bulk-1538055000", "neo4j-bulk-1538055000"];
    const expected_jobs = [
      {
        db: "neo4j",
        name: "neo4j-bulk-1538055240",
        cronJobName: "neo4j-bulk",
        pods: ["neo4j-0","neo4j-1"],
      },
      {
        db: "elastic",
        name: "elastic-bulk-1538055240",
        cronJobName: "elastic-bulk",
        pods: ["elasticsearch-0", "elasticsearch-1"],
      },
    ];

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(),
    }));

    deleteOldJobs(
      ingestParams,
      jobsToDelete,
      (_ingestParams, jobs) => {
      expect(ingestParams).toEqual(_ingestParams);
      expect(jobs).toEqual(expected_jobs);
      done();
      },
      noop
    );
  });

  it("should call createDeltaJobs when the ingest is a delta", (done) => {
    const ingestParams = {
      ingestName: "1538055240",
      ingestType: "incremental",
    };
    const jobsToDelete = ["elastic-delta-1538055000", "neo4j-delta-1538055000"];
    const expected_jobs = [
      {
        db: "neo4j",
        name: "neo4j-delta-1538055240",
        cronJobName: "neo4j-delta",
        pods: ["neo4j-0","neo4j-1"],
      },
      {
        db: "elastic",
        name: "elastic-delta-1538055240",
        cronJobName: "elastic-delta",
        pods: ["elasticsearch-0", "elasticsearch-1"],
      },
    ];

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(),
    }));

    deleteOldJobs(ingestParams, jobsToDelete, noop, (_ingestParams, jobs) => {
      expect(ingestParams).toEqual(_ingestParams);
      expect(jobs).toEqual(expected_jobs);
      done();
    });
  });
});

describe("Kubectl - checkPodStatus", () => {
  beforeAll(child_process.exec.mockClear);

  it("should wait for a pod to be in a ready state", (done) => {
    child_process.exec.mockImplementationOnce((command, callback) =>
      callback()
    );
    child_process.exec.mockImplementationOnce((command, callback) =>
      callback()
    );
    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(pod_status_ready))
    );

    checkPodStatus("neo4j-0", () => {
      expect(child_process.exec.mock.calls.length).toBe(3);
      done();
    });
  });
});

describe("Kubectl - checkRollingStatus", () => {
  beforeAll(child_process.exec.mockClear);

  it("should wait for a rolling update to complete", (done) => {
    for (var i = 0; i < 3; i++) {
      child_process.exec.mockImplementationOnce((command, callback) =>
        callback("err")
      );
    }
    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(pod_status_ready))
    );

    const jobStartTime = moment("2018-10-10T10:05:00Z");
    checkRollingStatus("neo4j-0", jobStartTime, () => {
      expect(child_process.exec.mock.calls.length).toBe(4);
      expect(moment("2018-10-10T10:10:00Z").isAfter(jobStartTime)).toBe(true);
      done();
    });
  });
});

describe("Kubectl - checkJobStatus", () => {
  beforeAll(child_process.exec.mockClear);

  it("should wait for a job to finish", (done) => {
    const running_job = {
      status: {
        startTime: "2016-09-22T13:56:42Z",
        active: 1,
      },
    };

    for (var i = 0; i < 2; i++) {
      child_process.exec.mockImplementationOnce((command, callback) =>
        callback("err")
      );
    }

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(running_job))
    );

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(null, JSON.stringify(complete_job))
    );

    checkJobStatus("neo4j-delta-1538055240", () => {
      expect(child_process.exec.mock.calls.length).toBe(4);
      done();
    });
  });
});

describe("Kubectl - waitForPods", () => {
  beforeAll(child_process.exec.mockClear);

  it("should wait for both pods to be ready", (done) => {
    const job = {
      db: "neo4j",
      name: "neo4j-delta-1538055240",
      cronJobName: "neo4j-delta",
      pods: ["neo4j-0", "neo4j-1"],
    };

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(false, JSON.stringify(pod_status_ready))
    );

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(false, JSON.stringify(pod_status_ready))
    );

    waitForPods(job, (err) => {
      //      expect(err).toBeFalsy();
      expect(child_process.exec.mock.calls.length).toBe(2);
      done();
    });
  });
});

describe("Kubectl - runJob", () => {
  beforeEach(() => {
    child_process.exec.mockClear();
    child_process.spawn.mockClear();

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(false, JSON.stringify(pod_status_ready))
    );

    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(false, JSON.stringify(pod_status_ready))
    );
  });

  it("should handle errors", (done) => {
    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(1),
    }));

    const job = {
      db: "neo4j",
      name: "neo4j-delta-1538022222",
      cronJobName: "neo4j-delta",
      pods: ["neo4j-0", "neo4j-1"],
    };

    const timer = {};

    runJob(job, timer, (err) => {
      expect(err instanceof Error).toBe(true);
      expect(err.message).toBe(
        "neo4j-delta-1538022222 exits with non zero code"
      );
      done();
    });
  });

  it("should trigger correct job", (done) => {
    child_process.exec.mockImplementationOnce((command, callback) =>
      callback(false, JSON.stringify(complete_job))
    );

    for (var i = 0; i < 3; i++) {
      child_process.exec.mockImplementationOnce((command, callback) =>
        callback(false, JSON.stringify(pod_status_ready))
      );
    }

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(0),
    }));

    const console_log = jest.spyOn(console, "log").mockImplementation(noop);

    const job = {
      db: "neo4j",
      name: "neo4j-delta-1538055240",
      cronJobName: "neo4j-delta",
      pods: ["neo4j-0", "neo4j-1"],
    };

    const timer = {
      setNeoStart: jest.fn(),
      setNeoEnd: jest.fn(),
      getNeoStart: () => moment("2018-10-10T10:05:00Z"),
    };

    runJob(job, timer, (err) => {
      const log = console_log.mock.calls[0][0].split(": ")[1];

      expect(err).toBeFalsy();
      expect(log).toBe("neo4j-delta-1538055240 triggered :)");
      console.log.mockRestore();
      done();
    });
  });
});

describe("Kubectl - createBulkJobs", () => {
  const bulkjobs = [
    {
      db: "neo4j",
      name: "neo4j-bulk-1538055555",
      cronJobName: "neo4j-bulk",
      pods: ["neo4j-0", "neo4j-1"],
    },
    {
      db: "elastic",
      name: "elastic-bulk-1538055555",
      cronJobName: "elastic-bulk",
      pods: ["elasticsearch-0", "elasticsearch-1"],
    },
  ];

  beforeEach(() => {
    child_process.exec.mockClear();
    child_process.spawn.mockClear();
  });

  it("should handle errors", (done) => {
    mockExec(4, pod_status_ready);

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(1),
    }));

    const ingestParams = { ingestName: "1538055555", ingestType: "bulk" };

    createBulkJobs(ingestParams, bulkjobs, (err) => {
      expect(err instanceof Error).toBe(true);
      done();
    });
  });

  it("should create bulk jobs and trigger them", async (done) => {
    const ingestParams = { ingestName: "1538055555", ingestType: "bulk" };
    const console_spy = jest.spyOn(console, "log").mockImplementation(noop);

    mockExec(4, pod_status_ready);
    mockExec(3, complete_job);
    mockExec(6, pod_status_ready);

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(0),
    }));

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(0),
    }));
    
    createBulkJobs(ingestParams, bulkjobs, (err, _ingestParams) => {
      expect(ingestParams).toEqual(_ingestParams);

      const logs = console_spy.mock.calls.map(([msg]) => {
        return msg.split(": ")[1];
      });

      const expected_logs = [
        "neo4j-bulk-1538055555 triggered :)",
        "elastic-bulk-1538055555 triggered :)",
        "elastic-bulk-1538055555 triggered :)",
        "neo4j-bulk-1538055555 pods ready",
        "elastic-bulk-1538055555 pods ready",
        "elastic-bulk-1538055555 pods ready",
      ];

      expect(logs.every((log) => expected_logs.indexOf(log) > -1)).toBe(true);
      console.log.mockRestore();
      console_spy.mockClear;
      done();
    });
  });
});

describe("Kubectl - createDeltaJobs", () => {
  const deltajobs = [
    {
      db: "neo4j",
      name: "neo4j-delta-1538055555",
      cronJobName: "neo4j-delta",
      pods: ["neo4j-0", "neo4j-1"],
    },
    {
      db: "elastic",
      name: "elastic-delta-1538055555",
      cronJobName: "elastic-delta",
      pods: ["elasticsearch-0", "elasticsearch-1"],
    },
  ];

  const deltajobs_suffix = [
    {
      db: "neo4j",
      name: "neo4j-delta-1538055555_nam",
      cronJobName: "neo4j-delta",
      pods: ["neo4j-0", "neo4j-1"],
    },
    {
      db: "elastic",
      name: "elastic-delta-1538055555_nam",
      cronJobName: "elastic-delta",
      pods: ["elasticsearch-0", "elasticsearch-1"],
    },
  ];

  beforeEach(() => {
    child_process.exec.mockClear();
    child_process.spawn.mockClear();
  });

  it("should handle errors", (done) => {
    mockExec(4, pod_status_ready);

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(1),
    }));

    const ingestParams = {
      ingestName: "1538055555",
      ingestType: "incremental",
    };

    createDeltaJobs(ingestParams, deltajobs, (err) => {
      expect(err instanceof Error).toBe(true);
      done();
    });
  });

  it("should create delta jobs and trigger them", (done) => {
    mockExec(2, pod_status_ready);
    mockExec(1, complete_job);
    mockExec(2, pod_status_ready);
    mockExec(2, pod_status_ready);
    mockExec(1, complete_job);
    mockExec(2, pod_status_ready);

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(0),
    }));

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(0),
    }));

    const ingestParams = {
      ingestName: "1538055555",
      ingestType: "incremental",
    };
    const console_spy = jest.spyOn(console, "log").mockImplementation(noop);
    
    createDeltaJobs(ingestParams, deltajobs, (err, _ingestParams) => {
      expect(ingestParams).toEqual(_ingestParams);

      const logs = console_spy.mock.calls.map(([msg]) => {
        return msg.split(": ")[1];
      });

      const expected_logs = [
        "neo4j-delta-1538055555 triggered :)",
        "elastic-delta-1538055555 triggered :)",
        "neo4j-delta-1538055555 pods ready",
        "elastic-delta-1538055555 pods ready",
      ];

      expect(logs.every((log) => expected_logs.indexOf(log) > -1)).toBe(true);
      console.log.mockRestore();
      console_spy.mockClear;
      done();
    });
  });

  it("should create delta jobs with suffix and trigger them", (done) => {
    mockExec(2, pod_status_ready);
    mockExec(1, complete_job);
    mockExec(2, pod_status_ready);
    mockExec(2, pod_status_ready);
    mockExec(1, complete_job);
    mockExec(2, pod_status_ready);

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(0),
    }));

    child_process.spawn.mockImplementationOnce(() => ({
      on: (command, callback) => callback(0),
    }));

    const ingestParams = {
      ingestName: "1538055555_nam",
      ingestType: "incremental",
    };
    const console_spy = jest.spyOn(console, "log").mockImplementation(noop);

    createDeltaJobs(ingestParams, deltajobs_suffix, (err, _ingestParams) => {
      expect(ingestParams).toEqual(_ingestParams);

      const logs = console_spy.mock.calls.map(([msg]) => {
        return msg.split(": ")[1];
      });

      const expected_logs = [
        "neo4j-delta-1538055555_nam triggered :)",
        "elastic-delta-1538055555_nam triggered :)",
        "neo4j-delta-1538055555_nam pods ready",
        "elastic-delta-1538055555_nam pods ready",
      ];

      expect(logs.every((log) => expected_logs.indexOf(log) > -1)).toBe(true);
      console.log.mockRestore();
      console_spy.mockClear;
      done();
    });
  });
});

describe("Kubectl - waitForCompletion", () => {
  it("should wait for completion and handle errors", (done) => {
    const _files = getIngestFiles({
      ingestName: "1538055240",
      ingestType: "bulk",
    })(s3_samples.ts_folders);
    const timer = {
      isComplete: jest
        .fn()
        .mockReturnValueOnce(false)
        .mockReturnValue(true),
      getIngestFiles: jest.fn().mockReturnValue(_files),
    };
    
    const ingestParams = {
      ingestName: "1538055555",
      ingestType: "incremental",
    };

    waitForCompletion(null, ingestParams, timer, (err) => {
      expect(err instanceof Error).toBe(true);
      done();
    });
  });

  it("should wait for completion and handle errors", (done) => {
    const _files = getIngestFiles({
      ingestName: "1538055240",
      ingestType: "bulk",
    })(s3_samples.ts_folders);
    
    const timer = {
      isComplete: jest.fn().mockReturnValue(true),
      getIngestFiles: jest.fn().mockReturnValue(_files),
      getNeoStart: () => moment("1970-01-01 13:00"),
      getNeoEnd: () => moment("1970-01-01 15:15"),
      getElasticStart: () => moment("1970-01-01 13:30"),
      getElasticEnd: () => moment("1970-01-01 14:35"),
      reset: jest.fn(),
    };
    
    const ingestParams = {
      ingestName: "1538055555",
      ingestType: "incremental",
    };

    const expected_output = {
      ingest: "1538055555",
      type: "incremental",
      load_date: 1540394977882,
      readable_date: "Fri Oct 24th 04:29pm",
      neo_job_duration: "2h:15mins",
      elastic_job_duration: "1h:05mins",
      total_job_duration: "2h:29mins",
    };

    waitForCompletion(null, ingestParams, timer, (err) => {
      const insert = mongoClient.mock.calls[0][0];
      const fields = Object.keys(insert);
      expect(fields).toEqual(Object.keys(expected_output));
      expect(insert.type).toEqual(expected_output.type);
      expect(insert.neo_job_duration).toEqual(expected_output.neo_job_duration);
      expect(insert.elastic_job_duration).toEqual(
        expected_output.elastic_job_duration
      );
      done();
    });
  });
});

describe("Error states", () => {
  it("start should enter an error state", () => {
    expect(start(new Error())).toBe(undefined);
  });

  it("waitForCompletion should enter an error state", () => {
    const ingestParams = {
      ingestName: "1538055555",
      ingestType: "incremental",
    };
    expect(waitForCompletion(new Error(), ingestParams, {}, noop)).toBe(true);
  });
});
