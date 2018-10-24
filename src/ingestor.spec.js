jest.mock('child_process');
const child_process = require('child_process');
const ingestor = require('./ingestor');
const {
  getOldJobs,
  deleteOldJobs,
  checkPodStatus,
  checkJobStatus,
  waitForPods,
  runJob,
  createBulkJobs,
  createDeltaJobs,
  enterErrorState,
  waitForCompletion
} = ingestor;

const moment = require('moment');
const noop = () => {};

describe('Kubectl - getOldJobs', () => {
  it('should, on error, enter an error state', done => {
    
    const spy = jest.spyOn(console, 'error').mockImplementation(noop);
      
    getOldJobs({}, noop, () => {
      expect(spy.mock.calls[0][0].message).toBe('kubectl error');
      console.error.mockRestore();
      done();
    });
  });

  it('should get jobs to delete for a bulk', done => {
    const ingestParams = {ingestName: '1538055240', ingestType: 'bulk'};
    getOldJobs(ingestParams, (params, jobs) => {
      
      expect(jobs.length).toBe(2);
      expect(jobs).toEqual(expect.arrayContaining(["elastic-bulk-1538055000", "neo4j-bulk-1538055000"]));
      expect(params).toEqual(ingestParams);
      
      done();
    });
  });

  it('should get jobs to delete for a delta', done => {
    const ingestParams = {ingestName: '1538055240', ingestType: 'incremental'};
    getOldJobs(ingestParams, (params, jobs) => {
      
      expect(jobs.length).toBe(2);
      expect(jobs).toEqual(expect.arrayContaining(["elastic-delta-1537362006", "elastic-delta-1537362006"]));
      expect(jobs).not.toEqual(expect.arrayContaining(["some-other-important-job"]));
      expect(params).toEqual(ingestParams);
      
      done();
    });
  });
});

describe('Kubectl - deleteOldJobs', () => {
  it('should call createBulkJobs when the ingest is a bulk', done => {
    const ingestParams = {ingestName: '1538055240', ingestType: 'bulk'};
    const jobsToDelete = ["elastic-bulk-1538055000", "neo4j-bulk-1538055000"];
    const expected_jobs = [
      {
        db: 'neo4j',
        name: 'neo4j-bulk-1538055240',
        cronJobName: 'neo4j-bulk',
        pods: [ 'neo4j-0', 'neo4j-1' ] 
      },
      {
        db: 'elastic',
        name: 'elastic-bulk-1538055240',
        cronJobName: 'elastic-bulk',
        pods: [ 'elasticsearch-0', 'elasticsearch-1' ]
      }
    ];

    deleteOldJobs(ingestParams, jobsToDelete, (_ingestParams, jobs) => {
      expect(ingestParams).toEqual(_ingestParams);
      expect(jobs).toEqual(expected_jobs);
      done();
    }, noop);
  });

  it('should call createDeltaJobs when the ingest is a delta', done => {
    const ingestParams = {ingestName: '1538055240', ingestType: 'incremental'};
    const jobsToDelete = ["elastic-delta-1538055000", "neo4j-delta-1538055000"];
    const expected_jobs = [
      {
        db: 'neo4j',
        name: 'neo4j-delta-1538055240',
        cronJobName: 'neo4j-delta',
        pods: [ 'neo4j-0', 'neo4j-1' ] 
      },
      {
        db: 'elastic',
        name: 'elastic-delta-1538055240',
        cronJobName: 'elastic-delta',
        pods: [ 'elasticsearch-0', 'elasticsearch-1' ]
      }
    ];

    deleteOldJobs(ingestParams, jobsToDelete, noop, (_ingestParams, jobs) => {
      expect(ingestParams).toEqual(_ingestParams);
      expect(jobs).toEqual(expected_jobs);
      done();
    });
  });
});

describe('Kubectl - checkPodStatus', () => {
  beforeAll(child_process.exec.mockClear);

  it('should wait for a pod to be in a ready state', done => {    
    checkPodStatus('neo4j-0', () => {
      expect(child_process.exec.mock.calls.length).toBe(4);
      done();
    });
  });

});

describe('Kubectl - checkJobStatus', () => {
  beforeAll(child_process.exec.mockClear);

  it('should wait for a job to finish', done => {    
    checkJobStatus('neo4j-delta-1538055240', () => {
      expect(child_process.exec.mock.calls.length).toBe(4);
      done();
    });
  });

});

describe('Kubectl - waitForPods', () => {
  it('should wait for both pods to be ready', done => {
    child_process.exec.mockClear();

    const job = {
      db: 'neo4j',
      name: 'neo4j-delta-1538055240',
      cronJobName: 'neo4j-delta',
      pods: [ 'neo4j-0', 'neo4j-1' ] 
    };

    waitForPods(job, err => {
      expect(err).toBeFalsy();
      expect(child_process.exec.mock.calls.length).toBe(2);
      done();
    });
  });
});

describe('Kubectl - runJob', () => {
  beforeAll(child_process.spawn.mockClear);

  it('should handle errors', done => {
    const job = {
      db: 'neo4j',
      name: 'neo4j-delta-1538022222',
      cronJobName: 'neo4j-delta',
      pods: [ 'neo4j-0', 'neo4j-1' ] 
    };

    runJob(job, err => {
      expect(err instanceof Error).toBe(true);
      expect(err.message).toBe('neo4j-delta-1538022222 exits with non zero code');
      done();
    })
  });

  it('should trigger correct job', done => {
    const console_log = jest.spyOn(console, 'log').mockImplementation(noop);

    const job = {
      db: 'neo4j',
      name: 'neo4j-delta-1538055240',
      cronJobName: 'neo4j-delta',
      pods: [ 'neo4j-0', 'neo4j-1' ] 
    };

    runJob(job, err => {
      const log = console_log.mock.calls[0][0].split(': ')[1];

      expect(err).toBeFalsy();
      expect(log).toBe('neo4j-delta-1538055240 triggered :)');
      console.log.mockRestore();
      done();
    })
  });
});

describe('Kubectl - createBulkJobs', () => {
  const bulkjobs = [
    {
      db: 'neo4j',
      name: 'neo4j-bulk-1538055555',
      cronJobName: 'neo4j-bulk',
      pods: [ 'neo4j-0', 'neo4j-1' ] 
    },
    {
      db: 'elastic',
      name: 'elastic-bulk-1538055555',
      cronJobName: 'elastic-bulk',
      pods: [ 'elasticsearch-0', 'elasticsearch-1' ]
    }
  ];

  it('should handle errors', done => {
    const ingestParams = {ingestName: '1538055555', ingestType: 'bulk'};

    createBulkJobs(ingestParams, bulkjobs, err => {
      expect(err instanceof Error).toBe(true);
      done();
    });
  })

  it('should create bulk jobs and trigger them', done => {
    const ingestParams = {ingestName: '1538055555', ingestType: 'bulk'};
    const console_spy = jest.spyOn(console, 'log').mockImplementation(noop);
    
    createBulkJobs(ingestParams, bulkjobs, (err, _ingestParams) => {
      expect(ingestParams).toEqual(_ingestParams);

      const logs = console_spy.mock.calls.map(([msg]) => {
        return msg.split(': ')[1];
      });

      const expected_logs = [
        "neo4j-bulk-1538055555 triggered :)",
        "elastic-bulk-1538055555 triggered :)",
        "neo4j-bulk-1538055555 pods ready",
        "elastic-bulk-1538055555 pods ready"
      ]

      expect(logs.every(log => expected_logs.indexOf(log) > -1)).toBe(true);
      console.log.mockRestore();
      done();
    });
  })
});

describe('Kubectl - createDeltaJobs', () => {
  const deltajobs = [
    {
      db: 'neo4j',
      name: 'neo4j-delta-1538055555',
      cronJobName: 'neo4j-delta',
      pods: [ 'neo4j-0', 'neo4j-1' ] 
    },
    {
      db: 'elastic',
      name: 'elastic-delta-1538055555',
      cronJobName: 'elastic-delta',
      pods: [ 'elasticsearch-0', 'elasticsearch-1' ]
    }
  ];

  it('should handle errors', done => {
    const ingestParams = {ingestName: '1538055555', ingestType: 'incremental'};

    createDeltaJobs(ingestParams, deltajobs, err => {
      expect(err instanceof Error).toBe(true);
      done();
    });
  })

  it('should create bulk jobs and trigger them', done => {
    const ingestParams = {ingestName: '1538055555', ingestType: 'incremental'};
    const console_spy = jest.spyOn(console, 'log').mockImplementation(noop);
    
    createDeltaJobs(ingestParams, deltajobs, (err, _ingestParams) => {
      expect(ingestParams).toEqual(_ingestParams);

      const logs = console_spy.mock.calls.map(([msg]) => {
        return msg.split(': ')[1];
      });

      const expected_logs = [
        "neo4j-delta-1538055555 triggered :)",
        "elastic-delta-1538055555 triggered :)",
        "neo4j-delta-1538055555 pods ready",
        "elastic-delta-1538055555 pods ready"
      ]

      expect(logs.every(log => expected_logs.indexOf(log) > -1)).toBe(true);
      console.log.mockRestore();
      done();
    });
  })
});

describe('Kubectl - waitForCompletion', () => {
  it('should wait for jobs to complete and', () => {
    expect(enterErrorState()).toBeTruthy();
  })
})
