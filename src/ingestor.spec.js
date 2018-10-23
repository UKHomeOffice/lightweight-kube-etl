jest.mock('child_process');
const child_process = require('child_process');
const ingestor = require('./ingestor');
const {
  getOldJobs,
  deleteOldJobs,
  checkPodStatus,
  checkJobStatus
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
    const jobsToDelete = ["elastic-bulk-1538055000", "neo4j-bulk-1538055000"];
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

describe.only('Kubectl - checkPodStatus', () => {
  it('should wait for a pod to be in a ready state', done => {    
    checkPodStatus('neo4j-0', () => {
      expect(child_process.exec.mock.calls.length).toBe(4);
      child_process.exec.mockClear();
      done();
    });
  });

  it('should wait for a job to finish', done => {    
    checkJobStatus('neo4j-delta-1538055240', () => {
      expect(child_process.exec.mock.calls.length).toBe(3);
      child_process.exec.mockClear();
      done();
    });
  });
});