jest.mock('child_process');
const R = require('ramda');

const ingestor = require('./ingestor');

const {
  getOldJobs,
  deleteOldJobs
} = ingestor;

const moment = require('moment');

describe('kubectl', () => {
  it('should enter an error state on error', done => {
    
    const spy = jest.spyOn(console, 'error').mockImplementation(() => {});

    const ingestParams = {ingestName: '1538055240', ingestType: 'bulk'};
          
    getOldJobs(ingestParams, () => {}, () => {
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
