const moment = require('moment');
const { s3_samples } = require('./__mocks__/s3-client');

const {
  isTimestamp,
  hasTimestampFolders,
  getIngestJobParams,
  getJobLabels,
  filterJobs,
  getStatus,
  getIngestFiles,
  getJobDuration,
  getPodStatus,
  Times
} = require('./helpers');

const complete_job = {
  "status": {
    "conditions": [
      {
        "type": "Complete",
        "status": "True",
        "lastProbeTime": "2016-09-22T13:59:03Z",
        "lastTransitionTime": "2016-09-22T13:59:03Z"
      }
    ],
    "startTime": "2016-09-22T13:56:42Z",
    "completionTime": "2016-09-22T13:59:03Z",
    "succeeded": 1
  }
}

const running_job = {
  "status": {
    "startTime": "2016-09-22T13:56:42Z",
    "active": 1,
  }
}

const pod_status_ready = {
  "status": {
    "containerStatuses": [
      {
        "name": "build",
        "ready": true,
        "restartCount": 0,
        "state": {
            "running": {
                "startedAt": "2018-10-10T10:10:00Z"
            }
        }
      },
      {
        "name": "neo4j",
        "ready": true,
        "restartCount": 0,
        "state": {
            "running": {
                "startedAt": "2018-10-10T10:11:00Z"
            }
        }
      }
    ]
  }
}

const pod_status_not_ready = {
  "status": {
    "containerStatuses": [
      {
        "name": "build",
        "ready": false,
        "restartCount": 0,
        "state": {
            "running": {
                "startedAt": "2018-10-09T10:10:00Z"
            }
        }
      },
      {
        "name": "neo4j",
        "ready": true,
        "restartCount": 0,
        "state": {
            "running": {
                "startedAt": "2018-10-10T10:11:00Z"
            }
        }
      }
    ]
  }
}

const pod_not_ready = {
  "status": {
    "containerStatuses": [
      {
        "name": "build",
        "ready": false,
        "restartCount": 0,
        "state": {
            "running": {
                "startedAt": "2018-10-09T10:10:00Z"
            }
        }
      }
    ]
  }
}

describe('Helper Functions', () => {
  it('can tell if something is a timestamp', () => {
    const ts = 1538055250;

    expect(isTimestamp(ts)).toBe(true);
    expect(isTimestamp(null)).toBe(false);
    expect(isTimestamp('str')).toBe(false);
    expect(isTimestamp('1538055250')).toBe(true);
  });
  
  it('can filter timestamped folders from s3', () => {
    expect(hasTimestampFolders(s3_samples.no_ts_folders)).toBe(false);
    expect(hasTimestampFolders(s3_samples.ts_folders)).toBe(true);
  });

  it('extracts the oldest timestamped folder from a list', () => {
    const {ingestType, ingestName} = getIngestJobParams(s3_samples.ts_folders);

    expect(ingestType).toBe('bulk');
    expect(ingestName).toBe('1538055240');
  });

  it('extracts the oldest timestamped folder from a list', () => {
    const {ingestType, ingestName} = getIngestJobParams(s3_samples.out_of_order_folders);

    expect(ingestType).toBe('incremental');
    expect(ingestName).toBe('1111');
  });

  it('handles mis-formed folder contents', () => {
    const response = getIngestJobParams(s3_samples.bad_folders);

    expect(response).toBeFalsy();
  });

  it('extracts job labels from kubectl responses', () => {
    const result = {
      items: [
        {
          metadata: {
            name: 'neo4j-bulk-123456'
          }
        },
        {
          metadata: {
            name: 'neo4j-delta-123456'
          }
        },
        {
          metadata: {
            name: 'elastic-bulk-123456'
          }
        },
        {
          metadata: {
            name: 'elastic-delta-123456'
          }
        }
      ]
    };

    const filterThis = {
      metadata: {
        name: 'download-job'
      }
    }

    const type = new RegExp(/-delta-/);

    const label = getJobLabels(type);

    expect(typeof label).toBe('function');
    expect(label(result)).toEqual(['neo4j-delta-123456', 'elastic-delta-123456']);
    expect(filterJobs(filterThis)).toBe(false);
    expect(filterJobs(result.items[0])).toBe(true);
  });

  it('can get the status of a job', () => {
    expect(getStatus(running_job)).toBeFalsy();
    expect(getStatus(complete_job)).toBeTruthy();
  });

  it('can get all the files and folders for an ingest', () => {
    const ingest = [
      {Key: 'pending/1538055250/manifest.json'},
      {Key: 'pending/1538055250'},
      {Key: 'pending/1538055250/person/person_headers.csv.gz'},
      {Key: 'pending/1538055250/person/person_sample.csv.gz'}
    ];

    const params = {
      ingestType: 'bulk',
      ingestName: '1538055250'
    }

    const files = getIngestFiles(params)(s3_samples.ts_folders);

    expect(files).toEqual(ingest);
  });

  it('should be able to get a job duration', () => {
    const start = moment('1970-01-01T13:00:00');
    const end = moment('1970-01-01T14:30:00');
    const s_zero_pad = moment('1970-01-01T13:00:00');
    const e_zero_pad = moment('1970-01-01T14:09:00');

    expect(getJobDuration(start, end)).toBe('1h:30mins');
    expect(getJobDuration(s_zero_pad, e_zero_pad)).toBe('1h:09mins');
    expect(getJobDuration({}, {})).toBe('timestamp error');
  });

  it('should be able to find the status of a pod', () => {
    const pod_ready = getPodStatus(pod_status_ready);
    const pod_false_ready =  getPodStatus(pod_not_ready);

    expect(pod_ready).toBe(true);
    expect(pod_false_ready).toBe(false);
  });
});

describe('Times', () => {
  it('should initate with no times set', () => {
    const times = new Times();
    expect(times.getNeoStart()).toBe(null);
    expect(times.getElasticStart()).toBe(null);
    expect(times.getNeoEnd()).toBe(null);
    expect(times.getElasticEnd()).toBe(null);
  });

  it('should be able to get/set a time', () => {
    const times = new Times();

    times.setNeoStart();

    const time = times.getNeoStart();

    expect(moment(time).isValid()).toBe(true);
    expect(time instanceof moment).toBe(true);
  });

  it('should hold the completed state', () => {
    const times = new Times();

    times.setNeoEnd();
    times.setElasticEnd();

    expect(times.isComplete()).toBe(true);
  });
})

module.exports = {
  complete_job,
  running_job,
  pod_status_ready,
  pod_status_not_ready,
  pod_not_ready
}
