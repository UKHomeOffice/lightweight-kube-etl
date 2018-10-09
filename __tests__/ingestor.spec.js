const ingestor = require('../src/ingestor');
const childProcess = require("child_process");

const {
  isTimestamp,
  hasTimestampFolders,
  getIngestJobParams,
  getJobLabels,
  filterJobs,
  getStatus,
  getPodStatus,
  getIngestFiles,
  getJobDuration
} = ingestor;

const moment = require('moment');

const s3 = {
  listObjectsV2: jest.fn().mockImplementation((params, cb) => {
    return cb(null, sample_s3_ts_folders);
  })
};

childProcess.exec = jest.fn().mockImplementation((command, callback) => {

  return callback(null, "exec success", null);
});

const sample_s3_no_ts_folders = {
  Contents: [
    {
      Key: 'pending/.DS_Store'
    },
    {
      Key: 'pending/manifest.json'
    }
  ]
};

const sample_s3_ts_folders = {
  Contents: [
    {
      Key: 'pending/.DS_Store'
    },
    {
      Key: 'pending/manifest.json'
    },
    {
      Key: 'pending/1538055240/person/person_headers.csv.gz'
    },
    {
      Key: 'pending/1538055240/bulk.txt'
    },
    {
      Key: 'pending/1538055240/manifest.json'
    },
    {
      Key: 'pending/1538055250/person/person_headers.csv.gz'
    },
    {
      Key: 'pending/1538055250/person/person_sample.csv.gz'
    }
  ]
}

const sample_complete_job = {
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

const sample_running_job = {
  "status": {
    "startTime": "2016-09-22T13:56:42Z",
    "active": 1,
  }
}

const sample_pod_status_ready = {
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

const sample_pod_status_not_ready = {
  "status": {
    "containerStatuses": [
      {
        "name": "build",
        "ready": true,
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

const sample_pod_not_ready = {
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

describe('Simple Ingestor Helper Functions', () => {
  it('can tell if something is a timestamp', () => {
    const ts = 1538055250;

    expect(isTimestamp(ts)).toBe(true);
    expect(isTimestamp(null)).toBe(false);
    expect(isTimestamp('str')).toBe(false);
    expect(isTimestamp('1538055250')).toBe(true);
  });
  
  it('can filter timestamped folders from s3', () => {
    expect(hasTimestampFolders(sample_s3_no_ts_folders)).toBe(false);
    expect(hasTimestampFolders(sample_s3_ts_folders)).toBe(true);
  });

  it('extracts the correct folder from a list', () => {
    const {ingestType, ingestName} = getIngestJobParams(sample_s3_ts_folders);

    expect(ingestType).toBe('bulk');
    expect(ingestName).toBe('1538055240');
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

    const label = getJobLabels(type)(result);

    expect(label).toEqual(['neo4j-delta-123456', 'elastic-delta-123456']);
    expect(filterJobs(filterThis)).toBe(false);
    expect(filterJobs(result.items[0])).toBe(true);
  });

  it('can get the status of a job', () => {
    expect(getStatus(sample_running_job)).toBeFalsy();
    expect(getStatus(sample_complete_job)).toBeTruthy();
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

    const files = getIngestFiles(params)(sample_s3_ts_folders);

    expect(files).toEqual(ingest);
  });

  it('should be able to get a job duration', () => {
    const start = moment('1970-01-01T13:00:00');
    const end = moment('1970-01-01T14:30:00');
    const s_zero_pad = moment('1970-01-01T13:00:00');
    const e_zero_pad = moment('1970-01-01T14:09:00');

    expect(getJobDuration(start, end)).toBe('1h:30mins');
    expect(getJobDuration(s_zero_pad, e_zero_pad)).toBe('1h:09mins');
  });

  it('should be able to find the status of a pod', () => {
    const pod_ready = getPodStatus(sample_pod_status_ready);
    const pod_false_ready =  getPodStatus(sample_pod_not_ready);

    expect(pod_ready).toBe(true);
    expect(pod_false_ready).toBe(false);
  });
});
