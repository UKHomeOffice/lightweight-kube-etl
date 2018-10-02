const {
  hasTimestampFolders,
  getIngestJobParams,
  getStatus,
  getIngestFiles,
  getJobDuration
} = require('../src/ingestor');

const m = require('moment');

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

describe('The Entity Search Ingestor', () => {
  it('ingests timestamped folders from s3', () => {
    expect(hasTimestampFolders(sample_s3_no_ts_folders)).toBe(false);
    expect(hasTimestampFolders(sample_s3_ts_folders)).toBe(true);
  });

  it('extracts the correct folder from a list', () => {
    const {ingestType, ingestName} = getIngestJobParams(sample_s3_ts_folders);

    expect(ingestType).toBe('bulk');
    expect(ingestName).toBe('1538055240');
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
    const start = m('1970-01-01T13:00:00');
    const end = m('1970-01-01T14:30:00');

    expect(getJobDuration(start, end)).toBe('1:30');
  })
});