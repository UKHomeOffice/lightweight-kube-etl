const {
  hasTimestampFolders,
  getIngestJobParams
} = require('../src/ingestor');

const sample_s3_no_ts_folders = {
  Content: [
    {
      Key: 'pending/.DS_Store'
    },
    {
      Key: 'pending/manifest.json'
    }
  ]
};

const sample_s3_ts_folders = {
  Content: [
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
    }
  ]
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
});