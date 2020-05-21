const { head, tail } = require("ramda");

const s3_samples = {
  empty: {
    Contents: [],
  },
  no_ts_folders: {
    Contents: [
      {
        Key: "preprod/.DS_Store",
      },
      {
        Key: "preprod/manifest.json",
      },
    ],
  },
  ts_folders_no_manifest: {
    Contents: [
      {
        Key: "preprod/1538055240/person/person_headers.csv.gz",
      },
      {
        Key: "preprod/1538055240/bulk.txt",
      },
      {
        Key: "preprod/1538055250/person/person_headers.csv.gz",
      },
      {
        Key: "preprod/1538055250/person/person_sample.csv.gz",
      },
    ],
  },
  ts_folders: {
    Contents: [
      {
        Key: "preprod/.DS_Store",
      },
      {
        Key: "preprod/manifest.json",
      },
      {
        Key: "preprod/1538055240/person/person_headers.csv.gz",
      },
      {
        Key: "preprod/1538055240/bulk.txt",
      },
      {
        Key: "preprod/1538055240/manifest.json",
      },
      {
        Key: "preprod/1538055250/person/person_headers.csv.gz",
      },
      {
        Key: "preprod/1538055250/person/person_sample.csv.gz",
      },
    ],
  },
  ts_folders_suffix: {
    Contents: [
      {
        Key: "preprod/.DS_Store",
      },
      {
        Key: "preprod/manifest.json",
      },
      {
        Key: "preprod/1538055240_nam/person/person_headers.csv.gz",
      },
      {
        Key: "preprod/1538055240_nam/bulk.txt",
      },
      {
        Key: "preprod/1538055240_nam/manifest.json",
      },
      {
        Key: "preprod/1538055250_nam/person/person_headers.csv.gz",
      },
      {
        Key: "preprod/1538055250_nam/person/person_sample.csv.gz",
      },
    ],
  },
  bad_folders: {
    Contents: [
      {
        Key: "preprod/.DS_Store",
      },
      {
        Key: "preprod/1538055240/person/person_headers.csv.gz",
      },
    ],
  },
  out_of_order_folders: {
    Contents: [
      {
        Key: "preprod/2222",
      },
      {
        Key: "preprod/2222/bulk.txt",
      },
      {
        Key: "preprod/2222/manifest.json",
      },
      {
        Key: "preprod/1111",
      },
      {
        Key: "preprod/1111/incremental.txt",
      },
      {
        Key: "preprod/3333",
      },
      {
        Key: "preprod/3333/incremental.txt",
      },
    ],
  },
  out_of_order_folders_suffix: {
    Contents: [
      {
        Key: "preprod/2222_nam",
      },
      {
        Key: "preprod/2222_nam/bulk.txt",
      },
      {
        Key: "preprod/2222_nam/manifest.json",
      },
      {
        Key: "preprod/1111",
      },
      {
        Key: "preprod/1111/incremental.txt",
      },
      {
        Key: "preprod/3333_nam",
      },
      {
        Key: "preprod/3333_nam/incremental.txt",
      },
    ],
  },
};

const ts_folders = jest
  .fn()
  .mockReturnValueOnce(new Error("aws error"))
  .mockReturnValueOnce(s3_samples.empty)
  .mockReturnValueOnce(s3_samples.empty)
  .mockReturnValueOnce(s3_samples.no_ts_folders)
  .mockReturnValueOnce(s3_samples.bad_folders)
  .mockReturnValueOnce(s3_samples.ts_folders);

const manifest_folders = jest
  .fn()
  .mockReturnValueOnce(s3_samples.empty)
  .mockReturnValueOnce(s3_samples.no_ts_folders)
  .mockReturnValueOnce(s3_samples.ts_folders_no_manifest)
  .mockReturnValueOnce(s3_samples.ts_folders);

const deleteObjects = jest
  .fn()
  .mockReturnValueOnce(new Error("aws delete error"))
  .mockReturnValue(true);

module.exports = {
  listObjectsV2: jest
    .fn()
    .mockImplementation(({ Bucket, Prefix }, callback) => {
      let reply;
      if (Prefix === "preprod/") {
        reply = ts_folders();
      } else if (Prefix === "preprod/1538055240/manifest.json") {
        reply = manifest_folders();
      }

      reply instanceof Error ? callback(reply) : callback(null, reply);
    }),
  s3_samples: s3_samples,
  deleteObjects: jest
    .fn()
    .mockImplementation(({ Bucket, Delete }, callback) => {
      const reply = deleteObjects();

      if (reply instanceof Error) {
        callback(reply);
      } else {
        callback(null, reply);
      }
    }),
};
