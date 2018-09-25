"use strict";

const s3 = require("../src/s3")

global.console.log = jest.fn();

describe("s3", () => {

  let res;

  afterAll(() => s3.client.mockClear())

  describe("getObjectHash", () => {
    it("should return the Etag of an object", async () => {
      s3.client.headObject = jest.fn().mockImplementation(() => ({
        promise: () =>
          Promise.resolve({
            ETag: "ba6119931c7010138eec96d9fb75701865908286"
          })
      }))

      res = await s3.getObjectHash("foo", "bar")
      expect(res).toEqual("ba6119931c7010138eec96d9fb75701865908286")

    })
  })

  describe("getManifest", () => {
    it("should get the manifest.json", async () => {
      s3.client.headObject = jest.fn().mockImplementation(() => ({
        promise: () =>
          Promise.resolve({
            ETag: "ba6119931c7010138eec96d9fb75701865908286"
          })
      }))
      s3.client.getObject = jest.fn().mockImplementation(() => ({
        promise: () =>
          Promise.resolve({
            AcceptRanges: "bytes",
            LastModified: "2018-04-26T16:16:21.000Z",
            ContentLength: 498,
            ETag: '"xxxxxxx"',
            VersionId: "xxxxxxx",
            ContentType: "application/octet-stream",
            ServerSideEncryption: "aws:kms",
            Metadata: {},
            SSEKMSKeyId: "arn:aws:kms:eu-west-2:xxx:key/xxx",
            Body: Buffer.from(
              JSON.stringify([
                {
                  FileName: "testfile1a1.csv.gz",
                  SHA256: "ba6119931c7010138eec96d9fb75701865908286"
                }
              ])
            )
          })
      }))

      res = await s3.getManifest("foo", "bar")
      expect(res).toEqual({
        data: [
          {
            FileName: "testfile1a1.csv.gz",
            SHA256: "ba6119931c7010138eec96d9fb75701865908286"
          }
        ]
      })
    })
  })

  describe("getJobType", () => {
    it("should return bulk if a bulk.txt file is found in the manifest.json directory", async () => {
      s3.client.headObject = jest
        .fn()
        .mockImplementationOnce(() => ({
          promise: () => Promise.reject(new Error("Some error"))
        }))
        .mockImplementationOnce(() => ({
          promise: () =>
            Promise.resolve({
              ETag: "ba6119931c7010138eec96d9fb75701865908286"
            })
        }))
      res = await s3.getIngestType("foo", "pending/123") //?
      expect(res).toEqual("bulk")
    })

    it("should return undefined if none of the jobs files are  found in the manifest.json directory", async () => {
      s3.client.headObject = jest.fn().mockImplementation(() => ({
        promise: () => Promise.reject(new Error("Some error"))
      }))
      res = await s3.getIngestType("foo", "pending/123") //?
      expect(res).toEqual(undefined)
    })
  })

    describe("when asked to list contents given a path", () => {

        const mockS3Response = [
            { Key: "pending/1537362018/bulk.txt" },
            { Key: "pending/1537362018/organisation/organisation_headers.csv.gz" },
            { Key: "pending/1537362006/relationships_wpd.csv.gz" },
            { Key: "pending/1537362006/relationships_crs1.csv.gz" },
            { Key: "pending/1537362006/incremental.txt" },
            { Key: "pending/1537362001/incremental.txt" },
            { Key: "pending/1537362002/incremental.txt" }
        ];

        s3.client.listObjects = jest.fn().mockImplementation(() => ({
            promise: () => Promise.resolve(mockS3Response)
        }));

        beforeEach(() => {

            s3.client.listObjects.mockClear();
        });

        it("should ask s3 client to list objects with Prefix", () => {

            return s3.getJobParameters("mockBucket", "pending/").then((jobParameters) => {

                expect(s3.client.listObjects.mock.calls[0][0]).toEqual({ Bucket: "mockBucket", Key: "pending/" });
                expect(jobParameters).toEqual({
                    ingestName: "1537362001",
                    ingestType: "incremental"
                });

            });

        });

        it("should return list of files and sub-directories of path", () => {

            const contents = s3.getIngestNameAndType(mockS3Response)

            expect(contents).toEqual({
                ingestName: "1537362001",
                ingestType: "incremental"
            });

        });

    });

})
