const AWS = require("aws-sdk")

describe("s3", async () => {
  beforeAll(() => {
    AWS.S3 = jest.fn().mockImplementation(() => ({
      headObject: (bucket, key) => ({
        promise: () =>
          Promise.resolve({
            ETag: "ba6119931c7010138eec96d9fb75701865908286"
          })
      }),
      getObject: (bucket, key) => ({
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
      })
    }))
  })

  afterAll(() => {
    AWS.S3.mockRestore()
  })

  describe("get_object_hash", () => {
    it("should return the Etag of an object", async () => {
      const s3 = require("../modules/s3")
      res = await s3.get_object_hash("foo", "bar")

      expect(res).toEqual("ba6119931c7010138eec96d9fb75701865908286")
    })
  })

  describe("get_manifest", () => {
    it("should get the manifest.json", async () => {
      const s3 = require("../modules/s3")
      res = await s3.get_manifest("foo", "bar")

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

  describe("check_manifest", () => {
    it("should return true if there are no unmatched hashes", async () => {
      const s3 = require("../modules/s3")
      res = await s3.check_manifest("foo")

      expect(res).toEqual(true)
    })
  })
})
