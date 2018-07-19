const messages = [
  {
    MessageId: "2da4371e-b831-4b30-b8f6-eb815e25c2de",
    ReceiptHandle: "xxxxx",
    MD5OfBody: "xxxxx",
    Body:
      '{"Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"eu-west-2","eventTime":"2018-07-16T14:19:30.694Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:xxxxxxxx"},"requestParameters":{"sourceIPAddress":"127.0.0.1"},"responseElements":{"x-amz-request-id":"xxxxx","x-amz-id-2":"xxxxx"},"s3":{"s3SchemaVersion":"1.0","configurationId":"tf-s3-queue-xxxxxxx","bucket":{"name":"bucket-s3","ownerIdentity":{"principalId":"xxxxxx"},"arn":"arn:aws:s3:::bucket-s3"},"object":{"key":"pending/222222222333/manifest.json","size":6692,"eTag":"fd8fe08686aaeebaf865abb176dba1d2","versionId":"hv5fvThMHjo_OIKJHsodJiX9cz7pidIe","sequencer":"005B4CA9725D1AD066"}}}]}',
    Attributes: {
      SenderId: "xxxxxx",
      ApproximateFirstReceiveTimestamp: "1531753607253",
      ApproximateReceiveCount: "2",
      SentTimestamp: "1531750770738"
    }
  },
  {
    MessageId: "959853a7-1889-4e1a-9aae-6a4ee129429e",
    ReceiptHandle: "xxxxx",
    MD5OfBody: "xxxxx",
    Body:
      '{"Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"eu-west-2","eventTime":"2018-07-16T14:38:24.166Z","eventName":"ObjectCreated:CompleteMultipartUpload","userIdentity":{"principalId":"AWS:xxxxxxxx"},"requestParameters":{"sourceIPAddress":"127.0.0.1"},"responseElements":{"x-amz-request-id":"xxxxx","x-amz-id-2":"xxxxx"},"s3":{"s3SchemaVersion":"1.0","configurationId":"tf-s3-queue-xxxxxxx","bucket":{"name":"bucket-s3","ownerIdentity":{"principalId":"xxxxxx"},"arn":"arn:aws:s3:::bucket-s3"},"object":{"key":"pending/222222222333/contact/contact_7_sample.csv.gz","size":588021664,"eTag":"9dc5ba6ed5027db5bca93a8a741ad93e-113","versionId":"VQ85xVdNXJ18Q2WKVCX5JxkGSm772LQ0","sequencer":"005B4CADACB6792C09"}}}]}',
    Attributes: {
      SenderId: "xxxxxx",
      ApproximateFirstReceiveTimestamp: "1531753607255",
      ApproximateReceiveCount: "2",
      SentTimestamp: "1531751904192"
    }
  }
]

describe("etl", () => {
  describe("is_manifest", () => {
    const {is_manifest} = require("../src/etl")

    it("should return true if the message is a manifest", () => {
      expect(is_manifest(messages[0])).toBeTruthy()
    })

    it("should return false if the message is not a manifest", () => {
      expect(is_manifest(messages[1])).toBeFalsy()
    })
  })

  describe("get_manifest_path", () => {
    const {get_manifest_path} = require("../src/etl")

    it("should return true if the message is a manifest", () => {
      expect(get_manifest_path(messages[0])).toEqual("pending/222222222333")
    })
  })

  describe("sqs_message_handler", () => {
    const done = () => true

    beforeEach(() => {
      jest.resetModules()
    })

    it('should call done function if the message does not contain a manifest key', function () {
      // TODO
    });

    it("should start the kube job if the manifest is good", async () => {
      jest.mock("../src/s3", () => ({
        check_manifest: bucket => Promise.resolve(true),
        get_job_type: (bucket, key) => Promise.resolve("delta")
      }))
      const kube = require("../src/kube")
      jest.spyOn(kube, "start_kube_job").mockReturnValue(true)

      const etl = require("../src/etl.js")

      await etl.sqs_message_handler(messages[0], done)
      expect(kube.start_kube_job).toHaveBeenCalledTimes(2)
    })

    it("should not start the kube job if the manifest is incorrect", async () => {
      jest.mock("../src/s3", () => ({
        check_manifest: bucket => Promise.resolve(false)
      }))
      const kube = require("../src/kube")
      jest.spyOn(kube, "start_kube_job").mockReturnValue(true)

      const etl = require("../src/etl.js")

      await etl.sqs_message_handler(messages[0], done)
      expect(kube.start_kube_job).toHaveBeenCalledTimes(0)
    })
  })
})
