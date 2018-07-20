"use strict";

const etl = require("../src/etl");
const s3 = require("../src/s3");
const kube = require("../src/kube");


jest.mock("../src/s3", () => ({
    check_manifest: bucket => Promise.resolve(true),
    get_job_type: (bucket, key) => Promise.resolve("delta")
}));
jest.mock("../src/kube");

const mockMessages = [
  {
    MessageId: "2da4371e-b831-4b30-b8f6-eb815e25c2de",
    ReceiptHandle: "xxxxx",
    MD5OfBody: "xxxxx",
    Body:`{
      "Records":[
        {
          "eventVersion":"2.0",
          "eventSource":"aws:s3",
          "awsRegion":"eu-west-2",
          "eventTime":"2018-07-16T14:19:30.694Z",
          "eventName":"ObjectCreated:Put",
          "userIdentity":{"principalId":"AWS:xxxxxxxx"},
          "requestParameters":{"sourceIPAddress":"127.0.0.1"},
          "responseElements":{"x-amz-request-id":"xxxxx","x-amz-id-2":"xxxxx"},
          "s3":{
            "s3SchemaVersion":"1.0",
            "configurationId":"tf-s3-queue-xxxxxxx",
            "bucket":{
              "name":"bucket-s3",
              "ownerIdentity":{"principalId":"xxxxxx"},
              "arn":"arn:aws:s3:::bucket-s3"
            },
            "object":{
              "key":"pending/222222222333/manifest.json",
              "size":6692,
              "eTag":"fd8fe08686aaeebaf865abb176dba1d2",
              "versionId":"hv5fvThMHjo_OIKJHsodJiX9cz7pidIe",
              "sequencer":"005B4CA9725D1AD066"
            }
          }
        }
      ]
    }`,
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
    Body: `{
      "Records":[
        {
          "eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"eu-west-2","eventTime":"2018-07-16T14:38:24.166Z","eventName":"ObjectCreated:CompleteMultipartUpload","userIdentity":{"principalId":"AWS:xxxxxxxx"},"requestParameters":{"sourceIPAddress":"127.0.0.1"},"responseElements":{"x-amz-request-id":"xxxxx","x-amz-id-2":"xxxxx"},
          "s3":{
            "s3SchemaVersion":"1.0","configurationId":"tf-s3-queue-xxxxxxx","bucket":{"name":"bucket-s3","ownerIdentity":{"principalId":"xxxxxx"},"arn":"arn:aws:s3:::bucket-s3"},
            "object":{
              "key":"pending/222222222333/contact/contact_7_sample.csv.gz","size":588021664,"eTag":"9dc5ba6ed5027db5bca93a8a741ad93e-113","versionId":"VQ85xVdNXJ18Q2WKVCX5JxkGSm772LQ0","sequencer":"005B4CADACB6792C09"
            }
          }
        }
      ]
    }`,
    Attributes: {
      SenderId: "xxxxxx",
      ApproximateFirstReceiveTimestamp: "1531753607255",
      ApproximateReceiveCount: "2",
      SentTimestamp: "1531751904192"
    }
  }
];



describe("etl", () => {

  describe("isManifest", () => {

    it("should return true if the message is a manifest", () => {
      expect(etl.isManifest(mockMessages[0])).toBeTruthy()
    })

    it("should return false if the message is not a manifest", () => {
      expect(etl.isManifest(mockMessages[1])).toBeFalsy()
    })
  })

  describe("getManifestPath", () => {

    it("should return true if the message is a manifest", () => {
      expect(etl.getManifestPath(mockMessages[0])).toEqual("pending/222222222333")
    })
  })

  describe("sqsMessageHandler", () => {

    jest.spyOn(kube, "startKubeJob").mockReturnValue(true);

    const doneMock = jest.fn();

    beforeEach(() => {

      kube.startKubeJob.mockReset();
      doneMock.mockReset();

    });

    it('should call done function if the message does not contain a manifest key', function () {

        const mockNonManifestMessage = mockMessages[1];

        return etl.sqsMessageHandler(mockNonManifestMessage, doneMock).then(() => {

          expect(doneMock).toHaveBeenCalledTimes(1);
          expect(kube.startKubeJob).toHaveBeenCalledTimes(0);

        });

    });

    it("should start the kube job if the manifest is good", async () => {

      await etl.sqsMessageHandler(mockMessages[0], doneMock);

      expect(doneMock).toHaveBeenCalledTimes(1);
      expect(kube.startKubeJob).toHaveBeenCalledTimes(2);

    });

    it("should not start the kube job if the manifest is incorrect", async () => {

      s3.check_manifest = jest.fn(bucket => Promise.resolve(false));

      await etl.sqsMessageHandler(mockMessages[0], doneMock);

      expect(doneMock).toHaveBeenCalledTimes(1);
      expect(kube.startKubeJob).toHaveBeenCalledTimes(0);

    });

  });

});
