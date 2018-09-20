"use strict"

const etl = require("../src/etl")
const s3 = require("../src/s3")
const mongodb = require("../src/mongodb")
const kube = require("../src/kubernetesClient")
const ingestionService = require("../src/ingestionService");

jest.mock("../src/kubernetesClient")

s3.checkManifest = jest.fn().mockImplementation(() => Promise.resolve(true))
s3.getIngestType = jest.fn().mockImplementation(() => Promise.resolve("delta"))
mongodb.insert = jest.fn().mockImplementation(() => Promise.resolve({ result: "mongoCommandResponse" }))
ingestionService.runIngest = jest.fn().mockImplementation(() => Promise.resolve())

global.console = {
    info: jest.fn(),
    error: jest.fn()
};

const mockMessages = [
  {
    MessageId: "2da4371e-b831-4b30-b8f6-eb815e25c2de",
    ReceiptHandle: "xxxxx",
    MD5OfBody: "xxxxx",
    Body: `{
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
]

describe("etl", () => {
  describe("isManifest", () => {
    it("should return true if the message is a manifest", () => {

      expect(etl.isManifest(mockMessages[0])).toBeTruthy()
    })

    it("should return false if the message is not a manifest", () => {
      expect(etl.isManifest(mockMessages[1])).toBeFalsy()
    })
  })

  describe("getIngestPath", () => {
    it("should return true if the message is a manifest", () => {
      expect(etl.getIngestPath(mockMessages[0])).toEqual("pending/222222222333")
    })
  })

  describe("messageHandler", () => {
    // jest.spyOn(kube, "runKubeJob").mockReturnValue(true)
    const doneMock = jest.fn()

    beforeEach(() => {
      ingestionService.runIngest.mockClear()
      doneMock.mockReset()
    })

    it("should call done function if the message does not contain a manifest key", function() {
      const mockNonManifestMessage = mockMessages[1]
      return etl
        .messageHandler(mockNonManifestMessage, doneMock)
        .then(() => {
          expect(doneMock).toHaveBeenCalledTimes(1)
          expect(ingestionService.runIngest).toHaveBeenCalledTimes(0)
        })
    })

    // TODO
    // it("should not start the kube job if the manifest is incorrect", async () => {
    //   s3.checkManifest = jest.fn(bucket => Promise.resolve(false))
    //   await etl.messageHandler(mockMessages[0], doneMock)
    //
    //   expect(doneMock).toHaveBeenCalledTimes(1)
    //   expect(kube.startKubeJob).toHaveBeenCalledTimes(0)
    // })

    it("should ask ingestion service to run correct job", () => {

        const mockNonManifestMessage = mockMessages[0];

        return etl.messageHandler(mockNonManifestMessage, doneMock).then(() => {

            expect(ingestionService.runIngest).toHaveBeenCalledTimes(1);
            expect(ingestionService.runIngest.mock.calls[0][0]).toEqual("delta");
            expect(ingestionService.runIngest.mock.calls[0][1]).toEqual("222222222333");

        });

    });

    it("should call done with no arguments when ingest succeeds", () => {

        const mockNonManifestMessage = mockMessages[0];

        return etl.messageHandler(mockNonManifestMessage, doneMock).then(() => {

            expect(doneMock).toHaveBeenCalledTimes(1);
            expect(doneMock.mock.calls[0][0]).toEqual(undefined);

        });

    });

  })

})
