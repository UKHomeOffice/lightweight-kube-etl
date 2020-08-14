jest.mock("./s3-client");

const { start, waitForManifest } = require("./ingestor");

const s3 = require("./s3-client");

describe("The s3 client", () => {
  it("will keep looking in an AWS bucket if it errors, is empty, has no timestamped folder, has no manifest file", done => {
    start(ingestParams => {
      expect(ingestParams).toEqual({ ingestName: "1538055240", ingestType: "bulk" });
      expect(s3.listObjectsV2.mock.calls.length).toBe(6);
      done();
    });
  });

  it("will wait for a manifest file", done => {
    const ingestParams = { ingestName: "1538055240", ingestType: "bulk" };

    waitForManifest(ingestParams, _ingestParams => {
      expect(ingestParams).toEqual(_ingestParams);
      done();
    });
  });
});
