const s3 = require("../modules/s3")
const kube = require("../modules/kube")

describe("etl", () => {
  describe("sqs_message_handler", () => {
    jest.spyOn(kube, "start_kube_job").mockReturnValue(true)
    const done = () => true

    beforeEach(() => {
      kube.start_kube_job.mockReset()
    })

    it("should start the kube job if the manifest is good", async () => {
      jest.mock("../modules/s3", () => ({
        check_manifest: () => Promise.resolve(true)
      }))
      const etl = require("../etl.js")

      await etl.sqs_message_handler("test", done)
      expect(kube.start_kube_job).toHaveBeenCalledTimes(1)
    })

    it("should not start the kube job if the manifest is incorrect", async () => {
      jest.mock("../modules/s3", () => ({
        check_manifest: () => Promise.resolve(false)
      }))
      const etl = require("../etl.js")

      await etl.sqs_message_handler("test", done)
      expect(kube.start_kube_job).toHaveBeenCalledTimes(1)
    })
  })
})
