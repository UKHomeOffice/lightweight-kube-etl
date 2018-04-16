describe("poormans kube etl", () => {
  describe("start_kube_job", () => {
    it("should delete jobs with the same role")
    it("should start a new job")
    it("should label the started job with a role")
  })
  describe("check_manifest", () => {
    it("should get the manifest.json")
    it("should iterate through each line of the manifest comparing the hash")
    it("should return false if there is an unmatched hash")
    it("should return true if there are no unmatched hashes")
  })
  describe("get_object_hash", () => {
    it("should return the Etag of an object")
  })
  describe("sqs_message_handler", () => {
    it("should start the kube job if the manifest is good")
    it("should log no matched files if the manifest is no good")
  })
})