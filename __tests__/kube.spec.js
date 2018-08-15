"use strict"

const kube = require("../src/kube")
const childProcess = require("child_process")


childProcess.exec = jest
  .fn()
  .mockImplementation((command, callback) =>
    callback(null, {stdout: "exec success", stderr: null})
  )

describe("kube", () => {
  describe("startKubeJob", () => {
    beforeEach(() => childProcess.exec.mockClear())

    it("should delete jobs with the same role", () => {

      return kube
        .startKubeJob("mockRole", "MockJobName", "mockJobId")
        .then(() => {
          expect(childProcess.exec.mock.calls[0][0]).toEqual(
            "/app/kubectl --token MOCK_TOKEN delete job -l role=mockRole"
          )
        })
    })

    it("should create a new job", () => {
      return kube
        .startKubeJob("mockRole", "MockJobName", "mockJobId")
        .then(() => {
          expect(childProcess.exec.mock.calls[1][0]).toEqual(
            "/app/kubectl --token MOCK_TOKEN create job MockJobName-mockJobId --from=cronjob/MockJobName"
          )
        })
    })

    it("should label the started job with a role", () => {
      return kube
        .startKubeJob("mockRole", "MockJobName", "mockJobId")
        .then(() => {
          expect(childProcess.exec.mock.calls[2][0]).toEqual(
            "/app/kubectl --token MOCK_TOKEN label job MockJobName-mockJobId role=mockRole"
          )
        })
    })
  })
})
