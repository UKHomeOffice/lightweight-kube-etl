"use strict"

const kube = require("../src/kubernetesClient")
const childProcess = require("child_process")
const R = require("ramda");


childProcess.exec = jest
  .fn()
  .mockImplementation((command, callback) =>{

    if (R.contains("create", command)) {


    }

    return callback(null, {stdout: "exec success", stderr: null})
  });

global.console.error = jest.fn();

describe("kube", () => {
  describe("runKubeJob", () => {
    beforeEach(() => childProcess.exec.mockClear())

    it("should delete jobs with the same role", () => {

      return kube
        .runKubeJob("MockJobName", "mockTimestamp")
        .then(() => {
          expect(childProcess.exec.mock.calls[0][0])
            .toEqual("/app/kubectl --token MOCK_TOKEN delete job -l role=mockRole")
        })
    })

    it("should create a new job", () => {
      return kube
        .runKubeJob("MockJobName", "mockTimestamp")
        .then(() => {
          expect(childProcess.exec.mock.calls[1][0])
            .toEqual("/app/kubectl --token MOCK_TOKEN create job MockJobName-mockTimestamp --from=cronjob/MockJobName")
        })
    })

    it("should label the started job with a role", () => {
      return kube
        .runKubeJob("MockJobName", "mockTimestamp")
        .then(() => {
          expect(childProcess.exec.mock.calls[2][0])
            .toEqual("/app/kubectl --token MOCK_TOKEN label job MockJobName-mockTimestamp role=mockRole")
        })
    })

    // describe.only("after creating a job", () => {
    //
    //   it("should wait for job to complete", () => {
    //
    //     expect(kube.waitForJob).toEqual(false);
    //
    //   });
    //
    // });

      describe.only("when asked to create a job given name and cronjob", () => {

        it("should exec correct kubectl command", () => {

            return kube.createJob("mockJob", "mockCronjob").then(() => {

                expect(childProcess.exec.mock.calls[0][0])
                    .toEqual("/app/kubectl --token MOCK_TOKEN create job mockJob --from=cronjob/mockCronjob");

            });
        });

      });

  })
})