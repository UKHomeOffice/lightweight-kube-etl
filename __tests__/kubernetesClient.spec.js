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

  beforeEach(() => childProcess.exec.mockClear())


  describe("when asked to create a job given name and cronjob", () => {

    it("should exec correct kubectl command", () => {

        return kube.createJob("mockJob", "mockCronjob").then(() => {

            expect(childProcess.exec.mock.calls[0][0])
                .toEqual("/app/kubectl --token MOCK_TOKEN create job mockJob --from=cronjob/mockCronjob");

        });
    });

  });

  describe("when asked to delete a job", () => {

      it("should exec correct kubectl command with correct label", () => {

          return kube.deleteJob().then(() => {

              expect(childProcess.exec.mock.calls[0][0])
                  .toEqual("/app/kubectl --token MOCK_TOKEN delete job -l role=mockRole");

          });
      });

  });

  describe("when asked to label a job", () => {

      it("should exec correct kubectl command with correct label", () => {

          return kube.labelJob("mockJob").then(() => {

              expect(childProcess.exec.mock.calls[0][0])
                  .toEqual("/app/kubectl --token MOCK_TOKEN label job mockJob role=mockRole");

          });
      });

  });

  describe("when asked to get job status", () => {

      it("should exec correct kubectl command", () => {

          return kube.getJobStatus("mockJob").then(() => {

              expect(childProcess.exec.mock.calls[0][0])
                  .toEqual("/app/kubectl --token MOCK_TOKEN get po mockJob -o jsonpath --template={.status.containerStatuses[*].state.terminated.reason}");

          });
      });

  });

})