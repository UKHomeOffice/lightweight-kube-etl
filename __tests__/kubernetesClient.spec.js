"use strict";

const kube = require("../src/kubernetesClient");
const childProcess = require("child_process");
const R = require("ramda");


global.console.error = jest.fn();

describe("kube", () => {

  beforeEach(() => {

      childProcess.exec = jest.fn().mockImplementation((command, callback) => {

          return callback(null, "exec success", null);

      });

  });

  afterEach(() => {

      childProcess.exec.mockReset();
  });


  describe("when asked to create a job given name and cronjob", () => {

    it("should exec correct kubectl command", () => {

        return kube.createJob("mockJob", "mockCronjob").then(() => {

            expect(childProcess.exec.mock.calls[0][0])
                .toEqual("/app/kubectl --token MOCK_TOKEN create job mockJob --from=cronjob/mockCronjob");

        });

    });


    it("should throw an exception if command execution fails", (done) => {

      childProcess.exec = jest.fn()
          .mockImplementation((command, callback) => callback("mock child_process.exec error", null, null));

      return kube.createJob("mockJob", "mockCronjob").catch((error) => {

          expect(error).toBeInstanceOf(Error);
          expect(error.message).toEqual("mock child_process.exec error");
          done();

      });

    });

    it("should throw an exception if command execution returns stderr", (done) => {

      childProcess.exec = jest.fn()
          .mockImplementation((command, callback) => callback(null, null, "stderr"));

      return kube.createJob("mockJob", "mockCronjob").catch((error) => {

          expect(error).toBeInstanceOf(Error);
          expect(error.message).toEqual("stderr");
          done();

      });

    });

  });

  describe("when asked to delete a job", () => {

      it("should exec correct kubectl command with correct label", () => {

          return kube.deleteJobs().then(() => {

              expect(childProcess.exec.mock.calls[0][0])
                  .toEqual("/app/kubectl --token MOCK_TOKEN delete job -l role=mockRole");

          });

      });

      it("should throw an exception if command execution fails", (done) => {

          childProcess.exec = jest.fn()
              .mockImplementation((command, callback) => callback("mock child_process.exec error", null, null));

          return kube.deleteJobs().catch((error) => {

              expect(error).toBeInstanceOf(Error);
              expect(error.message).toEqual("mock child_process.exec error");
              done();

          });

      });

      it("should throw an exception if command execution returns stderr", (done) => {

          childProcess.exec = jest.fn()
              .mockImplementation((command, callback) => callback(null, null, "stderr"));

          return kube.deleteJobs().catch((error) => {

              expect(error).toBeInstanceOf(Error);
              expect(error.message).toEqual("stderr");
              done();

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

      it("should throw an exception if command execution fails", (done) => {

          childProcess.exec = jest.fn()
              .mockImplementation((command, callback) => callback("mock child_process.exec error", null, null));

          return kube.labelJob("mockJob").catch((error) => {

              expect(error).toBeInstanceOf(Error);
              expect(error.message).toEqual("mock child_process.exec error");
              done();

          });

      });

      it("should throw an exception if command execution returns stderr", (done) => {

          childProcess.exec = jest.fn()
              .mockImplementation((command, callback) => callback(null, null, "stderr"));

          return kube.labelJob("mockJob").catch((error) => {

              expect(error).toBeInstanceOf(Error);
              expect(error.message).toEqual("stderr");
              done();

          });

      });

  });

  describe("when asked to get job status", () => {

      it("should exec correct kubectl command", () => {

          return kube.getJobStatus("mockJob").then(() => {

              expect(childProcess.exec).toHaveBeenCalledTimes(1);
              expect(childProcess.exec.mock.calls[0][0])
                  .toEqual("/app/kubectl --token MOCK_TOKEN get po mockJob -o jsonpath --template={.status.containerStatuses[*].state.terminated.reason}");

          });
      });

      it("should throw an exception if command execution fails", (done) => {

          childProcess.exec = jest.fn()
              .mockImplementation((command, callback) => callback("mock child_process.exec error", null, null));

          return kube.getJobStatus("mockJob").catch((error) => {

              expect(error).toBeInstanceOf(Error);
              expect(error.message).toEqual("mock child_process.exec error");
              done();

          });

      });

      it("should throw an exception if command execution returns stderr", (done) => {

          childProcess.exec = jest.fn()
              .mockImplementation((command, callback) => callback(null, null, "mock stderr"));

          return kube.getJobStatus("mockJob").catch((error) => {

              expect(error).toBeInstanceOf(Error);
              expect(error.message).toEqual("mock stderr");
              done();

          });

      });

  });

});