"use strict";

const Promise = require("bluebird");
const R = require("ramda");
const ingestionService = require("../src/ingestionService");
const kubernetesClient = require("../src/kubernetesClient");


kubernetesClient.deleteJobs = jest.fn().mockImplementation(() => Promise.resolve());
kubernetesClient.createJob = jest.fn().mockImplementation(() => Promise.resolve());
kubernetesClient.labelJob = jest.fn().mockImplementation(() => Promise.resolve());
kubernetesClient.getJobStatus = jest.fn().mockResolvedValue({ completed: true });


describe("ingestionService", () => {

    beforeEach(() => {

        kubernetesClient.deleteJobs.mockClear();
        kubernetesClient.createJob.mockClear();
        kubernetesClient.labelJob.mockClear();

    });

    describe("when asked to run jobType ingest", () => {

        it("should delete jobs", () => {

            return ingestionService.runIngest().then(() => {

                expect(kubernetesClient.deleteJobs).toHaveBeenCalledTimes(1);

            });

        });

        it("should create correct jobs", () => {

            return ingestionService.runIngest("mockJobType", "1537191996244").then(() => {

                expect(kubernetesClient.createJob).toHaveBeenCalledTimes(2);
                expect(kubernetesClient.createJob.mock.calls[0][0]).toEqual("neo4j-mockJobType-1537191996244");
                expect(kubernetesClient.createJob.mock.calls[0][1]).toEqual("neo4j-mockJobType");
                expect(kubernetesClient.createJob.mock.calls[1][0]).toEqual("elastic-mockJobType-1537191996244");
                expect(kubernetesClient.createJob.mock.calls[1][1]).toEqual("elastic-mockJobType");

            });

        });

        it("should label jobs", () => {

            return ingestionService.runIngest("mockJobType", "1537191996244").then(() => {

                expect(kubernetesClient.labelJob).toHaveBeenCalledTimes(2);
                expect(kubernetesClient.labelJob.mock.calls[0][0]).toEqual("neo4j-mockJobType-1537191996244");
                expect(kubernetesClient.labelJob.mock.calls[1][0]).toEqual("elastic-mockJobType-1537191996244");

            });

        });

        it("should wait for jobs to complete", () => {

            kubernetesClient.getJobStatus = jest.fn().mockImplementation((jobName) => {

                if (R.startsWith("neo4j", jobName)) {

                    return neo4jJobStatusMock();
                }

                return elasticJobStatusMock();
            });

            const neo4jJobStatusMock = jest.fn().mockResolvedValue({ completed: true })
                .mockResolvedValueOnce({ completed: false })
                .mockResolvedValueOnce({ completed: false })
                .mockResolvedValueOnce({ completed: false });

            const elasticJobStatusMock = jest.fn().mockResolvedValue({ completed: true })
                .mockResolvedValueOnce({ completed: false })
                .mockResolvedValueOnce({ completed: false });

            return ingestionService.runIngest("mockJobType", "1537191996244").then(() => {

                expect(kubernetesClient.getJobStatus).toHaveBeenCalledTimes(7);
                expect(neo4jJobStatusMock).toHaveBeenCalledTimes(4);
                expect(elasticJobStatusMock).toHaveBeenCalledTimes(3);
                expect(kubernetesClient.getJobStatus).toHaveBeenCalledWith("neo4j-mockJobType-1537191996244");
                expect(kubernetesClient.getJobStatus).toHaveBeenCalledWith("elastic-mockJobType-1537191996244");

            });

        });

    });

});