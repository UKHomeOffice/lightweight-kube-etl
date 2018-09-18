"use strict";

const Promise = require("bluebird");
const R = require("ramda");
const kubernetesClient = require("./kubernetesClient");

// TODO: jobTypes should configurable
const jobTypes = ["neo4j", "elastic"];

function runIngest(ingestType, ingestName) {

    return kubernetesClient.deleteJobs()
        .then(() => runJobs(ingestType, ingestName));

}

function runJobs(ingestType, ingestName) {

    return Promise.all(R.map((jobType) => {

        const cronjobName = jobType + "-" + ingestType,
            jobName = cronjobName + "-" + ingestName;

        return kubernetesClient.createJob(jobName, cronjobName)
            .then(() => kubernetesClient.labelJob(jobName))
            .then(() => waitForJob(jobName, 100));

    }, jobTypes));

}

// TODO: pollInterval should configurable
function waitForJob(jobName, pollInterval) {

    return kubernetesClient.getJobStatus(jobName)
        .then((status) => {

            if (status.completed) {

                return status;
            }

            return Promise.delay(pollInterval)
                .then(() => waitForJob(jobName, pollInterval));

        });
}

module.exports = { runIngest };