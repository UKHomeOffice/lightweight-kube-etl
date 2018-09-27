"use strict"

const childProcess = require("child_process")
const Promise = require("bluebird")
const R = require("ramda")

const {ROLE, KUBE_SERVICE_ACCOUNT_TOKEN} = process.env;


function getPods() {
  const kubectlGetPodsCommand = `kubectl get pods`

  return execPromise(kubectlGetPodsCommand)
}

function createJob(jobName, cronjobName) {

    const kubectlCreateCommand = "kubectl --token " + KUBE_SERVICE_ACCOUNT_TOKEN +
        " create job " + jobName + " --from=cronjob/" + cronjobName;

    return execPromise(kubectlCreateCommand);
}

function deleteJobs() {

    const kubectlDeleteCommand = `kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} delete job -l role=${ROLE}`;

    return execPromise(kubectlDeleteCommand);
}

function labelJob(jobName) {

    const kubectlLabelCommand = `kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} label job ${jobName} role=${ROLE}`;

    return execPromise(kubectlLabelCommand);
}

// TODO: need to check kubectl response for job failure & reject Error
function getJobStatus(jobName) {

    const kubectlPodStatusCommand = `kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} get job ${jobName} -o json`;

    return execPromise(kubectlPodStatusCommand).then(JSON.parse)
        .then(({ status }) => ({ completed: R.has("succeeded", status) }));

}


function execPromise(commandString) {

    return new Promise((resolve, reject) => {

        childProcess.exec(commandString, (error, stdout, stderr) => {

            if (error || stderr) {

                return reject(new Error(error || stderr));
            }

            return resolve(stdout);
        });

  });

}

module.exports = {
    createJob,
    deleteJobs,
    labelJob,
    getJobStatus
};