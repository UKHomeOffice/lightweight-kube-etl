"use strict"

const childProcess = require("child_process")
const Promise = require("bluebird")

const {ROLE, KUBE_SERVICE_ACCOUNT_TOKEN} = process.env;


function getPods() {
  const kubectlGetPodsCommand = `/app/kubectl get pods`

  return execPromise(kubectlGetPodsCommand)
}

function createJob(jobName, cronjobName) {

    const kubectlCreateCommand = "/app/kubectl --token " + KUBE_SERVICE_ACCOUNT_TOKEN +
        " create job " + jobName + " --from=cronjob/" + cronjobName;

    return execPromise(kubectlCreateCommand);
}

function deleteJobs() {

    const kubectlDeleteCommand = `/app/kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} delete job -l role=${ROLE}`;

    return execPromise(kubectlDeleteCommand);
}

function labelJob(jobName) {

    const kubectlLabelCommand = `/app/kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} label job ${jobName} role=${ROLE}`;

    return execPromise(kubectlLabelCommand);
}

function getJobStatus(jobName) {

    const kubectlPodStatusCommand = "/app/kubectl --token MOCK_TOKEN get po " + jobName +
        " -o jsonpath --template={.status.containerStatuses[*].state.terminated.reason}";

    return execPromise(kubectlPodStatusCommand);

}

function execPromise(commandString) {

    return new Promise((resolve, reject) => {

        childProcess.exec(commandString, (error, stdout, stderr) => {

            if (error || stderr) {

                return reject(new Error(error || stderr));
            }

            return resolve({stdout, stderr});
        });

  });

}

module.exports = {
    createJob,
    deleteJobs,
    labelJob,
    getJobStatus
};