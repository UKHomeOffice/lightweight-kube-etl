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

const filterJobs = R.compose(
  R.gt(R.__, 0),
  R.length,
  R.intersection(['neo4j', 'elastic']),
  R.split('-'),
  R.path(['metadata', 'name'])
);

function deleteJob (jobLabel) {
  const deleteJobCmd = `kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} delete job/${job} role=${ROLE}`;
  return execPromise(deleteJobCmd)
}

const getJobLabels = forIngestType => R.compose(
  R.filter(R.test(forIngestType)),
  R.map(R.path(['metadata', 'name'])),
  R.filter(filterJobs),
  R.prop('items')
)

function deleteJobs(ingestType) {
  let jobsToDelete;
  const forIngestType = ingestType === 'incremental' ? new RegExp(/-delta-/) : new RegExp(/-bulk-/);
  const listJobsCmd = `kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} get jobs -o json`;
  
  return execPromise(listJobsCmd)
    .then(jobsList => {
      jobsToDelete = getJobLabels(forIngestType)(jobsList);
      
      console.log({jobsToDelete});

      return Promise.all([jobsToDelete.map(deleteJob)]);
    })
    .then(() => (jobsToDelete));
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