"use strict"

const {ROLE} = process.env

const childProcess = require("child_process")
const Promise = require("bluebird")

const TOKEN = process.env.KUBE_SERVICE_ACCOUNT_TOKEN;

function startKubeJob(
  cronjobName,
  ingestTimestamp
) {

  const kubectlDeleteCommand = `/app/kubectl --token ${TOKEN} delete job -l role=${ROLE}`;
  const kubectlCreateCommand = `/app/kubectl --token ${TOKEN} create job ${cronjobName}-${ingestTimestamp} --from=cronjob/${cronjobName}`;
  const kubectlLabelCommand = `/app/kubectl --token ${TOKEN} label job ${cronjobName}-${ingestTimestamp} role=${ROLE}`;

  return execPromise(kubectlDeleteCommand)
    .then(() => execPromise(kubectlCreateCommand))
    .then(({stdout, stderr}) => {
      if (stderr) {
        console.error(stderr)
        throw stderr
      }
    })
    .then(() => execPromise(kubectlLabelCommand))
}

function getPods() {
  const kubectlGetPodsCommand = `/app/kubectl get pods`

  return execPromise(kubectlGetPodsCommand)
}

function execPromise(commandString) {
  return new Promise((resolve, reject) => {
    childProcess.exec(commandString, (error, stdout, stderr) => {
      if (error) {
        return reject(new Error(error))
      }

      return resolve({stdout, stderr})
    })
  })
}

module.exports = {startKubeJob, getPods}
