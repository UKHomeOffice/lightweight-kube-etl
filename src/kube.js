"use strict";

const crypto = require("crypto");
const childProcess = require("child_process");
const Promise = require("bluebird");


function startKubeJob(role, cronjob, job_id=crypto.randomBytes(16).toString("hex")) {

    const kubectlDeleteCommand = `../../kubectl delete job -l role=${role}`,
        kubectlCreateCommand = `../../kubectl create job ${cronjob}-${job_id} --from=cronjob/${cronjob}`,
        kubectlLabelCommand = `../../kubectl label job ${cronjob}-${job_id} role=${role}`;

    return execPromise(kubectlDeleteCommand)
        .then(() => execPromise(kubectlCreateCommand))
        .then(({ stdout, stderr }) => {

            if (stderr) {
                console.error(stderr);
                throw stderr
            }
        })
        .then(() => execPromise(kubectlLabelCommand));
}

function execPromise(commandString) {

    return new Promise((resolve, reject) => {

        childProcess.exec(commandString, (error, stdout, stderr) => {

            if (error) {

                return reject(new Error(error));
            }

            return resolve({ stdout, stderr });

        });
    });
}

module.exports = { startKubeJob };
