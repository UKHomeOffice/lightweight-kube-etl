"use strict";


function runIngest(type, ingestName) {


}

module.exports = { runIngest };

// runIngestion(type, ingestdir/ts)

// waitForJob -> see sandbox

// runIngestionForDb ??? neo / elastic


// const runIngestionJobs = (jobType, ingestPath) => {
//
//     const ingestTimestamp = R.last(ingestPath.split("/"));
//
//     return Promise.all([
//         kube.runKubeJob("neo4j-" + jobType, ingestTimestamp),
//         kube.runKubeJob("elastic-" + jobType, ingestTimestamp)
//     ]);
// };

// function runKubeJob(cronjobName, ingestTimestamp) {
//
//     const jobName = `${cronjobName}-${ingestTimestamp}`;
//     const kubectlDeleteCommand = `/app/kubectl --token ${TOKEN} delete job -l role=${ROLE}`;
//     const kubectlCreateCommand = `/app/kubectl --token ${TOKEN} create job ${jobName} --from=cronjob/${cronjobName}`;
//     const kubectlLabelCommand = `/app/kubectl --token ${TOKEN} label job ${jobName} role=${ROLE}`;
//
//     return execPromise(kubectlDeleteCommand)
//         .then(() => execPromise(kubectlCreateCommand))
//         .then(({stdout, stderr}) => {
//             if (stderr) {
//                 console.error(stderr)
//                 throw stderr
//             }
//         })
//         .then(() => execPromise(kubectlLabelCommand))
//
// }