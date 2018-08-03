"use strict";

const {BUCKET, ROLE} = process.env;

const s3 = require("./s3");
const kube = require("./kube");
const R = require("ramda");
const lastIngestRepository = require("./lastIngestRepository");

const sqsMessageHandler = async (message, done) => {

    if (!isManifest(message)) {

        return done();
    }

    return s3.checkManifest(BUCKET, getUploadPath(message)).then((checksumsOk) => {

        if (!checksumsOk) {

            return;
        }

        return s3.getJobType(BUCKET, getIngestPath(message))
            // .then(startIngestionJobs)
            .then((jobType) => {console.log(jobType); return jobType;})
            .then(() => lastIngestRepository.insert({ "ingest": getIngestName(message), "loadDate": Date.now() }))
            .then(done);
            // .catch(console.log);

    });

};

function startIngestionJobs(jobType) {

    console.log(jobType)
    return Promise.all([
        kube.startKubeJob(ROLE, "neo4j-" + jobType),
        kube.startKubeJob(ROLE, "elastic-" + jobType)
    ]);
}

const getUploadPath = (message) => (JSON.parse(message.Body).Records[0].s3.object.key);

const getIngestPath = message => (R.head(getUploadPath(message).split("/manifest.json")));

const isManifest = message => (getUploadPath(message).indexOf("manifest") > -1);

const getIngestName = message => getUploadPath(message).split("/")[1];


module.exports = { sqsMessageHandler, isManifest };
