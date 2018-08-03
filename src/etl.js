"use strict"

const {BUCKET, ROLE} = process.env

const s3 = require("./s3")
const kube = require("./kube")
const R = require("ramda")
const lastIngestRepository = require("./lastIngestRepository")

const sqsMessageHandler = async (message, done) => {
  if (!isManifest(message)) {
    return done()
  }

  const incrementalFilePath = getManifestPath(message) + "/incremental.txt"
  const jobType = await s3.getJobType(BUCKET, incrementalFilePath)
  console.info(`jobType: ${jobType}`);
  await startIngestionJobs(jobType);
  await lastIngestRepository.insert({
    ingest: getIngestName(message),
    loadDate: Date.now()
  })

  // return done()
}

function startIngestionJobs(jobType) {
  console.log(jobType)
  return Promise.all([
    kube.startKubeJob(ROLE, "neo4j-" + jobType),
    kube.startKubeJob(ROLE, "elastic-" + jobType)
  ])
}

const getUploadPath = message =>
  JSON.parse(message.Body).Records[0].s3.object.key

const getIngestPath = message =>
  R.head(getUploadPath(message).split("/manifest.json"))

const isManifest = message => getUploadPath(message).indexOf("manifest") > -1

const getIngestName = message => getUploadPath(message).split("/")[1]

module.exports = {sqsMessageHandler, isManifest}
