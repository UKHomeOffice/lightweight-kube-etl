"use strict"

const {BUCKET} = process.env

const s3 = require("./s3")
const kube = require("./kube")
const R = require("ramda")
const mongodb = require("./mongodb")

const sqsMessageHandler = async (message, done) => {

  if (!isManifest(message)) return done()

  const ingestPath = getIngestPath(message)
  const jobType = await s3.getJobType(BUCKET, ingestPath)

  if (jobType === undefined) return done()

  console.info(`jobType: ${jobType}`)

  await startIngestionJobs(jobType, ingestPath).catch(console.error)

  console.info(`insert into Mongo date: ${jobType}`)

  await mongodb.insert({
    ingest: getIngestName(message),
    loadDate: Date.now()
  })

  return done()
}

const startIngestionJobs = (jobType, ingestPath) => {

    const ingestTimestamp = R.last(ingestPath.split("/"));

    return Promise.all([
        kube.startKubeJob("neo4j-" + jobType, ingestTimestamp),
        kube.startKubeJob("elastic-" + jobType, ingestTimestamp)
    ]);
};

const getUploadPath = message =>
  JSON.parse(message.Body).Records[0].s3.object.key

const getIngestPath = message =>
    R.head(getUploadPath(message).split("/manifest.json"))

const isManifest = message => getUploadPath(message).indexOf("manifest") > -1

const getIngestName = message => getUploadPath(message).split("/")[1]

module.exports = {sqsMessageHandler, isManifest, getIngestPath}