"use strict"

const s3 = require("./s3")
const kube = require("./kubernetesClient")
const R = require("ramda")
const mongodb = require("./mongodb")
const ingestionService = require("./ingestionService")

const {BUCKET} = process.env


const sqsMessageHandler = async (message, done) => {

  if (!isManifest(message)) return done()

  const ingestPath = getIngestPath(message)
  const jobType = await s3.getJobType(BUCKET, ingestPath)

  if (jobType === undefined) return done()

  console.info(`jobType: ${jobType}`)

  const ingestName = getIngestName(message)

  return ingestionService.runIngest(jobType, ingestName).then(() => {

    console.info(`insert into Mongo date: ${jobType}`);

    return  mongodb.insert({
      ingest: ingestName,
      loadDate: Date.now()
    });

  }).then(done)
  .catch(console.error);

};


const getUploadPath = message =>
  JSON.parse(message.Body).Records[0].s3.object.key

const getIngestPath = message =>
    R.head(getUploadPath(message).split("/manifest.json"))

const isManifest = message => getUploadPath(message).indexOf("manifest") > -1

const getIngestName = message => getUploadPath(message).split("/")[1]


module.exports = {sqsMessageHandler, isManifest, getIngestPath}