"use strict"

const s3 = require("./s3")
const kube = require("./kubernetesClient")
const R = require("ramda")
const mongodb = require("./mongodb")
const ingestionService = require("./ingestionService")

const {BUCKET} = process.env


const messageHandler = async (message, done) => {

  if (!isManifest(message)) return done()

  const ingestPath = getIngestPath(message)
  const ingestType = await s3.getIngestType(BUCKET, ingestPath)

  if (ingestType === undefined) return done()

  console.info(`ingestType: ${ingestType}`)

  const ingestName = getIngestName(message)

  return ingestionService.runIngest(ingestType, ingestName).then(() => {

    console.info(`insert into Mongo date: ${ingestType}`);

    // TODO: add job details e.g. type, start time, end time, duration?
    return  mongodb.insert({
      ingest: ingestName,
      loadDate: Date.now()
    });

  })
  .then(() => done())
  .catch(console.error);

};


const getUploadPath = message =>
  JSON.parse(message.Body).Records[0].s3.object.key

const getIngestPath = message =>
    R.head(getUploadPath(message).split("/manifest.json"))

const isManifest = message => getUploadPath(message).indexOf("manifest") > -1

const getIngestName = message => getUploadPath(message).split("/")[1]


module.exports = {messageHandler, isManifest, getIngestPath}