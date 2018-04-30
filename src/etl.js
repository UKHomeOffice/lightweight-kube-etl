"use strict"

const {BUCKET, ROLE, CRONJOB} = process.env

const {check_manifest} = require("./s3")
const {start_kube_job} = require("./kube")

const sqs_message_handler = async (message, done) => {
  if (await check_manifest(BUCKET)) await start_kube_job(ROLE, CRONJOB)
  else console.info("Files don't yet match the manifest")
  done()
}

module.exports = {
  sqs_message_handler
}
