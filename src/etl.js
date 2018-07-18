"use strict"

const {BUCKET, ROLE} = process.env

const {check_manifest, get_job_type} = require("./s3")
const {start_kube_job} = require("./kube")

const sqs_message_handler = async (message, done) => {
  if (is_manifest(message)) {
    if (await check_manifest(BUCKET)) {
      const incrementalFilePath = get_manifest_path(message) + "/incremental"
      const jobType = await get_job_type(incrementalFilePath)

      if (jobType === "bulk") {
        await start_kube_job(ROLE, "neo4j-bulk")
        await start_kube_job(ROLE, "elastic-bulk")

      } else if (jobType === "delta") {
        await start_kube_job(ROLE, "neo4j-delta")
        await start_kube_job(ROLE, "elastic-delta")
      } else {
        console.error("jobType wasnt captured correctly")
      }
    } else {
      console.info("Files don't yet match the manifest")
    }
  }

  done()
}

const is_manifest = a =>
  JSON.parse(a.Body).Records[0].s3.object.key.indexOf("manifest") > -1
    ? true
    : false

const get_manifest_path = a =>
  JSON.parse(a.Body).Records[0].s3.object.key.split("/manifest.json")[0]

module.exports = {
  sqs_message_handler,
  is_manifest,
  get_manifest_path
}
