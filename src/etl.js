"use strict"

const {BUCKET, ROLE} = process.env;

const s3 = require("./s3");
const kube = require("./kube");
const R = require("ramda");

const sqsMessageHandler = async (message, done) => {

  if (isManifest(message)) {

    const isManifestMessage = await s3.check_manifest(BUCKET);

    if (isManifestMessage) {

      const incrementalFilePath = getManifestPath(message) + "/incremental";
      const jobType = await s3.get_job_type(incrementalFilePath);

      if (jobType === "bulk") {

        await kube.startKubeJob(ROLE, "neo4j-bulk");
        await kube.startKubeJob(ROLE, "elastic-bulk");

      } else if (jobType === "delta") {

        await kube.startKubeJob(ROLE, "neo4j-delta");
        await kube.startKubeJob(ROLE, "elastic-delta");

      } else {

        console.error("jobType wasnt captured correctly");

      }

    } else {

      console.info("Files don't yet match the manifest");

    }
  }

  return done();

};

const isManifest = message => (JSON.parse(message.Body).Records[0].s3.object.key.indexOf("manifest") > -1);

const getManifestPath = message => (R.head(JSON.parse(message.Body).Records[0].s3.object.key.split("/manifest.json")));

module.exports = { sqsMessageHandler, isManifest, getManifestPath };
