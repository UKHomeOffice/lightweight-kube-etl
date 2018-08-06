"use strict"

const {S3_ACCESS_KEY, S3_SECRET_KEY, REGION} = process.env
const AWS = require("aws-sdk")

const client = new AWS.S3({
  accessKeyId: S3_ACCESS_KEY,
  secretAccessKey: S3_SECRET_KEY,
  region: REGION
})

const getManifest = async (bucket, manifest_file = "manifest.json") =>
  client
    .getObject({Bucket: bucket, Key: manifest_file})
    .promise()
    .then(object => ({
      data: JSON.parse(object.Body.toString("utf8"))
    }))

const getObjectHash = async (bucket, key) =>
  client
    .headObject({Bucket: bucket, Key: key})
    .promise()
    .then(object => object.ETag)

const checkManifest = async bucket => {
  const manifest = await getManifest(bucket)
  const res = await Promise.all(
    manifest.data.map(
      async object =>
        (await getObjectHash(bucket, object.FileName)) === object.SHA256
    )
  )

  return !res.includes(false)
}

const getJobType = async (bucket, key) => {
  let isFound = false
  const keyJobType = key.indexOf("incremental") > -1 ? "delta" : "bulk"

  if (key.indexOf("incremental") > -1 || key.indexOf("bulk") > -1) {
    isFound =
      (await client.headObject({Bucket: bucket, Key: key}).promise()) == null
        ? false
        : true //?
  }

  return isFound ? keyJobType : null //?
}

module.exports = {
  client,
  getManifest,
  getObjectHash,
  checkManifest,
  getJobType
}
