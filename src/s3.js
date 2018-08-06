"use strict"

const {S3_ACCESS_KEY, S3_SECRET_KEY, REGION} = process.env
const AWS = require("aws-sdk")
const R = require("ramda")

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

const isFileFound = async (bucket, keyPath, file) => {
  let isFound = false
  try {
    isFound =
      (await client
        .headObject({Bucket: bucket, Key: keyPath + file})
        .promise()) == null
        ? false
        : true //?
  } catch (e) {
    console.log(`Exception retrieveing file metadata: ${e.message}`) //?
  }
  return isFound
}

const jobFiles = [
  {type: "delta", path: "/incremental.txt"},
  {type: "bulk", path: "/bulk.txt"}
]

const getJobType = async (bucket, keyPath) => {
  const res = await Promise.all(
    R.map(async a => {
      const isFound = await isFileFound(bucket, keyPath, a.path) //?
      return {
        type: a.type,
        isFound
      }
    })(jobFiles)
  ) //?

  return R.compose(
    R.prop("type"),
    R.head,
    R.filter(a => a.isFound)
  )(res) //?
}

module.exports = {
  client,
  getManifest,
  getObjectHash,
  checkManifest,
  getJobType
}
