const {BUCKET, ROLE, CRONJOB, QUEUE} = process.env

const exec = require("util").promisify(require("child_process").exec)
const sqs = require("sqs-consumer")
const AWS = require("aws-sdk")
const s3 = new AWS.S3()
const crypto = require("crypto")

const start_kube_job = async (job_id = crypto.randomBytes(16).toString("hex")) => {
  await exec(`./kubectl delete job -l role=${ROLE}`)
  const {stdout, stderr} = await exec(`./kubectl create job ${CRONJOB}-${job_id} --from=cronjob/${CRONJOB}`)
  await exec(`./kubectl label job ${CRONJOB}-${job_id} role=${ROLE}`)
  console.info(stdout)

  if (stderr) {
    console.error(stderr)
    throw stderr
  }
}

const check_manifest = async () => {
  const manifest = await s3.getObject({Bucket: BUCKET, Key: "manifest.json"}).promise()
  await manifest.data.map(async object =>
    await get_object_hash(object.Path) === object.Hash
  )
  return manifest.includes(false)
}

const get_object_hash = async (key) =>
  s3.headObject({Bucket: BUCKET, Key: key}).promise()
    .then(object => object.ETag)

const sqs_message_handler = async (message, done) => {
  if (await check_manifest())
    await start_kube_job()
  else
    console.info("Files don't yet match the manifest")
  done()
}

const app = sqs.create({
  queueUrl: QUEUE,
  handleMessage: sqs_message_handler
})

app.on("error", (err) => console.error(err.message))

module.exports = {
  start_kube_job: start_kube_job,
  check_manifest: check_manifest,
  get_object_hash: get_object_hash,
  sqs_message_handler: sqs_message_handler
}

if (!global.it)
  app.start()