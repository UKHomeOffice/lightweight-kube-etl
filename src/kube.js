"use strict"

const crypto = require("crypto")
const {promisify} = require("util")
const exec = promisify(require("child_process").exec)

const create_kube_job = (job_id, cronjob) =>
  exec(
    `../../kubectl create job ${cronjob}-${job_id} --from=cronjob/${cronjob}`
  )

const label_kube_job = (job_id, cronjob, role) =>
  exec(
    `../../kubectl label job ${cronjob}-${job_id} role=${role}`
  )

const delete_kube_job = role =>
  exec(`../../kubectl delete job -l role=${role}`)

const start_kube_job = async (
  role,
  cronjob,
  job_id = crypto.randomBytes(16).toString("hex")
) => {
  await delete_kube_job(role)
  const {stdout, stderr} = await create_kube_job(job_id, cronjob)
  await label_kube_job(job_id, cronjob, role)

  if (stderr) {
    console.error(stderr)
    throw stderr
  }
}

module.exports = {
  start_kube_job
}
