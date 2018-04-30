"use strict"

const {SQS_ACCESS_KEY, SQS_SECRET_KEY, REGION} = process.env

const AWS = require("aws-sdk")
const sqs = require("sqs-consumer")

const client = new AWS.SQS({
  accessKeyId: SQS_ACCESS_KEY,
  secretAccessKey: SQS_SECRET_KEY,
  region: REGION
})

const create_queue = handler =>
  sqs.create({
    queueUrl: QUEUE,
    handleMessage: handler,
    sqs: client
  })

module.exports = {
  create_queue
}
