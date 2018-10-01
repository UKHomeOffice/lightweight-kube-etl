const AWS = require("aws-sdk");
const { insert: mongoClient } = require("./src/mongodb");
const { start } = require('./src/ingestor');

const {
  S3_ACCESS_KEY,
  S3_SECRET_KEY,
  REGION,
  KUBE_SERVICE_ACCOUNT_TOKEN
} = process.env;

const s3_client = new AWS.S3({
  accessKeyId: S3_ACCESS_KEY,
  secretAccessKey: S3_SECRET_KEY,
  region: REGION
});

start(s3_client);