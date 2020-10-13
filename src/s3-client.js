const AWS = require("aws-sdk");

const {
  BUCKET: Bucket,
  KUBE_SERVICE_ACCOUNT_TOKEN,
  S3_ACCESS_KEY,
  S3_SECRET_KEY,
  NODE_ENV = "production"
} = process.env;

const s3 = new AWS.S3({
  accessKeyId: S3_ACCESS_KEY,
  secretAccessKey: S3_SECRET_KEY,
  region: "eu-west-2"
});

module.exports = s3;
