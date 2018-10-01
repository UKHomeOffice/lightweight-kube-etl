const AWS = require("aws-sdk");
const { MongoClient } = require("mongodb");
const { control_loop } = require('./src/ingestor');

const {
  S3_ACCESS_KEY,
  S3_SECRET_KEY,
  REGION,
  MONGO_CONN,
  KUBE_SERVICE_ACCOUNT_TOKEN
} = process.env;

const s3_client = new AWS.S3({
  accessKeyId: S3_ACCESS_KEY,
  secretAccessKey: S3_SECRET_KEY,
  region: REGION
});

const kube_base_cmd = `kubectl --token ${KUBE_SERVICE_ACCOUNT_TOKEN} `;

MongoClient.connect(`${MONGO_CONN}entitysearch`, { useNewUrlParser: true }, (err, mongo_client) => {
  const mongodb = mongo_client.db('entitysearch');
  const mongo = mongodb.collection('es_load_dates');

  control_loop(s3_client, mongo, kube_base_cmd);
});