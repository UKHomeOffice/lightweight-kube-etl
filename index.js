"use strict"

const R = require('ramda');
const { client } = require('./src/s3');
const KubeAPIClient = require('./src/kubeAPIClient');

const PollingInterval = 1; // number of minutes
const timeout = PollingInterval * 60 * 1000;

const {
  BUCKET,
  KUBE_SERVICE_ACCOUNT_TOKEN
} = process.env;

const kubeClient = new KubeAPIClient(KUBE_SERVICE_ACCOUNT_TOKEN);

function go () {

  client.listObjectsV2({Bucket: BUCKET, Prefix: "pending/", Delimiter: ""}, (err, result) => {
    if (!result) {
      console.error('no results from s3 contents request');
      return setTimeout(go, timeout);
    }

    if (!result.Contents.length) {
      console.error('s3 bucket is empty');
      return setTimeout(go, timeout);
    }
    
    const nextIngestJobParams = _getIngestNameAndType(result.Contents);
    
    poll(nextIngestJobParams, ready);
  });
  
}

function poll (nextIngestJobParams, ready) {
  const manifestPath = `pending/${nextIngestJobParams.ingestName}/manifest.json`;
  
  client.listObjectsV2({Bucket: BUCKET, Prefix: manifestPath, Delimiter: ""}, (err, {Contents}) => {

    if (!Contents.length) {

      setTimeout(() => poll(nextIngestJobParams, ready), timeout);
    } else {
      const manifest = R.head(Contents);
      
      ready(nextIngestJobParams);
    }
  });
}

function ready (nextIngestJobParams) {
  kubeClient.on('msg', msg => console.log(msg));
  kubeClient.on('error', err => console.error(err));
  kubeClient.startNextIngestJob(nextIngestJobParams);
}

const _getIngestNameAndType = R.compose(
  R.evolve({ingestType: R.replace(".txt", "")}),
  R.zipObj(["ingestName", "ingestType"]),
  R.tail,
  R.head,
  R.sort((older, newer) => (older[1] > newer[1])),
  R.filter(R.compose(R.contains(R.__, ["bulk.txt", "incremental.txt"]), R.last)),
  R.map(R.take(3)),
  R.map(R.compose(R.split("/"), R.prop("Key")))
);

go();
