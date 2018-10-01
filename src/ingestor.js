const R = require('ramda');
const moment = require('moment');
const pollingInterval = 1000; // 1 minuet
const { BUCKET: Bucket } = process.env;

const isTimestamp = label => moment.unix(label).isValid();

const hasTimestampFolders = R.compose(
  R.any(isTimestamp),
  R.map(R.compose(R.head, R.tail, R.split('/'), R.prop('Key'))),
  R.prop('Contents')
);

const getIngestJobParams = R.compose(
  R.evolve({ingestType: R.replace(".txt", "")}),
  R.zipObj(["ingestName", "ingestType"]),
  R.tail,
  R.head,
  R.sort((older, newer) => (older[1] > newer[1])),
  R.filter(R.compose(R.contains(R.__, ["bulk.txt", "incremental.txt"]), R.last)),
  R.map(R.take(3)),
  R.map(R.compose(R.split("/"), R.prop("Key"))),
  R.prop('Contents')
);

function start (s3, kubectl) {
  s3.listObjectsV2({Bucket, Prefix: "pending/", Delimiter: ""}, (err, folder) => {

    if (err) {
      console.error(JSON.stringify(err, null, 2));
      
      return setTimeout(start, pollingInterval);

    } else if (!folder || !folder.Contents.length) {
      
      return setTimeout(start, pollingInterval);

    } else if (!hasTimestampFolders(folder)) {
      
      return setTimeout(start, pollingInterval);

    } else {
      const ingestParams = getIngestJobParams(folder);

      waitForManifest(s3, ingestParams, kubectl)
    }
  });
};

function waitForManifest (s3, ingestParams, kubectl) {
  const { ingestName } = ingestParams;
  const manifestPrefix = `pending/${ingestName}/manifest.json`;

  s3.listObjectsV2({Bucket, Prefix: manifestPrefix, Delimiter: ""}, (err, {Contents}) => {
    return !Contents.length
      ? setTimeout(() => waitForManifest(s3, ingestParams, kubectl), pollingInterval)
      : triggerIngest(ingestParams, kubectl);
  });
};

function triggerIngest (ingestParams, kubectl) {
  const {ingestType, ingestName} = ingestParams;


  console.log({ingestType, ingestName});
}

module.exports = {
  start,
  hasTimestampFolders,
  getIngestJobParams
};
