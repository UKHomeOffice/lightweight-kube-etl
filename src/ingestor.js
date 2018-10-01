const R = require('ramda');
const moment = require('moment');
const pollingInterval = 1 * 60 * 1000; // 1 minuet

const isTimestamp = label => moment.unix(label).isValid();

const hasTimestampFolders = R.compose(
  R.any(isTimestamp),
  R.map(R.compose(R.head, R.tail, R.split('/'), R.prop('Key'))),
  R.prop('Content')
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
  R.prop('Content')
);

function control_loop (s3, mongodb, kubectl) {
  s3.listObjectsV2({Prefix: "pending/", Delimiter: ""}, (err, folder) => {
    if (!folder) {
      setTimeout(control_loop, pollingInterval);
    } else if (!folder.Contents.length) {
      setTimeout(control_loop, pollingInterval);
    } else if (!hasTimestampFolders(folder)) {
      setTimeout(control_loop, pollingInterval);
    } else {
      const ingestParams = getIngestJobParams(folder);
    }
  });
};

module.exports = {
  control_loop,
  hasTimestampFolders,
  getIngestJobParams
};
