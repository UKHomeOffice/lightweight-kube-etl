const R = require('ramda');
const moment = require('moment');
const pollingInterval = 1000; // 1 minuet
const { BUCKET: Bucket, KUBE_SERVICE_ACCOUNT_TOKEN } = process.env;
const { Readable } = require('stream');
const { spawn, exec } = require('child_process');

const isTimestamp = label => moment.unix(label).isValid();

const baseArgs = [
  '--context', 'acp-notprod_DACC',
  '-n', 'dacc-entitysearch',
  '--token', KUBE_SERVICE_ACCOUNT_TOKEN
]

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

const getJobLabels = forIngestType => R.compose(
  R.filter(R.test(forIngestType)),
  R.map(R.path(['metadata', 'name'])),
  R.filter(filterJobs),
  R.prop('items')
);

const filterJobs = R.compose(
  R.gt(R.__, 0),
  R.length,
  R.intersection(['neo4j', 'elastic']),
  R.split('-'),
  R.pathOr('', ['metadata', 'name']),
);

function start (s3) {
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

      waitForManifest(s3, ingestParams)
    }
  });
};

function waitForManifest (s3, ingestParams) {
  
  const { ingestName } = ingestParams;
  const manifestPrefix = `pending/${ingestName}/manifest.json`;

  s3.listObjectsV2({Bucket, Prefix: manifestPrefix, Delimiter: ""}, (err, {Contents}) => {
    return !Contents.length
      ? setTimeout(() => waitForManifest(s3, ingestParams), pollingInterval)
      : triggerIngest(ingestParams);
  });
};

function triggerIngest (ingestParams) {

  const {ingestType, ingestName} = ingestParams;
  const forIngestType = ingestType === 'incremental' ? new RegExp(/-delta-/) : new RegExp(/-bulk-/);
  
  exec(`kubectl ${baseArgs.join(' ')} get jobs -o json`, (err, stdout, stderr) => {
    if (err) return console.error(err);

    const jobsToDelete = getJobLabels(forIngestType)(JSON.parse(stdout));

    spawnIngestJobs(ingestParams, jobsToDelete);
  });
}

function spawnIngestJobs ({ingestType, ingestName}, jobsToDelete) {
  const currentNeoJob = R.pipe(R.filter( R.startsWith(`neo4j-${ingestType}`)), R.head)(jobsToDelete);
  const currentElasticJob = R.pipe(R.filter( R.startsWith(`elastic-${ingestType}`)), R.head)(jobsToDelete);
  const nextNeoJob = `neo4j-${ingestType}-${ingestName}`;
  const nextElasticJob = `elastic-${ingestType}-${ingestName}`;
  const cronjobType = ingestType === 'incremental' ? 'delta' : 'bulk'

  const deleteNeo = spawn('kubectl', R.concat(baseArgs, ['delete', 'jobs', currentNeoJob]), {env: process.env});
  deleteNeo.on('exit', () => startNeoIngest(nextNeoJob, cronjobType));

  const deleteElastic = spawn('kubectl', R.concat(baseArgs, ['delete', 'jobs', currentElasticJob]), {env: process.env});
  deleteNeo.on('exit', () => startElasticIngest(nextNeoJob, cronjobType));
}

function startNeoIngest (nextNeoJob, cronjobType) {
  console.log('starting neo ingest', nextNeoJob);
  
  const args = R.concat(baseArgs, ['create', 'job', nextNeoJob, '--from', `cronjob/neo-${cronjobType}`]);
  const job = spawn('kubectl', args, {env: process.env});
  
  job.on('exit', (code, sig) => console.log('neo exits with code', code));
}

function startElasticIngest (nextElasticJob, cronjobType) {
  console.log('starting elastic ingest', nextElasticJob);
  
  const args = R.concat(baseArgs, ['create', 'job', nextElasticJob, '--from', `cronjob/elastic-${cronjobType}`]);
  const job = spawn('kubectl', args, {env: process.env});
  
  job.on('exit', (code, sig) => console.log('elastic exits with code', code));
}

module.exports = {
  start,
  hasTimestampFolders,
  getIngestJobParams
};
