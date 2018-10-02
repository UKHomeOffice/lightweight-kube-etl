const R = require('ramda');
const moment = require('moment');
const AWS = require("aws-sdk");
const { spawn, exec } = require('child_process');
const { insert: mongoClient } = require("./mongodb");

const { 
  BUCKET: Bucket, 
  KUBE_SERVICE_ACCOUNT_TOKEN,
  S3_ACCESS_KEY,
  S3_SECRET_KEY,
  REGION,
  NODE_ENV = 'production'
} = process.env;

const s3 = new AWS.S3({
  accessKeyId: S3_ACCESS_KEY,
  secretAccessKey: S3_SECRET_KEY,
  region: REGION
});

let neoStartTime, neoEndTime = null, elasticStartTime, elasticEndTime = null, ingestFiles;

const pollingInterval = NODE_ENV === 'test' ? 1000 : 1000 * 60;

const isTimestamp = label => moment.unix(label).isValid();

let baseArgs = ['--token', KUBE_SERVICE_ACCOUNT_TOKEN];

if (NODE_ENV === 'test') {
  baseArgs = R.concat(['--context', 'acp-notprod_DACC', '-n', 'dacc-entitysearch'], baseArgs);
}

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

const getStatus = R.pathOr(false, ['status', 'succeeded']);

const getIngestFiles = ({ingestName}) => R.compose(
  R.concat([{Key: `pending/${ingestName}/manifest.json`}, {Key: `pending/${ingestName}`}]),
  R.filter(R.compose(R.contains(ingestName), R.split('/'), R.prop('Key'))),
  R.map(R.pick(['Key'])),
  R.prop('Contents')
);

const getJobDuration = (start, end) => {
  const seconds = end.diff(start, 'seconds');
  const hours = Math.floor(seconds / 3600) % 24;
  const minutes = Math.floor(seconds / 60) % 60;
  return `${hours}h:${minutes < 10 ? `0${minutes}` : minutes}mins`;
}

/*
..######..########....###....########..########
.##....##....##......##.##...##.....##....##...
.##..........##.....##...##..##.....##....##...
..######.....##....##.....##.########.....##...
.......##....##....#########.##...##......##...
.##....##....##....##.....##.##....##.....##...
..######.....##....##.....##.##.....##....##...
*/

function start () {
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
      
      ingestFiles = getIngestFiles(ingestParams)(folder);
      
      console.log(`new ${ingestParams.ingestType} ingest detected in folder ${ingestParams.ingestName}  - waiting for manifest file...`)

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
      : deleteOldJobs(ingestParams);
  });
};

function deleteOldJobs (ingestParams) {

  const {ingestType, ingestName} = ingestParams;
  const forIngestType = ingestType === 'incremental' ? new RegExp(/-delta-/) : new RegExp(/-bulk-/);
  
  exec(`kubectl ${baseArgs.join(' ')} get jobs -o json`, (err, stdout, stderr) => {
    if (err) {
      console.error(err);
      return enterErrorState();
    }

    const jobsToDelete = getJobLabels(forIngestType)(JSON.parse(stdout));

    spawnIngestJobs(ingestParams, jobsToDelete);
  });
}

function spawnIngestJobs ({ingestType, ingestName}, jobsToDelete) {
  const type = ingestType === 'incremental' ? 'delta' : ingestType;
  const currentNeoJob = R.pipe(R.filter( R.startsWith(`neo4j-${type}`)), R.head)(jobsToDelete);
  const currentElasticJob = R.pipe(R.filter( R.startsWith(`elastic-${type}`)), R.head)(jobsToDelete);
  const nextNeoJob = `neo4j-${type}-${ingestName}`;
  const nextElasticJob = `elastic-${type}-${ingestName}`;

  const deleteNeo = spawn('kubectl', R.concat(baseArgs, ['delete', 'jobs', currentNeoJob]), {env: process.env});
  
  deleteNeo.on('exit', () => startNeoIngest(nextNeoJob, type));

  const deleteElastic = spawn('kubectl', R.concat(baseArgs, ['delete', 'jobs', currentElasticJob]), {env: process.env});
  
  deleteNeo.on('exit', () => startElasticIngest(nextElasticJob, type));
  
  waitForCompletion({ingestType, ingestName});
}

/*
.##....##.########..#######..##..............##
.###...##.##.......##.....##.##....##........##
.####..##.##.......##.....##.##....##........##
.##.##.##.######...##.....##.##....##........##
.##..####.##.......##.....##.#########.##....##
.##...###.##.......##.....##.......##..##....##
.##....##.########..#######........##...######.
*/

function startNeoIngest (nextNeoJob, cronjobType) {
  neoStartTime = moment(new Date());

  console.log(`${moment(neoStartTime).format('MMM Do hh:mm')}: starting neo ingest ${nextNeoJob}`);
   
  const args = R.concat(baseArgs, ['create', 'job', nextNeoJob, '--from', `cronjob/neo4j-${cronjobType}`]);

  const job = spawn('kubectl', args, {env: process.env});

  job.on('exit', (code) => {
    if (code !== 0) {
      console.error(`${moment(new Date).format('MMM Do hh:mm')}: ERROR ${nextNeoJob} exits with non-zero code ${code}`);
      enterErrorState();
    } else {
      console.error(`${moment(new Date).format('MMM Do hh:mm')}: created new job ${nextNeoJob}`);
      setTimeout(() => isJobCompleted(nextNeoJob, false), pollingInterval);
    }
  });
}

function onNeoCompleted (jobName) {
  neoEndTime = moment(new Date);

  const jobDuration = getJobDuration(neoStartTime, neoEndTime);

  console.log(`${neoEndTime.format('MMM Do hh:mm')}: completed ${jobName} in ${jobDuration}`);
}

/*
.########.##..........###.....######..########.####..######.
.##.......##.........##.##...##....##....##.....##..##....##
.##.......##........##...##..##..........##.....##..##......
.######...##.......##.....##..######.....##.....##..##......
.##.......##.......#########.......##....##.....##..##......
.##.......##.......##.....##.##....##....##.....##..##....##
.########.########.##.....##..######.....##....####..######.
*/

function startElasticIngest (nextElasticJob, cronjobType) {
  elasticStartTime = moment(new Date());

  console.log(`${moment(elasticStartTime).format('MMM Do hh:mm')}: starting elastic ingest ${nextElasticJob}`);
   
  const args = R.concat(baseArgs, ['create', 'job', nextElasticJob, '--from', `cronjob/elastic-${cronjobType}`]);

  const job = spawn('kubectl', args, {env: process.env});

  job.on('exit', (code) => {
    if (code !== 0) {
      console.error(`${moment(new Date).format('MMM Do hh:mm')}: ERROR ${nextElasticJob} exits with non-zero code ${code}`);
      enterErrorState();
    } else {
      console.error(`${moment(new Date).format('MMM Do hh:mm')}: created new job ${nextElasticJob}`);
      setTimeout(() => isJobCompleted(nextElasticJob, false), pollingInterval);
    }
  });
}

function onElasticComplete (jobName) {
  elasticEndTime = moment(new Date);

  const jobDuration = getJobDuration(elasticStartTime, elasticEndTime);

  console.log(`${elasticEndTime.format('MMM Do hh:mm')}: completed ${jobName} in ${jobDuration}`);  
}

function enterErrorState () {
  setTimeout(enterErrorState, pollingInterval);
}

/*
.########.####.##....##.####..######..##.....##
.##........##..###...##..##..##....##.##.....##
.##........##..####..##..##..##.......##.....##
.######....##..##.##.##..##...######..#########
.##........##..##..####..##........##.##.....##
.##........##..##...###..##..##....##.##.....##
.##.......####.##....##.####..######..##.....##
*/

function waitForCompletion ({ingestType, ingestName}) {
  const complete = moment(neoEndTime).isValid() && moment(elasticEndTime).isValid();

  if (!complete) {
    setTimeout(() => waitForCompletion({ingestType, ingestName}), pollingInterval);
  } else {
    const deleteParams = {
      Bucket,
      Delete: {
        Objects: ingestFiles,
        Quiet: true
      }
    }

    s3.deleteObjects(deleteParams, err => {
      const ts = moment(new Date());
     
      if (err) {
        console.error(`${ts.format('MMM Do hh:mm')}: ${JSON.stringify(err, null, 2)}`);
        enterErrorState();
      } else {
        
        const store_ingest_details = {
          ingest: ingestName,
          loadDate: Date.now(),
          neo_job_duration: getJobDuration(neoStartTime, neoEndTime),
          elastic_job_duration: getJobDuration(elasticStartTime, elasticEndTime),
          total_job_duration: getJobDuration(neoStartTime, ts)
        }
        
        console.log(`${ts.format('MMM Do hh:mm')}: ${JSON.stringify(store_ingest_details, null, 4)}`);

        mongoClient(store_ingest_details).then(process.exit);
      }
    })
  }
}

function isJobCompleted (jobName, status) {
  if (status) {
    R.startsWith('neo4j')(jobName) ? onNeoCompleted(jobName) : onElasticComplete(jobName);
  } else {
    exec(`kubectl ${baseArgs.join(' ')} get jobs ${jobName} -o json`, (err, stdout, stderr) => {
      if (err) {
        console.error(err);
        return enterErrorState();
      }

      const status = getStatus(JSON.parse(stdout));
  
      setTimeout(() => isJobCompleted(jobName, status), pollingInterval);
    });
  }
}

module.exports = {
  start,
  hasTimestampFolders,
  getIngestJobParams,
  getStatus,
  getIngestFiles,
  getJobDuration
};
