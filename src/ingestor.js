/*

.##....##.##.....##.########..########.........########.########.##......
.##...##..##.....##.##.....##.##...............##..........##....##......
.##..##...##.....##.##.....##.##...............##..........##....##......
.#####....##.....##.########..######...#######.######......##....##......
.##..##...##.....##.##.....##.##...............##..........##....##......
.##...##..##.....##.##.....##.##...............##..........##....##......
.##....##..#######..########..########.........########....##....########

This is the KUBE-ETL that manages the jobs that perform the ingestion of data into entity search.
At present we have two datastores; 'elastic' and 'neo4j'. The following script
basically executes 10 steps:

1) Keep looking into an s3 bucket for timestamped folders.
2) Take the oldest timestamped folder and wait for it to have a 'manifest.json' file in it.
3) Work out from the folder what kind of ingest it is 'delta' or 'bulk'.
4) Delete any jobs for that kind of ingest.
5) If it is a bulk trigger the ingests in parallel, if it is a delta, do neo4j first then elastic.
6) Wait for all the jobs to finish.
7) Wait for all the pods to be ready.
8) When both jobs are finished and all the pods are up then delete the ingest folder from s3.
9) Work out how long everything took, and write that to mongodb.
10) Exit the process with a zero code then kubernetes will start the whole thing again.

*/

const R = require('ramda');
const moment = require('moment');
const async = require('async');
const AWS = require("aws-sdk");
const { spawn, exec } = require('child_process');
const { insert: mongoClient } = require("./mongodb");

const { 
  BUCKET: Bucket, 
  KUBE_SERVICE_ACCOUNT_TOKEN,
  S3_ACCESS_KEY,
  S3_SECRET_KEY,
  NODE_ENV = 'production'
} = process.env;

const s3 = new AWS.S3({
  accessKeyId: S3_ACCESS_KEY,
  secretAccessKey: S3_SECRET_KEY,
  region: 'eu-west-2'
});

let neoStartTime, neoEndTime = null, elasticStartTime, elasticEndTime = null, ingestFiles;

const pollingInterval = NODE_ENV === 'test' ? 1000 : 1000 * 30;

const isTimestamp = label => !!(label && moment.unix(label).isValid());

let baseArgs = ['--token', KUBE_SERVICE_ACCOUNT_TOKEN];

if (NODE_ENV === 'test') {
  baseArgs = R.concat(['--context', 'acp-notprod_DACC', '-n', 'dacc-entitysearch'], baseArgs);
}

const hasTimestampFolders = R.compose(
  R.any(isTimestamp),
  R.map(R.compose(R.head, R.tail, R.split('/'), R.prop('Key'))),
  R.prop('Contents')
);

const getIngestJobParams = folder => {
  const oldestFolder = R.compose(
    R.head,
    R.sort((older, newer) => (older[1] > newer[1])),
    R.filter(R.compose(R.contains(R.__, ["bulk.txt", "incremental.txt"]), R.last)),
    R.map(R.take(3)),
    R.map(R.compose(R.split("/"), R.prop("Key"))),
    R.prop('Contents')
  )(folder);

  if (!oldestFolder) return;

  return R.compose(
    R.evolve({ingestType: R.replace(".txt", "")}),
    R.zipObj(["ingestName", "ingestType"]),
    R.tail,
  )(oldestFolder);
}

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

const getPodStatus = R.compose(
  R.prop('ready'),
  R.head,
  R.filter(R.propEq('name', 'build')),
  R.pathOr([], ['status', 'containerStatuses'])
)

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

      if (!ingestParams) {
        console.error('error in s3 bucket - check folders');
        return setTimeout(start, pollingInterval);
      }
      
      ingestFiles = getIngestFiles(ingestParams)(folder);
      
      console.log(`new ${ingestParams.ingestType} ingest detected in folder ${ingestParams.ingestName} - waiting for manifest file...`)
      
      waitForManifest(ingestParams)
    }
  });
};

function waitForManifest (ingestParams) {
  
  const { ingestName } = ingestParams;
  const manifestPrefix = `pending/${ingestName}/manifest.json`;

  s3.listObjectsV2({Bucket, Prefix: manifestPrefix, Delimiter: ""}, (err, {Contents}) => {
    !Contents.length
      ? setTimeout(() => waitForManifest(ingestParams), pollingInterval)
      : getOldJobs(ingestParams);
  });
};

function getOldJobs (ingestParams) {

  const {ingestType, ingestName} = ingestParams;
  const forIngestType = ingestType === 'incremental' ? new RegExp(/-delta-/) : new RegExp(/-bulk-/);
  
  exec(`kubectl ${baseArgs.join(' ')} get jobs -o json`, (err, stdout, stderr) => {
    if (err) {
      console.error(err);
      return enterErrorState();
    }

    const jobsToDelete = getJobLabels(forIngestType)(JSON.parse(stdout));

    deleteOldJobs(ingestParams, jobsToDelete);
  });
}

function deleteOldJobs ({ingestType, ingestName}, jobsToDelete) {
  const jobType = ingestType === 'incremental' ? 'delta' : ingestType;
  
  const currentNeoJob = R.pipe(R.filter( R.startsWith(`neo4j-${jobType}`)), R.head)(jobsToDelete);
  const currentElasticJob = R.pipe(R.filter( R.startsWith(`elastic-${jobType}`)), R.head)(jobsToDelete);
  
  if (currentNeoJob && currentElasticJob) {
    console.log(`${moment(new Date()).format('MMM Do HH:mm')}: delete jobs ${currentNeoJob} & ${currentElasticJob}`);
  }

  const deleteJobs = spawn('kubectl', R.concat(baseArgs, ['delete', 'jobs', currentNeoJob, currentElasticJob]));  
  
  const jobs = [
    {
      db: 'neo4j',
      name: `neo4j-${jobType}-${ingestName}`,
      cronJobName: `neo4j-${jobType}`,
      pods: ['neo4j-0', 'neo4j-1']
    },
    {
      db: 'elastic',
      name: `elastic-${jobType}-${ingestName}`,
      cronJobName: `elastic-${jobType}`,
      pods: ['elasticsearch-0', 'elasticsearch-1']
    }
  ];
  
  deleteJobs.on('exit', () => {
    jobType === 'bulk'
    ? createBulkJobs({ingestType, ingestName}, jobs)
    : createDeltaJobs({ingestType, ingestName}, jobs);
  });
}

/*
.......##..#######..########...######.
.......##.##.....##.##.....##.##....##
.......##.##.....##.##.....##.##......
.......##.##.....##.########...######.
.##....##.##.....##.##.....##.......##
.##....##.##.....##.##.....##.##....##
..######...#######..########...######.
*/

function checkPodStatus (podName, podReady) {
  const poll = () => checkPodStatus(podName, podReady);
  
  exec(`kubectl ${R.join(' ', baseArgs)} get pods ${podName} -o json`, (err, stdout, stderr) => {
    if (err || stderr) {
      setTimeout(poll, pollingInterval);
    } else {
      const ready = getPodStatus(JSON.parse(stdout));
  
      ready ? podReady() : setTimeout(poll, pollingInterval);
    }
  });
}

function checkJobStatus (jobName, jobComplete) {
  const poll = () => checkJobStatus(jobName, jobComplete);

  exec(`kubectl ${R.join(' ', baseArgs)} get jobs ${jobName} -o json`, (err, stdout, stderr) => {
    if (err || stderr) {
      setTimeout(poll, pollingInterval);
    } else {
      const ready = getStatus(JSON.parse(stdout));

      ready ? jobComplete() : setTimeout(poll, pollingInterval);
    }
  });
}

const onJobComplete = ingestParams => err => {
  if (err) {
    console.error(err);
    enterErrorState();
  } else {
    waitForCompletion(ingestParams)
  }
}

function waitForPods (job, next) {
  const checks = R.map(podName => ready => checkPodStatus(podName, ready))(job.pods);
  async.parallel(checks, err => next(err));
}

function runJob (job, callback) {
  async.waterfall([
    next => waitForPods(job, next),
    next => {
      
      const args = R.concat(baseArgs, ['create', 'job', job.name, '--from', `cronjob/${job.cronJobName}`]);
      
      const jobPod = spawn('kubectl', args);
      
      jobPod.on('exit', code => {
        const err = code !== 0 ? new Error(`${job.name} exits with non zero code`) : null;
        next(err);
      });
    },
    next => {      
      const startTime = moment(new Date());
  
      job.db === 'neo4j' ? neoStartTime = startTime : elasticStartTime = startTime;

      console.log(`${moment(new Date()).format('MMM Do HH:mm')}: ${job.name} triggered :)`);
      
      checkJobStatus(job.name, next);
    },
    next => waitForPods(job, next)
  ], err => {
    if (!err) {
      const endTime = moment(new Date());
                
      job.db === 'neo4j' ? neoEndTime = endTime : elasticEndTime = endTime;
  
      console.log(`${endTime.format('MMM Do HH:mm')}: ${job.name} pods ready`);
    }

    callback(err);
  });
}

function createBulkJobs (ingestParams, jobs) {
  const [neo4j, elastic] = jobs;

  async.parallel([
    done => runJob(neo4j, done),
    done => runJob(elastic, done)
  ], onJobComplete(ingestParams));
}

function createDeltaJobs(ingestParams, jobs) {
  async.eachSeries(jobs, runJob, onJobComplete(ingestParams));
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
      const ingestEndTime = moment(new Date());
     
      if (err) {
        console.error(`${ingestEndTime.format('MMM Do HH:mm')}: ${JSON.stringify(err, null, 2)}`);
        enterErrorState();
      } else {
        
        const store_ingest_details = {
          ingest: ingestName,
          type: ingestType,
          loadDate: Date.now(),
          readableDate: moment(new Date()).format('MMM Do HH:mm'),
          neo_job_duration: getJobDuration(neoStartTime, neoEndTime),
          elastic_job_duration: getJobDuration(elasticStartTime, elasticEndTime),
          total_job_duration: getJobDuration(neoStartTime, ingestEndTime)
        }
        
        console.log(`${ingestEndTime.format('MMM Do HH:mm')}: ${JSON.stringify(store_ingest_details, null, 4)}`);

        mongoClient(store_ingest_details).then(() => start());
      }
    })
  }
}

module.exports = {
  isTimestamp,
  hasTimestampFolders,
  getIngestJobParams,
  getJobLabels,
  filterJobs,
  getStatus,
  getIngestFiles,
  getJobDuration,
  getPodStatus,
  start
};
