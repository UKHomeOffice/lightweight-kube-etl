/*

.##....##.##.....##.########..########.........########.########.##......
.##...##..##.....##.##.....##.##...............##..........##....##......
.##..##...##.....##.##.....##.##...............##..........##....##......
.#####....##.....##.########..######...#######.######......##....##......
.##..##...##.....##.##.....##.##...............##..........##....##......
.##...##..##.....##.##.....##.##...............##..........##....##......
.##....##..#######..########..########.........########....##....########

This is the KUBE-ETL that manages the ingestion of data into entity search.
At present we have two data stores, 'elastic' and 'neo4j'. The following script
basically executes these 10 steps:

1) Keep looking into an s3 bucket for timestamped folders.
2) Take the oldest timestamped folder and wait for it to have a 'manifest.json' file in it.
3) Work out from the folder what kind of ingest it is 'delta' or 'bulk'.
4) Delete any jobs for that kind of ingest
5) Create job labels for the next ingest
6) Trigger a neo4j job first and wait for the container to be ready (that indicates the end of the job).
7) Trigger the elastic job (takes much less time) wait for this to complete.
8) When both jobs are finished then delete the folder from s3.
9) Work out how long everything took, and write that to mongodb.
10) Exit the process with a zero code to start the whole thing again.

*/

const R = require('ramda');
const moment = require('moment');
const AWS = require("aws-sdk");
const EventEmitter = require('events');
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

const getPodStatus = (podStatus, startTime) => {
  const [ready, state] = R.compose(
    R.props(['ready', 'state']),
    R.head,
    R.filter(R.propEq('name', 'build')),
    R.pathOr([], ['status', 'containerStatuses'])
  )(podStatus);

  const startedAt = R.pathOr(null, ['running', 'startedAt'])(state);
  
  return ready && startedAt && moment(startedAt).isAfter(startTime);
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

      waitForManifest(ingestParams)
    }
  });
};

function waitForManifest (ingestParams) {
  
  const { ingestName } = ingestParams;
  const manifestPrefix = `pending/${ingestName}/manifest.json`;

  s3.listObjectsV2({Bucket, Prefix: manifestPrefix, Delimiter: ""}, (err, {Contents}) => {
    return !Contents.length
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
  
  const deleteJobs = spawn('kubectl', R.concat(baseArgs, ['delete', 'jobs', currentNeoJob, currentElasticJob]));  
  
  const jobs = [
    {
      db: 'neo4j',
      name: `elastic-${jobType}-${ingestName}`,
      cronJobName: `elastic-${jobType}`,
      pods: ['elasticsearch-0', 'elasticsearch-1']
    },
    {
      db: 'elastic',
      name: `neo4j-${jobType}-${ingestName}`,
      cronJobName: `neo4j-${jobType}`,
      pods: ['neo4j-0', 'neo4j-1']
    }
  ];
  
  deleteJobs.on('exit', () => createNewJobs({ingestType, ingestName}, jobs));
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

class WaitForJob extends EventEmitter {
  constructor(job) {
    super();

    this.db = job.db;
    this.pod0 = job.pods[0];
    this.pod1 = job.pods[1];
    this.pod0_ready = false;
    this.pod1_ready = false;
    this._poll_pod0();
    this._poll_pod1();
    this._hasFinished();
  }

  _poll_pod0 () {
    const self = this;
    
    exec(`kubectl ${R.join(' ', baseArgs)} get pods ${self.pod0} -o json`, (err, stdout, stderr) => {
      if (err || stderr) {
        return setTimeout(() => self._poll_pod0(), pollingInterval);
      }

      const startTime = self.db === 'neo4j' ? neoStartTime : elasticStartTime;
      const ready = getPodStatus(JSON.parse(stdout), startTime);
      
      ready ? self.pod0_ready = true : setTimeout(() => self._poll_pod0(), pollingInterval);
    });
  }

  _poll_pod1 () {
    const self = this;
    
    exec(`kubectl ${R.join(' ', baseArgs)} get pods ${self.pod1} -o json`, (err, stdout, stderr) => {
      if (err || stderr) {
        return setTimeout(() => self._poll_pod1(), pollingInterval);
      }

      const startTime = self.db === 'neo4j' ? neoStartTime : elasticStartTime;
      const ready = getPodStatus(JSON.parse(stdout), startTime);
      
      ready ? self.pod1_ready = true : setTimeout(() => self._poll_pod1(), pollingInterval);
    });
  }

  _hasFinished () {
    const self = this;

    self.pod0_ready && self.pod1_ready
      ? self.emit('finished')
      : setTimeout(() => self._hasFinished(), pollingInterval);
  }
};

function createNewJobs(ingestParams, jobs) {
  
  if (!jobs.length) return waitForCompletion(ingestParams);

  const job = jobs.pop();

  const startTime = moment(new Date());

  job.db === 'neo4j' ? neoStartTime = startTime : elasticStartTime = startTime;

  console.log(`${moment(neoStartTime).format('MMM Do HH:mm')}: starting ${job.name}`);

  const args = R.concat(baseArgs, ['create', 'job', job.name, '--from', `cronjob/${job.cronJobName}`]);

  const handleError = (code, sig) => {
    if (code !== 0) {
      console.error(`${moment(new Date).format('MMM Do HH:mm')}: ERROR ${job.name} terminated with signal ${sig}`);
      enterErrorState();
    }
  }
  
  const jobPod = spawn('kubectl', args, {env: process.env});
  
  jobPod.on('exit', handleError);
  jobPod.on('error', handleError);

  waitForJob = new WaitForJob(job);
  waitForJob.on('finished', () => {
    const endTime = moment(new Date());
    job.db === 'neo4j' ? neoEndTime = endTime : elasticEndTime = endTime;
    createNewJobs(ingestParams, jobs);
  });
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
        console.error(`${ts.format('MMM Do HH:mm')}: ${JSON.stringify(err, null, 2)}`);
        enterErrorState();
      } else {
        
        const store_ingest_details = {
          ingest: ingestName,
          loadDate: Date.now(),
          neo_job_duration: getJobDuration(neoStartTime, neoEndTime),
          elastic_job_duration: getJobDuration(elasticStartTime, elasticEndTime),
          total_job_duration: getJobDuration(neoStartTime, ts)
        }
        
        console.log(`${ts.format('MMM Do HH:mm')}: ${JSON.stringify(store_ingest_details, null, 4)}`);

        mongoClient(store_ingest_details).then(process.exit);
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
