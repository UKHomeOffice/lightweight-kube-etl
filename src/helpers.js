const R = require('ramda');
const moment = require('moment');

/*
.##.....##.########.##.......########..########.########...######.
.##.....##.##.......##.......##.....##.##.......##.....##.##....##
.##.....##.##.......##.......##.....##.##.......##.....##.##......
.#########.######...##.......########..######...########...######.
.##.....##.##.......##.......##........##.......##...##.........##
.##.....##.##.......##.......##........##.......##....##..##....##
.##.....##.########.########.##........########.##.....##..######.
*/

const isTimestamp = label => !!(label && moment.unix(label).isValid());

const hasTimestampFolders = R.compose(
  R.any(isTimestamp),
  R.map(R.compose(R.head, R.tail, R.split('/'), R.prop('Key'))),
  R.prop('Contents')
);

const getIngestJobParams = folder => {
  // const oldestFolder = R.compose(
  //   R.head,
  //   R.sort((older, newer) => (older[1] > newer[1])),
  //   R.filter(R.compose(R.contains(R.__, ["bulk.txt", "incremental.txt"]), R.last)),
  //   R.map(R.take(3)),
  //   R.map(R.compose(R.split("/"), R.prop("Key"))),
  //   R.prop('Contents')
  // )(folder);

  const splits = R.compose(
    R.map(R.take(3)),
    R.map(R.compose(R.split("/"), R.prop("Key"))),
    R.prop('Contents')
  )(folder);

  console.log("splits: " + splits);

  const filtered = R.compose(
    R.filter(R.compose(R.contains(R.__, ["bulk.txt", "incremental.txt"]), R.last)),
  )(splits);

  console.log("");
  console.log("filtered: " + filtered);

  // const sorted = R.compose(
  //   R.sort((older, newer) => (older[1] > newer[1])),
  // )(filtered);

//  const sorted = R.sort.sort((older, newer) => (older[1] > newer[1]),filtered);
  
const sorted = R.sort((older, newer) => {
  var result = 0;
  if (older[1] > newer[1]) {
    result = 1;
  } else if (older[1] < newer[1]) {
    result = -1;
  }
  return result;
},filtered);


  console.log("");
  console.log("sorted: " + sorted);

  const oldestFolder = R.compose(
    R.head
  )(sorted);

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

const getIngestFiles = ({ingestFolder,ingestName}) => R.compose(
  R.concat([{Key: `${ingestFolder}${ingestName}/manifest.json`}, {Key: `${ingestFolder}${ingestName}`}]),
  R.filter(R.compose(R.contains(ingestName), R.split('/'), R.prop('Key'))),
  R.map(R.pick(['Key'])),
  R.prop('Contents')
);

const getJobDuration = (start, end) => {
  if (!end || !end.diff) return 'timestamp error';
  
  const seconds = end.diff(start, 'seconds');
  const hours = Math.floor(seconds / 3600) % 24;
  const minutes = Math.floor(seconds / 60) % 60;
  
  return `${hours}h:${minutes < 10 ? `0${minutes}` : minutes}mins`;
}

const getPodStatus = R.compose(
  R.propOr(false, 'ready'),
  R.head,
  R.filter(R.propEq('name', 'build')),
  R.pathOr([], ['status', 'containerStatuses'])
)

const getPodStartedAt = R.compose(
  R.pathOr(false, ['state', 'running', 'startedAt']),
  R.head,
  R.filter(R.propEq('name', 'build')),
  R.pathOr([], ['status', 'containerStatuses'])  
)

class Times {
  constructor () {
    this.neoStart = null;
    this.neoEnd = null;
    this.elasticStart = null;
    this.elasticEnd = null;
    this.ingestFiles = null;
  }

  setNeoStart () { this.neoStart = moment(new Date()); }
  getNeoStart () { return this.neoStart; }
  setNeoEnd () { this.neoEnd = moment(new Date()); }
  getNeoEnd () { return this.neoEnd; }

  setElasticStart () { this.elasticStart = moment(new Date()); }
  getElasticStart () { return this.elasticStart; }
  setElasticEnd () { this.elasticEnd = moment(new Date()); }
  getElasticEnd () { return this.elasticEnd; }

  setIngestFiles (files) { this.ingestFiles = files; }
  getIngestFiles () { return this.ingestFiles; }

  isComplete () { return moment(this.neoEnd).isValid() && moment(this.elasticEnd).isValid() }

  reset () {
    this.neoStart = null;
    this.neoEnd = null;
    this.elasticStart = null;
    this.elasticEnd = null;
    this.ingestFiles = null;
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
  getPodStartedAt,
  Times
}