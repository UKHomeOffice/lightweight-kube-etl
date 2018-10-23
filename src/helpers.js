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

module.exports = {
  isTimestamp,
  hasTimestampFolders,
  getIngestJobParams,
  getJobLabels,
  filterJobs,
  getStatus,
  getIngestFiles,
  getJobDuration,
  getPodStatus
}