const request = require('request-promise');
const { runJobs } = require('./ingestionService');
const EventEmitter = require('events');
const baseUrl = "http://127.0.0.1:8181";
const R = require('ramda');

class KubeAPIClient extends EventEmitter {
  constructor(KUBE_SERVICE_ACCOUNT_TOKEN) {
    super();
    this.token = KUBE_SERVICE_ACCOUNT_TOKEN
    this.baseOptions = {
      uri: `${baseUrl}/api`,
      method: 'GET',
      headers: {
        Authorization: `Bearer ${this.token}`
      },
      json: true
    }
    this.jobs = {}
  }
  
  startNextIngestJob ({ingestName, ingestType}) {
    const self = this;
    
    self.emit('msg', `==============${new Date()}============\nStarting ${ingestType} ingestions from folder ${ingestName}`);

    return this._getJobsToDelete(ingestType)
      .then(jobsToDelete => {
        !jobsToDelete.length
        ? self.emit('msg', `No jobs to delete`)
        : self.emit('msg', `Deleting jobs - ${JSON.stringify(jobsToDelete, null, 4)}`);

        return Promise.all(jobsToDelete.map(this.deleteJob.bind(this)));
      })
      .then(() => {
        return runJobs(ingestType,ingestName )
      })
      .then(() => {
        self.emit('finished', `${new Date()} - Completed ${ingestType} ${ingestName}`)
      })
  }

  deleteJob (deleteJobUrl) {
    const getJobName = R.compose(R.last, R.split('/'));
    
    const options = {
      uri: `${baseUrl}${deleteJobUrl}`,
      method: 'DELETE',
      body: {
        kind: 'DeleteOptions',
        name: getJobName(deleteJobUrl),
        propagationPolicy: 'Background',
        in: 3
      }
    };
    
    return this._makeRequest(options);
  }

  _getJobsToDelete (ingestType) {
    const options = {
      uri: `${baseUrl}/apis/batch/v1/namespaces/dacc-entitysearch/jobs`
    };

    const forIngestType = ingestType === 'incremental' ? new RegExp(/-delta-/) : new RegExp(/-bulk-/);

    const filterJobs = R.compose(
      R.gt(R.__, 0),
      R.length,
      R.intersection(['neo4j', 'elastic']),
      R.split('-'),
      R.path(['metadata', 'name'])
    );

    const formatGetJobResults = R.compose(
      R.filter(R.test(forIngestType)),
      R.map(R.path(['metadata', 'selfLink'])),
      R.filter(filterJobs),
      R.prop('items')
    )
  
    return this._makeRequest(options).then(formatGetJobResults);
  }

  _makeRequest (options) {
    return request(Object.assign({}, this.baseOptions, options));
  }
}

module.exports = KubeAPIClient;