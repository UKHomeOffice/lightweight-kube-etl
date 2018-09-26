const request = require('request-promise');
const { exec } = require('child_process');
const EventEmitter = require('events');
const baseUrl = "http://127.0.0.1:8181";
const R = require('ramda');

function execPromise(commandString) {
  return new Promise((resolve, reject) => {
      exec(commandString, (error, stdout, stderr) => {
          if (error || stderr) {
              return reject(new Error(error || stderr));
          }

          return resolve(stdout);
      });
});

}

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
  }
  
  startNextIngestJob ({ingestName, ingestType}) {
    this.emit('msg', `==============${new Date()}============\nStarting ${ingestType} ingestions from folder ${ingestName}`);

    return this._getJobsToDelete(ingestType)
      .then(jobsToDelete => {
        this.emit('msg', `Deleting jobs - ${JSON.stringify(jobsToDelete, null, 4)}`);

        Promise.all(jobsToDelete.map(this.deleteJob.bind(this)))
      })
      .then(() => {
        const nextIngestJobs = [
          `neo4j-${ingestType}-${ingestName}`,
          `elastic-${ingestType}-${ingestName}`
        ];

        return this.createJob(nextIngestJobs[0]);
      })
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

  createJob (ingestName) {
    const createCmd = `/app/kubectl --token ${this.token} create job ${ingestName} --from=cronjob/neo4j-bulk`;

    execPromise(createCmd)
      .then(() => {
        this.emit('msg', `Created jobs - ${JSON.stringify(ingestName, null, 4)}`);
      })
      .catch(err => {
        console.error(err);
      });
  }



  _makeRequest (options) {
    return request(Object.assign({}, this.baseOptions, options));
  }
}

module.exports = KubeAPIClient;