const request = require('request-promise');
const { exec } = require('child_process');
const EventEmitter = require('events');
const baseUrl = "http://127.0.0.1:8181";
const R = require('ramda');

// function execPromise(commandString) {
//   return new Promise((resolve, reject) => {
//     exec(commandString, (error, stdout, stderr) => {
//       if (error || stderr) {
//         return reject(new Error(error || stderr));
//       }

//       return resolve(stdout);
//     });
//   });
// }

/*
    "status": {
        "active": 1,
        "startTime": "2018-09-26T15:52:15Z"
    }

    "status": {
        "completionTime": "2018-09-26T15:58:00Z",
        "conditions": [
            {
                "lastProbeTime": "2018-09-26T15:58:00Z",
                "lastTransitionTime": "2018-09-26T15:58:00Z",
                "status": "True",
                "type": "Complete"
            }
        ],
        "startTime": "2018-09-26T15:52:15Z",
        "succeeded": 1
    }
*/

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
    this.emit('msg', `==============${new Date()}============\nStarting ${ingestType} ingestions from folder ${ingestName}`);

    return this._getJobsToDelete(ingestType)
      .then(jobsToDelete => {
        !jobsToDelete.length
        ? this.emit('msg', `No jobs to delete`)
        : this.emit('msg', `Deleting jobs - ${JSON.stringify(jobsToDelete, null, 4)}`);

        return Promise.all(jobsToDelete.map(this.deleteJob.bind(this)));
      })
      .then(() => {
        const nextIngestJobs = [
          `neo4j-${ingestType}-${ingestName}`,
          `elastic-${ingestType}-${ingestName}`
        ];

        return Promise.all(nextIngestJobs.map(this.createJob.bind(this)));
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

  createJob (jobName) {
    const cronjob = R.compose(R.join('-'), R.slice(0, 2), R.split('-'))(jobName);
    const createCmd = `knp --token ${this.token} create job ${jobName} --from=cronjob/${cronjob}`;
    console.log(createCmd);
    const self = this;

    return execPromise(createCmd)
      .then(() => {
        self.emit('msg', `Created job - ${JSON.stringify(ingestName, null, 4)}`);

        self.jobs[jobName] = 'running'

        self.emit('msg', `Status - ${JSON.stringify(self.jobs, null, 4)}`);
      })
      .catch(err => {
        self.emit('err', JSON.stringify(err, null, 4));
      });
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