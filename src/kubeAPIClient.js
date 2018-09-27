const { runJobs } = require('./ingestionService');
const { deleteJobs } = require('./kubernetesClient');
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

    return deleteJobs(ingestType)    
      .then(deletedJobs => {
        self.emit('msg', `Deleted jobs - ${JSON.stringify(deletedJobs, null, 4)}`);
        self.emit('msg', `Running jobs - ${JSON.stringify([
          `neo4j-${ingestType}-${ingestName}`,
          `elastic-${ingestType}-${ingestName}`
        ], null, 4)}`);
        return runJobs(ingestType, ingestName)
      })
      .then(() => {
        self.emit('msg', `${new Date()} - Completed ${ingestType} ${ingestName}`);
        self.emit('completed', {ingestType, ingestName}); 
      }) 
  }
}

module.exports = KubeAPIClient;