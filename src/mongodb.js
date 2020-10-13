"use strict";

const Promise = require("bluebird");
const MongoClient = Promise.promisifyAll(require("mongodb")).MongoClient;
const { MONGO_CONN, MONGO_DB_NAME, MONGO_HAS_REPLICAS, MONGO_RS_NAME } = process.env;
const dbName = MONGO_DB_NAME;
let mongoConnection = null;

const connectionString =
  MONGO_HAS_REPLICAS == "true"
    ? `${MONGO_CONN}${MONGO_DB_NAME}?replicaSet=${MONGO_RS_NAME}&readPreference=secondary`
    : `${MONGO_CONN}${MONGO_DB_NAME}?readPreference=secondary`;

function insert(doc) {
  return getCollection().then(collection => {
    return collection.insertOne(doc);
  });
}

function connect() {
  return MongoClient.connect(
    connectionString,
    { useNewUrlParser: true }
  ).then(client => {
    return client.db(dbName);
  });
}

function getCollection() {
  if (mongoConnection) {
    return Promise.resolve(mongoConnection.collection("es_load_dates"));
  }
  return connect().then(connection => {
    mongoConnection = connection;
    return mongoConnection.collection("es_load_dates");
  });
}

module.exports = { insert };
