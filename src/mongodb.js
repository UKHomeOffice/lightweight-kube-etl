"use strict"

const Promise = require("bluebird")
const MongoClient = Promise.promisifyAll(require("mongodb")).MongoClient
const dbName = "entitysearch"

const {MONGO_CONN} = process.env
let mongoConnection = null

function insert(doc) {
  return getCollection().then(collection => {
    return collection.insertOne(doc)
  })
}

function connect() {
  return MongoClient.connect(
    MONGO_CONN + dbName,
    {useNewUrlParser: true}
  ).then(client => {
    return client.db(dbName)
  })
}

function getCollection() {

    if (mongoConnection) {

        return Promise.resolve(mongoConnection.collection("es_load_dates"));
    }

    return connect().then((connection) => {

        mongoConnection = connection;

        return mongoConnection.collection('es_load_dates');
    });
}

module.exports = {insert}
