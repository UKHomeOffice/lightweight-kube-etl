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

module.exports = {insert}
