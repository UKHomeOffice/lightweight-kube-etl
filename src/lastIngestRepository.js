"use strict";

const Promise = require("bluebird");
const MongoClient = Promise.promisifyAll(require('mongodb')).MongoClient;

const { MONGO_CONN } = process.env;//,
    // connectionUrl = `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${DB_NAME}`;
console.log(MONGO_CONN);
let mongoConnection = null;


function insert(doc) {

    return getCollection().then((collection) => {

        return collection.insertOne(doc);
    });
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


function connect() {

    return MongoClient.connect(MONGO_CONN + "entitysearch", { useNewUrlParser: true }).then((client) =>  {

        return client.db("entitysearch");

    });

}

mongodb://entitysearch:pleasechangeme@mongo:27017/

module.exports = { insert };