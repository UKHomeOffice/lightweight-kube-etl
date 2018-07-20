"use strict"

const {create_consumer} = require("./src/sqs");
const {sqsMessageHandler} = require("./src/etl");

const app = create_consumer(sqsMessageHandler);

app.on("error", err => console.error(err.message));

if (!global.it) app.start();