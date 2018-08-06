"use strict"

const {createConsumer} = require("./src/sqs")
const {sqsMessageHandler} = require("./src/etl")

const app = createConsumer(sqsMessageHandler)

app.on("error", err => console.error(err.message))

if (!global.it) app.start()
