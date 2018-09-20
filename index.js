"use strict"

const {createConsumer} = require("./src/sqs")
const {messageHandler} = require("./src/etl")

const messageConsumer = createConsumer(messageHandler)

messageConsumer.on("error", err => console.error(err.message))

if (!global.it) messageConsumer.start()
