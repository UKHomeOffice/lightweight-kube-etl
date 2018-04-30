"use strict"

const {create_queue} = require("./src/sqs")
const {sqs_message_handler} = require("./src/etl")

const app = create_queue(sqs_message_handler)

app.on("error", err => console.error(err.message))

if (!global.it) app.start()
