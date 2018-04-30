"use strict"

const {create_queue} = require("./sqs")
const {sqs_message_handler} = require("./etl")

const app = create_queue(sqs_message_handler)

app.on("error", err => console.error(err.message))

if (!global.it) app.start()
