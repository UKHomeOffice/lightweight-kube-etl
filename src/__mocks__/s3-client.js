module.exports = {
    respondsWith: {Contents: []},
    listObjectsV2 (options, callback) {
        process.nextTick(() => {
            callback(null, this.respondsWith);
        })
    }
};