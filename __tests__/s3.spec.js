jest.mock('../src/s3-client');

const s3 = require('../src/s3-client');

describe('The S3 client', () => {
    it('waits for data to be uploaded', done => {
        s3.listObjectsV2({Bucket: 'test/bucket', Prefix: "pending/", Delimiter: ""}, (err, folders) => {
            expect(folders.Contents.length).toBe(0);
            done();
        })
    })

    it('returns folder Contents', done => {
        s3.respondsWith = {Contents: [{}]};

        s3.listObjectsV2({Bucket: 'test/bucket', Prefix: "pending/", Delimiter: ""}, (err, folders) => {
            expect(folders.Contents.length).toBe(1);
            done();
        })
    })
})