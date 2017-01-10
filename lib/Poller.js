'use strict';

const Consumer = require('sqs-consumer');
const AWS = require('aws-sdk');
const Aggregate = require('./Aggregate.js');

const handleMessage = (self, message, done) => {
    //parse the body of the messager
    const json = JSON.parse(message.Body);

    //check the parsed object for the data we need
    if (json.Records &&
        json.Records[0] &&
        json.Records[0].s3 &&
        json.Records[0].s3.bucket &&
        json.Records[0].s3.bucket.name &&
        json.Records[0].s3.object &&
        json.Records[0].s3.object.key) {

        //s3 read
        let bucket = json.Records[0].s3.bucket.name;
        let key = json.Records[0].s3.object.key;
        let readParams = { Bucket: bucket, Key: key };

        self.app.c.a('Downloading ' + key);
        self.aggregate = new Aggregate(self.app.c, self.app.config, self.app.mongo);
        self.aggregate.save(readParams, (err) => {
            done();
        });

    } else {
        done();
    }
};

module.exports = class Poller {
    constructor(app) {
        let self = this;
        self.app = app;
        let c = self.app.c;

        // does it need credentials 
        let sqs = app.config.input.sqs;
        if (sqs.accessKeyId !== undefined && sqs.secretAccessKey !== undefined) {
            let creds = new AWS.Credentials(sqs.accessKeyId, sqs.secretAccessKey);
            AWS.config.update({
                region: sqs.region,
                correctClockSkew: true,
                credentials: creds
            });
        } else {
            AWS.config.update({
                region: sqs.region
            });
        }

        self.sqs = Consumer.create({
            queueUrl: sqs.queueUrl,
            batchSize: 1,
            handleMessage: (message, done) => {
                c.a('Message recieved, processing');
                handleMessage(self, message, done);
            }
        });

        self.sqs.on('error', (err) => {
            c.a('Error with SQS queue', 3);
            c.a(err, 3);
        });

        self.sqs.on('processing_error', (err) => {
            c.a('Error with SQS message', 3);
            c.a(err, 3);
        });

        self.sqs.on('message_processed', (err) => {
            c.a('Finsihed With Message');
        });
    }

    start() {
        let self = this;

        if (self.aggregate !== undefined) {
            delete self.aggregate;
        }

        self.sqs.start();
        self.app.c.a('Continue Polling SQS');
    }

    stop() {
        let self = this;
        self.sqs.stop();
        self.app.c.a('Pausing Polling SQS');
    }
};