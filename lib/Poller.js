'use strict';

const Consumer = require('sqs-consumer');
const AWS = require('aws-sdk');

const handles = require('./handles');

module.exports = class Poller {
    constructor(cargo) {
        let self = this;
        self.c = cargo.c;
        self.config = cargo.config;

        // does it need credentials 
        if (self.config.input.sqs.accessKeyId !== undefined && self.config.input.sqs.secretAccessKey !== undefined) {
            let creds = new AWS.Credentials(self.config.input.sqs.accessKeyId, self.config.input.sqs.secretAccessKey);
            AWS.config.update({
                region: self.config.input.sqs.region,
                correctClockSkew: true,
                credentials: creds
            });
        } else {
            AWS.config.update({
                region: self.config.input.sqs.region
            });
        }

        self.sqs = Consumer.create({
            queueUrl: self.config.input.sqs.queueUrl,
            batchSize: 1,
            handleMessage: (message, done) => {
                self.c.a('Message recieved from ' + self.config.input.sqs.name + ' queue, processing');
                if (self.config.input.sqs.name === 'puddle-json' ||
                self.config.input.sqs.name === 'dead-puddle-json') {
                    self.mongo = cargo.mongo;
                    handles.mongo(self, message, done);
                } else if (self.config.input.sqs.name === 'puddle-redshift') {
                    self.postgresql = cargo.postgresql;
                    handles.redshift(self, message, done);
                }
            }
        });

        self.sqs.on('error', (err) => {
            self.c.a('Error with SQS queue', 3);
            self.c.a(err, 3);
        });

        self.sqs.on('processing_error', (err) => {
            self.c.a('Error with SQS message', 3);
            self.c.a(err, 3);
        });

        self.sqs.on('message_processed', (err) => {
            self.c.a('Finsihed With Message');
        });
    }

    start() {
        let self = this;

        if (self.aggregate !== undefined) {
            delete self.aggregate;
        }

        self.sqs.start();
        self.c.a('Polling ' + self.config.input.sqs.name + ' queue');
    }

    stop() {
        let self = this;
        self.sqs.stop();
        self.c.a('Pausing Polling SQS');
    }
};