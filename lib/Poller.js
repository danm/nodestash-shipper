'use strict';

const Consumer = require('sqs-consumer');
const AWS = require('aws-sdk');
const Aggregate = require('./Aggregate');
const Postgresql = require('./Postgresql');

const handlePuddleJSON = (self, message, done) => {
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

        self.aggregate = new Aggregate(self.c, self.config.output.mongo);
        self.aggregate.save(readParams, (err) => {
            done(err);
        });

    } else {
        done();
    }
};

const handlePuddleRedshift = (self, message, done) => {
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
        let query;

        if (self.env === 'sandbox') {
            query = `COPY dax from 's3://${readParams.Bucket}/${readParams.Key}'
credentials 'aws_access_key_id=${self.sqsConfig.accessKeyId};aws_secret_access_key=${self.sqsConfig.secretAccessKey}'
gzip delimiter ',' removequotes escape TRUNCATECOLUMNS TRIMBLANKS;`;
        } else {
            query = `COPY dax from 's3://${readParams.Bucket}/${readParams.Key}'
iam_role '${self.config.output.postgresql.iam}'
gzip delimiter ',' removequotes escape TRUNCATECOLUMNS TRIMBLANKS;`;
        }

        self.postgresql = new Postgresql(self.c, self.config.output.postgresql);
        self.postgresql.copy(query, (err) => {
            if (err) {
                self.c.a('Error Copying ' + key + ' to redshift', 3, 'Poller');
                self.c.a(JSON.stringify(err), 3, 'Poller-Redshift');
            } else {
                self.c.a('Finsihed Copying ' + key + ' to redshift', 1, 'Poller');
            }
            done();
        });

    } else {
        done();
    }
};

module.exports = class Poller {
    constructor(c, config, sqs, env) {
        let self = this;
        self.c = c;
        self.config = config;
        self.sqsConfig = sqs;
        self.env = env;

        // does it need credentials 
        if (sqs.accessKeyId !== undefined && sqs.secretAccessKey !== undefined) {
            let creds = new AWS.Credentials(sqs.accessKeyId, sqs.secretAccessKey);
            AWS.config.update({
                region: self.sqsConfig.region,
                correctClockSkew: true,
                credentials: creds
            });
        } else {
            AWS.config.update({
                region: self.sqsConfig.region
            });
        }

        self.sqs = Consumer.create({
            queueUrl: self.sqsConfig.queueUrl,
            batchSize: 1,
            handleMessage: (message, done) => {
                c.a('Message recieved from ' + self.sqsConfig.name + ' queue, processing');
                if (self.sqsConfig.name === 'puddle-json') {
                    handlePuddleJSON(self, message, done);
                } else if (self.sqsConfig.name === 'puddle-redshift') {
                    handlePuddleRedshift(self, message, done);
                }
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
        self.c.a('Continue recieved from ' + self.sqsConfig.name + ' queue, processing');
    }

    stop() {
        let self = this;
        self.sqs.stop();
        self.c.a('Pausing Polling SQS');
    }
};