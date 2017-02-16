'use strict';

//npm
const Logback = require('logback');
const AWS = require('aws-sdk');

//local
const Rest = require('./Rest.js');
const Poller = require('./Poller.js');
const Mongo = require('./Mongo');
const Postgresql = require('./Postgresql');

module.exports = class Shipper {
    constructor(config) {
        let self = this;
        self.config = config;

        //logback setup
        if (self.config.logback !== undefined && self.config.logback.active === true) {
            if (self.config.logback) {
                self.c = new Logback(self.config.name + '-' + self.config.environment || "nodestash-shipper", self.config.logback, 'json', self.config.logback.debug || false);
            } else {
                self.c = new Logback("nodestash-shipper");
            }
        } else {
            this.c = {};
            this.c.a = () => {
                //do nothing
            };
        }
        this.c.a(`Starting ${self.config.name}-${self.config.environment} 1.0`);

        //s3 setup
        if (self.config.s3.accessKeyId !== undefined && self.config.s3.secretAccessKey !== undefined) {
            let creds = new AWS.Credentials(self.config.s3.accessKeyId, self.config.s3.secretAccessKey);
            AWS.config.update({
                region: self.config.s3.region,
                correctClockSkew: true,
                credentials: creds
            });
        } else {
            AWS.config.update({
                region: self.config.s3.region
            });
        }

        //rest setup
        if (self.config.rest !== undefined && self.config.rest.active === true) {
            self.rest = new Rest(self);
        }

        //mongo setup
        if (self.config.output.mongo !== undefined && self.config.output.mongo.active === true) {
            self.mongo = new Mongo(self);
        }

        //potgres setup
        if (self.config.output.postgresql !== undefined && self.config.output.postgresql.active === true) {
            self.postgresql = new Postgresql(self.c, self.config.output.postgresql);
        }

        //sqs setup
        if (self.config.input.sqs !== undefined) {
            let sqs = self.config.input.sqs;
            //check for data
            if (sqs.region === undefined || sqs.queueUrl === undefined) {
                self.c.a('SQS Region or Queuename not provided', 3);
                throw new Error('SQS Region or Queuename not provided');
            } else {
                self.sqs = new Poller(self);
            }
        }
    }

    setup(cb) {
        let self = this;

        if (self.config.output.mongo !== undefined && self.config.output.mongo.active === true) {
            self.mongo.getServers((err, res) => {
                if (err) { cb(err); return; }
                self.mongo.connect((err, res) => {
                    if (err) { cb(err); return; }
                    self.mongo.create();
                    cb(null);
                });
            });
        } else {
            cb();
        }
    }
};