'use strict';

//npm
const Logback = require('logback');
const AWS = require('aws-sdk');

//local
const Rest = require('./Rest.js');
const Poller = require('./Poller.js');
const Mongo = require('./Mongo');

module.exports = class Shipper {
    constructor(config, env) {
        let self = this;
        self.config = config;

        //logback setup
        if (self.config.logback) {
            self.c = new Logback('docks', self.config.logback, 'json', self.config.logback.debug || false);
        } else {
            self.c = new Logback('docks');
        }

        this.c.a('Starting Docks 1.0');

        //rest setup
        if (self.config.rest !== undefined && self.config.rest.active === true) {
            self.rest = new Rest(self);
        }

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

        //sqs setup
        self.sqs = [];
        if (self.config.input.sqs !== undefined) {
            if (Array.isArray(self.config.input.sqs) === true) {
                for (let i = 0; i < self.config.input.sqs.length; i++) {
                    let sqs = self.config.input.sqs[i];
                    if (sqs.region === undefined || sqs.queueUrl === undefined) {
                        self.c.a('SQS Region or Queuename not provided', 3);
                        throw new Error('SQS Region or Queuename not provided');
                    } else {
                        self.sqs.push(new Poller(self.c, self.config, sqs, env));
                    }
                }
            } else {
                let sqs = self.config.input.sqs;
                //check for data
                if (sqs.region === undefined || sqs.queueUrl === undefined) {
                    self.c.a('SQS Region or Queuename not provided', 3);
                    throw new Error('SQS Region or Queuename not provided');
                } else {
                    self.sqs.push(new Poller(self.c, self.config, sqs, env));
                }
            }
        }

        //mongo setup
        self.mongo = new Mongo(self);
    }

    setup(cb) {
        let self = this;
        self.mongo.getEnviroment(() => {
            self.mongo.getServers((err, res) => {
                if (err) { cb(err); return; }
                self.mongo.connect((err, res) => {
                    if (err) { cb(err); return; }
                    self.mongo.create();
                    cb(null);
                });
            });
        });
    }


};