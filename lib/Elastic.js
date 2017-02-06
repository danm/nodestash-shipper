'use strict';

const request = require('request');
const fs = require('fs');
const zlib = require('zlib');
const AWS = require('aws-sdk');


module.exports = class Elastic {
    constructor(app) {
        let self = this;
        self.c = app.c;

        self.s3 = app.config.output.s3;
        if (self.s3.accessKeyId !== undefined && self.s3.secretAccessKey !== undefined) {
            let creds = new AWS.Credentials(self.s3.accessKeyId, self.s3.secretAccessKey);
            AWS.config.update({
                region: self.s3.region,
                correctClockSkew: true,
                credentials: creds
            });
        } else {
            AWS.config.update({
                region: self.s3.region
            });
        }
    }

    shipES(key, cb) {
        let self = this;
        let reader = fs.createReadStream('elastic.tmp');

        let options = {
            method: 'POST',
            baseUrl: 'https://search-puddle-u5gt2q4p7nojmrsee7ldndd72y.eu-west-1.es.amazonaws.com',
            uri: '/_bulk',
            body: reader
        };

        self.c.a('Shipping ES File', 1, key);

        request(options, (err, res, body) => {
            self.c.a('ES File Shipped', 1, key);
            if (err) {
                self.c.a('Error Shipping ES File', 3, key);
            } else {
                self.c.a('ES File Sent', 1, key);
            }
            cb(null);
        });
    }
};