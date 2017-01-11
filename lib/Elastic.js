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

    ships3(key, cb) {
        let self = this;
        let reader = fs.createReadStream('output.tmp');
        let zipper = zlib.createGzip();

        self.c.a('Shipping ES File', 1, key);

        let writeParams = { Bucket: self.s3.bucket, Key: key, Body: reader.pipe(zipper) };
        var s3Up = new AWS.S3().upload(writeParams, function(err, res) {
            if (err) { cb(err); }
            self.c.a('ES File Shipped', 1, key);
            cb(null);
        });
    }
};