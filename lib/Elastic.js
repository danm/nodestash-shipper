'use strict';

const request = require('request');
const fs = require('fs');
const zlib = require('zlib');
const AWS = require('aws-sdk');


module.exports = class Elastic {
    constructor(app) {
        let self = this;
        self.c = app.c;
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