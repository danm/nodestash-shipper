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
        
        const options = {
            uri: 'https://search-abacus-elastic-7pfdoqwfexpm5pyjy5ebrv2tue.eu-west-1.es.amazonaws.com/_bulk',
            method: 'POST',
            headers: {
              'content-type': 'application/x-ndjson',
            },
            body: fs.createReadStream('elastic.tmp'),
          };

        self.c.a('Shipping ES File', 1, key);

        request(options, (err, res, body) => {
            self.c.a('ES File Shipped', 1, key);
            if (err) {
                console.log(err.message);
                console.log(err.stack);
                self.c.a('Error Shipping ES File', 3, key);
            } else {
                self.c.a('ES File Sent', 1, key);
            }
            cb(null);
        });
    }
};