'use strict';

const request = require('request');
const fs = require('fs');

module.exports = class Elastic {
    constructor(app) {
        this.elasticurl = app.config.output.elastic.endpoint;
        this.customs = '';
        this.count = 0;
        this.file = 0;
        this.app = app;
    }

    ship(container, key, nolines, cb) {
        let self = this;
        nolines = nolines / self.app.config.output.elastic.batchSize;
        nolines = Math.round(nolines);
        let config = {
            body: container,
            method: 'POST',
            url: 'https://' + self.elasticurl + '/pond/_bulk'
        };

        self.file++;
        self.app.c.a('Sending part ' + self.file + ' of ' + nolines + ' for ' + key);

        request(config, (err, res, body) => {
            if (err) {
                self.app.c.a('Error sending part ' + self.file, 3);
                self.app.c.a(err, 3);
                cb(err);
            } else {
                self.app.c.a('Part received ' + self.file + ' of ' + nolines + ' for ' + key);
                cb(null);
            }
        });
    }
};