'use strict';

const request = require('request');
const fs = require('fs');

const arrange = (barge, key, config, log, loop, cb) => {
    if (barge.length >= loop) {
        execute(barge, key, config, log, loop, cb);
    } else {
        cb(null);
    }
};

const execute = (barge, key, config, log, loop, cb) => {

    log.a('Sending part ' + (loop + 1) + ' of ' + barge.length + ' for ' + key);
    config.body = barge[loop];
    request(config, (err, res, body) => {
        if (err) {
            log.a('Error sending part ' + (loop + 1) + ' of ' + barge.length + ' for ' + key, 3);
            log.a(err, 3);
            cb(err);
        } else {
            log.a('Part received ' + (loop + 1) + ' of ' + barge.length + ' for ' + key);
            loop++;
            arrange(barge, key, config, log, loop, cb);
        }
    });
};

module.exports = class Elastic {
    constructor(app) {
        this.elasticurl = app.config.output.elastic.endpoint;
        this.customs = '';
        this.count = 0;
        this.file = 0;
        this.app = app;
    }

    ship(barge, key, cb) {
        let self = this;
        let config = {
            method: 'POST',
            url: 'https://' + self.elasticurl + '/pond/_bulk'
        };

        let log = self.app.c;
        arrange(barge, key, config, log, 0, cb);
    }
};