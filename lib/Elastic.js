'use strict';

const request = require('request');
const fs = require('fs');

module.exports = class Elastic {
    constructor(app) {
        this.elasticurl = app.config.output.elastic.endpoint;
        this.c = app.c;
        this.customs = '';
        this.count = 0;
        this.file = 0;

        this.loop = 0;
    }


    arrange() {
        let self = this;
        if (self.barge.length >= self.loop) {
            self.execute();
        } else {
            self.cb(null);
        }
    }

    execute() {
        let self = this;

        self.c.a('Sending part ' + (self.loop + 1) + ' of ' + self.barge.length + ' for ' + self.key);
        self.config.body = self.barge[self.loop];
        request(self.config, (err, res, body) => {
            if (err) {
                self.c.a('Error sending part ' + (self.loop + 1) + ' of ' + self.barge.length + ' for ' + self.key, 3);
                self.c.a(err, 3);
                self.cb(err);
            } else {
                self.c.a('Part received ' + (self.loop + 1) + ' of ' + self.barge.length + ' for ' + self.key);
                self.loop++;
                self.arrange();
            }
            return;
        });
    }

    ship(barge, key, cb) {
        let self = this;
        self.barge = barge;
        self.key = key;
        self.cb = cb;

        self.config = {
            method: 'POST',
            url: 'https://' + self.elasticurl + '/pond/_bulk'
        };

        let log = self.c;
        self.arrange();
        return;
    }
};