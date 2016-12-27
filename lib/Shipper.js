'use strict';

//npm
const Logback = require('logback');
const AWS = require('aws-sdk');
const fs = require('fs');
const readline = require('readline');
const zlib = require('zlib');

//local
const Rest = require('./Rest.js');
const Poller = require('./Poller.js');
const Elastic = require('./Elastic');

let lines = 0;

module.exports = class Shipper {
    constructor(config) {
        let self = this;
        self.config = config;

        //logback setup
        if (self.config.logback) {
            self.c = new Logback('docks', self.config.logback, 'json');
        } else {
            self.c = new Logback('docks');
        }

        this.c.a('Starting Docks');

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
        if (self.config.input.sqs !== undefined && self.config.input.sqs.active === true) {
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

    save(params, done) {
        let self = this;
        let download = () => new AWS.S3().getObject(params).createReadStream();
        let save = fs.createWriteStream(params.Key);
        download().pipe(save);

        save.on('error', (err) => {
            self.c.a('Error Saving ' + params.Key, 3);
        });

        save.on('close', () => {
            self.c.a('Saved ' + params.Key);
            self.lines(params.Key, (nolines) => {
                self.c.a(params.Key + ' has ' + nolines + ' lines to process');
                self.read(params.Key, nolines, done);
            });
        });
    }

    lines(key, cb) {
        let nolines = 0;
        let reader = fs.createReadStream(key);
        let gunzip = zlib.createGunzip();
        let stream = readline.createInterface({
            input: reader.pipe(gunzip),
            terminal: false
        });

        stream.on('line', (line) => nolines++);
        stream.on('close', () => cb(nolines));

    }

    read(key, nolines, done) {
        let self = this;
        let elastic = new Elastic(self);
        self.c.a('Processing ' + key);
        let container = '';
        let lines = 0;
        let reader = fs.createReadStream(key);
        let gunzip = zlib.createGunzip();
        let stream = readline.createInterface({
            input: reader.pipe(gunzip),
            terminal: false
        });

        //elastic setup

        let all = 0;
        let over = 0;
        let start = new Date().getTime();
        let leftover = '';
        let parts = 0;

        stream.on('line', (line) => {
            let json = JSON.parse(line);
            let date = new Date(json.date);
            let dateString = '';
            dateString += date.getFullYear();
            dateString += '-';
            dateString += date.getMonth() + 1;
            dateString += '-';
            dateString += date.getDate();

            let type;
            let obj = {};

            //bbc stuff
            if (json.name) obj.bbc_counter = json.name;
            if (json.cps_asset_id) obj.bbc_cps = json.cps_asset_id;
            if (json.section) obj.bbc_section = json.section;

            if (json.page_type) obj.bbc_page = json.page_type;
            if (json.app_type) obj.bbc_app = json.app_type;
            if (json.app_name) obj.bbc_name = json.app_name;
            if (json.bbc_site) obj.bbc_site = json.bbc_site;

            //general
            if (json.date) obj.date = json.date;
            if (json.ns_vid) obj.user = json.ns_vid;

            //referral
            if (json.rp) {
                if (json.rp.type) obj.ref_type = json.rp.type;
                if (json.rp.name) obj.ref_name = json.rp.name;
                if (json.rp.page) obj.ref_page = json.rp.page;
                if (json.rp.host) obj.ref_host = json.rp.host;
            }

            //device
            if (json.ns_v_platform) obj.device_platform = json.ns_v_platform;
            if (json.ua) {
                if (json.ua.os && json.ua.os.family) obj.device_os = json.ua.os.family;
                if (json.ua.family) obj.device_browser = json.ua.family;
                if (json.ua.device && json.ua.device.family) obj.device_family = json.ua.device.family;
            }

            //location
            if (json.loc) {
                if (json.loc.cont) obj.loc_cont = json.loc.cont;
                if (json.loc.country) obj.loc_country = json.loc.country;
                if (json.loc.city) obj.loc_city = json.loc.city;
                if (json.loc.ll[0] && json.loc.ll[1]) obj.loc_ll = json.loc.ll;
            }

            if (json.type === 'view') {

                type = json.app_name + '-view';

            } else if (json.ns_st_ci === undefined) {

                type = json.app_name + '-hidden';
                if (json.action_name) obj.action_name = json.action_name;
                if (json.action_type) obj.action_type = json.action_type;

            } else {
                type = json.app_name + '-video';
                //video
                if (json.ns_st_ci) obj.video_id = json.ns_st_ci;
                if (json.ns_st_cl) obj.video_length = json.ns_st_cl;
                if (json.ns_st_ev) obj.video_event = json.ns_st_ev;
                if (json.ns_st_id) obj.video_playid = json.ns_st_id;
                if (json.ns_st_po) obj.video_position = json.ns_st_po;
                if (json.ns_st_pt) obj.video_accumulated = json.ns_st_pt;
            }

            let meta = JSON.stringify({ "index": { "_index": "comscore-" + dateString, "_type": type } });
            all++;
            lines++;
            if (lines < self.config.output.elastic.batchSize) {
                container += meta + '\n';
                container += JSON.stringify(obj) + '\n';
            } else if (lines === self.config.output.elastic.batchSize) {
                stream.pause();
                elastic.ship(container, key, nolines, (err) => {
                    //add some error control here
                    parts++;
                    lines = 0;
                    container = leftover;
                    leftover = '';
                    stream.resume();
                });
            } else if (lines > self.config.output.elastic.batchSize) {
                over++;
                leftover += meta + '\n';
                leftover += line + '\n';
            }
        });

        stream.on('close', (line) => {
            done();
            elastic.ship(container, key, nolines, (err) => {
                //add some error control here
                lines = 0;
                parts++;
                container = '';
                let end = new Date().getTime();
                let total = end - start;
                self.c.a('Stats for ' + key + ': ' + self.config.output.elastic.batchSize + ' ' + all + ' lines in ' + total + ' across ' + parts + ' parts');
                self.c.a('Finished Elasticsearch ' + key);
                fs.unlink(key);

                self.sqs.start();
            });
        });

        stream.on('error', (err) => {
            self.c.a('Error reading ' + key, 3);
        });
    }
};