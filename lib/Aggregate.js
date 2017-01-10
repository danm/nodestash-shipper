const fs = require('fs');
const readline = require('readline');
const zlib = require('zlib');
const Elastic = require('./Elastic');
const AWS = require('aws-sdk');

module.exports = class Aggregate {
    constructor(c, config, mongo) {
        this.c = c;
        this.config = config;
        this.mongo = mongo;
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
        stream.on('close', () => {
            cb(nolines);
        });

    }

    read(key, nolines, done) {
        let self = this;
        let elastic = new Elastic(self);
        self.c.a('Processing started for ' + key + ' over ' + nolines + ' lines');
        //a barge is full of many containers
        let barge = [];
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

        stream.on('line', (line) => {
            let json = JSON.parse(line);
            line = null;
            if (json.action_name === 'action_name') { return; }
            let date = new Date(json.date);
            let dateString = '';
            dateString += date.getFullYear();
            dateString += '-';
            dateString += date.getMonth() + 1;
            dateString += '-';
            dateString += date.getDate();

            let type;
            let app;
            let obj = {};

            //bbc stuff
            if (json.name) obj.bbc_counter = json.name;
            if (json.cps_asset_id) obj.bbc_cps = json.cps_asset_id;
            if (json.section) obj.bbc_section = json.section;

            if (json.page_type) obj.bbc_page = json.page_type;
            if (json.app_type) obj.bbc_app = json.app_type;
            if (json.app_name) obj.bbc_name = json.app_name;
            if (json.bbc_site) obj.bbc_site = json.bbc_site;
            if (json.app) obj.bbc_app_v = json.app;

            //general
            if (json.date) obj.date = json.date;
            if (json.ns_vid) obj.user = json.ns_vid;
            if (json.ns_jspageurl) { obj.url = json.ns_jspageurl; }

            //referral
            if (json.rp) {
                if (json.rp.type) obj.ref_type = json.rp.type;
                if (json.rp.name) obj.ref_name = json.rp.name;
                if (json.rp.page) obj.ref_page = json.rp.page;
                if (json.rp.host) obj.ref_host = json.rp.host;
                if (json.rp.search) obj.ref_term = json.rp.search;
            }
            if (json.postID) obj.ref_post = json.postID;

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

            //make sure app name is valid
            let n = json.app_name;
            if (n === 'afrique' ||
                n === 'arabi' ||
                n === 'arabic' ||
                n === 'azeri' ||
                n === 'bengali' ||
                n === 'burmese' ||
                n === 'gahuza' ||
                n === 'hausa' ||
                n === 'hindi' ||
                n === 'indonesia' ||
                n === 'kyrgyz' ||
                n === 'mundo' ||
                n === 'nepali' ||
                n === 'news' ||
                n === 'pashto' ||
                n === 'persian' ||
                n === 'portuguese' ||
                n === 'russian' ||
                n === 'sinhala' ||
                n === 'swahili' ||
                n === 'tamil' ||
                n === 'turkce' ||
                n === 'ukchina' ||
                n === 'ukrainian' ||
                n === 'urdu' ||
                n === 'uzbek' ||
                n === 'vietnamese' ||
                n === 'zhongwen') {
                app = n;
            } else {
                app = obj.bbc_counter.substring(0, obj.bbc_counter.indexOf('.'));
            }

            if (json.type === 'view') {
                type = app + '-view';
                if (obj.bbc_cps !== undefined) {
                    self.mongo.build(app, obj);
                }

            } else if (json.ns_st_ci === undefined) {
                type = app + '-hidden';
                if (json.action_name) obj.action_name = json.action_name;
                if (json.action_type) obj.action_type = json.action_type;

            } else {
                type = app + '-video';
                //video
                if (json.ns_st_ci) obj.video_id = json.ns_st_ci;
                if (json.ns_st_cl) obj.video_length = parseInt(json.ns_st_cl);
                if (json.ns_st_ev) obj.video_event = json.ns_st_ev;
                if (json.ns_st_id) obj.video_playid = json.ns_st_id;
                if (json.ns_st_po) obj.video_position = parseInt(json.ns_st_po);
                if (json.ns_st_pt) obj.video_accumulated = parseInt(json.ns_st_pt);
            }

            obj.file = key;

            let meta = JSON.stringify({ "index": { "_index": "comscore-" + dateString, "_type": type } });
            lines++;
            all++;

            //containers reach their maximum line size and then added to the barge.
            //a new conainer is then used for the next one.

            if (lines < self.config.output.elastic.batchSize) {
                container += meta + '\n';
                obj.part = barge.length;
                container += JSON.stringify(obj) + '\n';
            } else if (lines === self.config.output.elastic.batchSize) {
                barge.push(container);
                container = '';
                lines = 0;
            }

            obj = null;
        });

        //this close is triggered before the all the line events are completed (i think)


        stream.on('close', () => {
            //delete queue message
            //ship data to dbs

            self.c.a('Shipping ' + key + ' to Mongo');
            self.mongo.ship(() => {
                self.c.a('Finsihed sending ' + key + ' to Mongo');

                elastic.ship(barge, key, (err) => {
                    if (err) throw err;
                    let end = new Date().getTime();
                    let total = end - start;
                    self.c.a('Finished Elasticsearch ' + key);
                    self.c.a('Stats for ' + key + ': ' + self.config.output.elastic.batchSize + ' ' + all + ' lines in ' + total + ' across ' + barge.length + ' parts');
                    fs.unlink(key);
                    self.mongo.create();
                    container = null;
                    barge = null;
                    //restart polling

                    //clear memory
                    done();
                });
            });
        });

        stream.on('error', (err) => {
            self.c.a('Error reading ' + key, 3);
        });
    }
};