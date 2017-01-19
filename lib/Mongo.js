'use strict';

const MongoClient = require('mongodb').MongoClient;
process.env.TZ = 'Europe/London';

const sortSanitizeAndFilter = (obj, limit) => {
    let arr = [];
    for (let row in obj) {
        arr.push({ k: row, v: obj[row] });
    }
    arr.sort((a, b) => { return b.v - a.v; });
    arr = arr.slice(0, limit);
    let nobj = {};
    for (let idx of arr) {
        let clean = idx.k;
        clean = clean.split(".").join("-");
        clean = clean.split("$").join("-");
        if (idx.v >= 0) {
            nobj[clean] = idx.v;
        }
    }
    return nobj;
};

const aggregate = function(elm, obj, row) {
    if (obj[row] === undefined) {
        obj[row] = {};
        obj[row][elm] = 1;
    } else if (obj[row][elm] === undefined) {
        obj[row][elm] = 1;
    } else {
        obj[row][elm]++;
    }
};

const arrange = (inserts, site, article, database, cb, cps) => {

    if (cps === undefined) {
        cps = database.collection('cps');
    }

    if (inserts[site].data[article] === undefined) {
        //no more articles
        if (inserts[site + 1] === undefined) {
            //no more sites
            inserts = null;
            article = null;
            cb();
        } else {
            cps = database.collection('cps');
            arrange(inserts, site + 1, 0, database, cb, cps);
        }
    } else {
        execute(inserts, site, article, database, cb, cps);
    }
};

const execute = (inserts, site, article, database, cb, cps) => {
    cps.update(inserts[site].data[article][0], inserts[site].data[article][1], inserts[site].data[article][2], (err) => {
        if (err) { console.log(err); }
        article++;
        arrange(inserts, site, article, database, cb, cps);
    });
};

module.exports = class Elastic {
    constructor(app) {
        let self = this;
        self.app = app;
        if (self.app.config.mongo && self.app.config.mongo.database) {
            self.databasename = self.app.config.mongo.database;
            self.region = self.app.config.mongo.region;
        } else {
            throw new Error('No Mongo config found');
        }
    }

    getEnviroment(cb) {
        let config;
        let self = this;

        try {
            config = require('/config/config.json');
            self.environment = config.environment;
        } catch (e) {
            self.environment = 'sandbox';
        }

        cb();
    }

    getServers(cb) {
        let self = this;

        if (self.environment === 'sandbox') {
            self.connection = 'mongodb://127.0.0.1:27017/' + self.databasename;
            cb(null);
        } else {
            let filter = { "Filters": [{ "Name": "tag:BBCComponent", "Values": ["mongo-db"] }, { "Name": "instance-state-name", "Values": ["running"] }, { "Name": "tag:BBCEnvironment", "Values": [self.environment] }] };
            const AWS = require('aws-sdk');
            const ec2 = new AWS.EC2({ "region": self.region });
            ec2.describeInstances(filter, function(err, res) {
                if (err) {
                    cb(err);
                } else if (res.Reservations.length > 0) {
                    self.connection = 'mongodb://';
                    res.Reservations.forEach(function(x, idx) {
                        self.connection += x.Instances[0].PrivateIpAddress + '/' + self.databasename;
                        if (idx < res.Reservations.length - 1) { self.connection += ','; }
                    });
                    cb(null);
                } else {
                    cb(new Error("no mongo instances found"));
                }
            });
        }
    }

    connect(cb) {
        //enviroment modules here to specify which mongo to connect to 
        let self = this;
        MongoClient.connect(self.connection, function(err, db) {
            if (err) cb(err);
            self.app.c.a("Connected to MongoDB");
            self.database = db;
            cb(null);
        });
    }

    disconnect() {
        this.database.close();
        this.app.c.a("Disconneted from MongoDB");
    }

    create() {
        let self = this;
        self.file = {};
    }

    build(app, obj) {
        //time
        let self = this;
        let file = self.file;
        let time = new Date(obj.date);
        let mins = time.getMinutes();
        if (mins >= 0 && mins < 15) {
            time.setMinutes(0, 0, 0);
        } else if (mins >= 15 && mins < 30) {
            time.setMinutes(15, 0, 0);
        } else if (mins >= 30 && mins < 45) {
            time.setMinutes(30, 0, 0);
        } else {
            time.setMinutes(45, 0, 0);
        }
        let timeid = time.getFullYear() + '-' + (time.getMonth() + 1) + '-' + time.getDate() + '-' + time.getHours() + '-' + time.getMinutes();

        //cps id
        let id = parseInt(obj.bbc_cps);

        //site, cps and time
        {
            if (file[app] === undefined) {
                file[app] = {};
                file[app][id] = {};
                file[app][id][timeid] = {};
                file[app][id][timeid].views = 1;
                file[app][id][timeid].date = time;
            } else if (file[app][id] === undefined) {
                file[app][id] = {};
                file[app][id][timeid] = {};
                file[app][id][timeid].views = 1;
                file[app][id][timeid].date = time;
            } else if (file[app][id][timeid] === undefined) {
                file[app][id][timeid] = {};
                file[app][id][timeid].views = 1;
                file[app][id][timeid].date = time;
            } else {
                file[app][id][timeid].views++;
            }
        }

        //location
        //continent
        if (obj.loc_cont) { aggregate(obj.loc_cont, file[app][id][timeid], 'continent'); }
        //country
        if (obj.loc_country) { aggregate(obj.loc_country, file[app][id][timeid], 'country'); }
        //city
        if (obj.loc_city !== undefined && obj.loc_country === 'GB') { aggregate(obj.loc_city, file[app][id][timeid], 'city'); }

        //device                
        //browser
        if (obj.device_browser) { aggregate(obj.device_browser, file[app][id][timeid], 'browser'); }
        //platform
        if (obj.device_platform) { aggregate(obj.device_platform, file[app][id][timeid], 'platform'); }
        //platform
        if (obj.device_family) { aggregate(obj.device_family, file[app][id][timeid], 'device'); }
        //operating system
        if (obj.device_os) { aggregate(obj.device_os, file[app][id][timeid], 'os'); }

        //refferreals
        //entry
        if (obj.ref_type) { aggregate(obj.ref_type, file[app][id][timeid], 'entry'); }

        switch (obj.ref_type) {
            case 'bbc':
                {
                    if (obj.ref_name !== 'homepage') {
                        aggregate(obj.ref_page, file[app][id][timeid], 'bbc_' + obj.ref_name);
                    }

                    break;
                }
            case 'search':
                {
                    aggregate(obj.ref_name, file[app][id][timeid], 'ref_search');
                    if (obj.ref_term) {
                        aggregate(obj.ref_term, file[app][id][timeid], 'ref_term');
                    }
                    break;
                }
            case 'social':
                {
                    aggregate(obj.ref_name, file[app][id][timeid], 'ref_social');
                    break;
                }
            case 'news':
                {
                    aggregate(obj.ref_name, file[app][id][timeid], 'ref_news');
                    break;
                }
            case 'email':
                {
                    aggregate(obj.ref_name, file[app][id][timeid], 'ref_email');
                    break;
                }
            default:
                {
                    if (obj.ref_host) {
                        aggregate(obj.ref_host, file[app][id][timeid], 'ref_unknown');
                    }
                }
        }

    }

    ship(cb) {

        let self = this;
        self.app.c.a("Shipping to Mongo");
        let file = self.file;
        for (let app in file) {
            for (let id in file[app]) {
                for (let time in file[app][id]) {
                    for (let property in file[app][id][time]) {
                        if (property !== 'views' && property !== 'date') {
                            file[app][id][time][property] = sortSanitizeAndFilter(file[app][id][time][property], 10);
                        }
                    }
                }
            }
        }

        let inserts = [];
        let loop = 0;
        for (let app in file) {
            inserts[loop] = { name: app, data: [] };
            for (let id in file[app]) {
                let incs = {};
                let sets = {};
                for (let time in file[app][id]) {
                    for (let obj in file[app][id][time]) {
                        if (obj === 'views') {
                            incs['analytics.' + time + '.' + obj] = file[app][id][time][obj];
                        } else if (obj === 'date') {
                            sets['analytics.' + time + '.date'] = new Date(file[app][id][time][obj]);
                        } else {
                            for (let param in file[app][id][time][obj]) {
                                incs['analytics.' + time + '.' + obj + '.' + param] = file[app][id][time][obj][param];
                            }
                        }
                    }
                }

                inserts[loop].data.push([{ _id: parseInt(id) }, { $inc: incs, $set: sets }, { upsert: true }]);
            }
            loop++;
        }

        arrange(inserts, 0, 0, self.database, cb);
    }
};