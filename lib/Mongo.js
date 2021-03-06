'use strict';

const MongoClient = require('mongodb').MongoClient;
process.env.TZ = 'Europe/London';
const fs = require('fs');
const readline = require('readline');

const aggregate = function(newObject, oldObject, property) {
    if (newObject[property] === undefined) {
        newObject[property] = {};
        newObject[property][oldObject] = 1;
    } else if (newObject[property][oldObject] === undefined) {
        newObject[property][oldObject] = 1;
    } else {
        newObject[property][oldObject]++;
    }
};

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



const arrange = (inserts, article, database, cps, cb) => {

    if (cps === undefined) {
        cps = database.collection('cps');
    }

    if (article < inserts.length) {
        //no more articles
        execute(inserts, article, database, cps, cb);
    } else {
        cb();
    }
};

const execute = (inserts, article, database, cps, cb) => {

    cps.update({ _id: inserts[article].id, "analytics.date": inserts[article].date }, { $set: { updated: new Date() }, $inc: inserts[article].inc }, { upsert: true }, (err) => {
        if (err) {
            //likely it doesn't exist so we need to create it
            cps.update({ _id: inserts[article].id }, { $set: { updated: new Date() },  $push: { analytics: inserts[article].pushed } }, { upsert: true }, (err) => {
                if (err) throw err;
                // console.log(`${article} / ${inserts.length} pushed`);
                article++;
                arrange(inserts, article, database, cps, cb);
            });
        } else {
            // the increments worked
            // console.log(`${article} / ${inserts.length} incremented`);
            article++;
            arrange(inserts, article, database, cps, cb);
        }

    });
};

module.exports = class Mongo {
    constructor(app) {
        let self = this;
        self.app = app;

        if (self.app.config.output.mongo && self.app.config.output.mongo.database) {
            self.databasename = self.app.config.output.mongo.database;
            self.region = self.app.config.output.mongo.region;
        } else {
            throw new Error('No Mongo config found');
        }
    }

    getServers(cb) {
        let self = this;

        let mongo = self.app.config.output.mongo;

        if (self.app.environment === 'sandbox') {
            self.connection = `mongodb://${mongo.local}:${mongo.port}/${mongo.database}`;
            cb(null);
        } else {
            let filter = { "Filters": [{ "Name": "tag:BBCComponent", "Values": ["mongo-db"] }, { "Name": "instance-state-name", "Values": ["running"] }, { "Name": "tag:BBCEnvironment", "Values": ["live"] }] };
            const AWS = require('aws-sdk');
            const ec2 = new AWS.EC2({ "region": mongo.region });
            ec2.describeInstances(filter, function(err, res) {
                if (err) {
                    cb(err);
                } else if (res.Reservations.length > 0) {
                    self.connection = 'mongodb://';
                    res.Reservations.forEach(function(x, idx) {
                        self.connection += x.Instances[0].PrivateIpAddress;
                        if (idx < res.Reservations.length - 1) { self.connection += ','; }
                    });

                    self.connection += '/' + self.databasename + '?replicaSet=telescope';
                    self.app.c.a("Servers: " + self.connection, 1, 'Mongo');
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


    ship(cb) {
        let self = this;
        self.app.c.a("Agregating Documents for Mongo");

        let reader = fs.createReadStream('mongo.tmp');
        let stream = readline.createInterface({
            input: reader,
            terminal: false
        });

        let articles = {};

        stream.on('line', (line) => {
            line = JSON.parse(line);

            //remove pages we don't care for... index, business
            if (line.bbc_page === "Index") {
                return;
            }

            //remove any documents we feel are not storys - hack until we fix poundlock
            line.bbc_cps = parseInt(line.bbc_cps);
            if (line.bbc_cps < 10000000 || line.bbc_cps > 99999999) {
                return;
            }

            //bucket datetime into 15 min blocks
            line.date = new Date(line.date);
            let mins = line.date.getMinutes();
            if (mins >= 0 && mins < 15) {
                line.date.setMinutes(15, 0, 0);
            } else if (mins >= 15 && mins < 30) {
                line.date.setMinutes(30, 0, 0);
            } else if (mins >= 30 && mins < 45) {
                line.date.setMinutes(45, 0, 0);
            } else {
                line.date.setMinutes(60, 0, 0);
            }

            //create time string
            let time = line.date.toISOString();

            //views
            if (articles[line.bbc_cps] === undefined) {
                articles[line.bbc_cps] = {};
                articles[line.bbc_cps][time] = {};
                articles[line.bbc_cps][time].views = 1;
                articles[line.bbc_cps][time].date = line.date;
            } else if (articles[line.bbc_cps][time] === undefined) {
                articles[line.bbc_cps][time] = {};
                articles[line.bbc_cps][time].views = 1;
                articles[line.bbc_cps][time].date = line.date;
            } else {
                articles[line.bbc_cps][time].views++;
            }

            let doc = articles[line.bbc_cps][time];

            if (line.loc_cont) { aggregate(doc, line.loc_cont, 'continent'); }

            if (line.loc_country) { aggregate(doc, line.loc_country, 'country'); }

            if (line.loc_city !== undefined && line.loc_country === 'GB') { aggregate(doc, line.loc_city, 'city'); }

            if (line.device_browser) { aggregate(doc, line.device_browser, 'browser'); }

            if (line.device_platform) { aggregate(doc, line.device_platform, 'platform'); }

            if (line.device_family) { aggregate(doc, line.device_family, 'device'); }

            if (line.device_os) { aggregate(doc, line.device_os, 'os'); }

            if (line.ref_type) { aggregate(doc, line.ref_type, 'entry'); }

            if (line.bbc_app) { aggregate(doc, line.bbc_app, 'app'); }
            
            if (line.edition) { aggregate(doc, line.edition, 'edition'); }

            switch (line.ref_type) {
                case 'bbc':
                    {
                        if (line.ref_name !== 'homepage') {
                            aggregate(doc, line.ref_page, 'bbc_' + line.ref_name.slice(0, 10).split(".").join("-").split("$").join("-"));
                        }

                        break;
                    }
                case 'search':
                    {
                        aggregate(doc, line.ref_name, 'ref_search');
                        if (line.ref_term) {
                            aggregate(doc, line.ref_term, 'ref_term');
                        }
                        break;
                    }
                case 'social':
                    {
                        aggregate(doc, line.ref_name, 'ref_social');
                        break;
                    }
                case 'news':
                    {
                        aggregate(doc, line.ref_name, 'ref_news');
                        break;
                    }
                case 'email':
                    {
                        aggregate(doc, line.ref_name, 'ref_email');
                        break;
                    }
                default:
                    {
                        if (line.ref_host) {
                            aggregate(doc, line.ref_host, 'ref_unknown');
                        }
                    }
            }
        });

        stream.on('close', () => {

            self.app.c.a("Completed Agregating Documents for Mongo");

            let documents = [];

            for (let id in articles) {
                for (let time in articles[id]) {
                    for (let property in articles[id][time]) {
                        if (property !== 'views' && property !== 'date') {
                            articles[id][time][property] = sortSanitizeAndFilter(articles[id][time][property], 10);
                        }
                    }
                }
            }


            //if the array already exists we want to inc the values,
            //if it doesn't exist, we need to create it by pushing the values to the array.

            let pusher = [];
            for (let id in articles) {
                for (let time in articles[id]) {
                    let incs = {};
                    let pushed;
                    properties:
                        //prepare the increments
                        for (let prop in articles[id][time]) {
                            if (prop === 'date') {
                                continue properties;
                            }
                            //check if it is deep
                            if (typeof articles[id][time][prop] === 'object') {
                                for (let subprop in articles[id][time][prop]) {
                                    incs["analytics.$." + prop + "." + subprop] = articles[id][time][prop][subprop];
                                }
                            } else {
                                incs["analytics.$." + prop] = articles[id][time][prop];
                            }
                        }
                        //prepare the push
                    pusher.push({ id: parseInt(id), date: articles[id][time].date, inc: incs, pushed: articles[id][time] });
                }
            }

            self.app.c.a("Shipping Documents to Mongo");
            arrange(pusher, 0, self.database, undefined, cb);
        });
    }
};