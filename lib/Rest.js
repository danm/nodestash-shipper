'use strict';

const express = require('express');
const rest = express();

const startServer = (app) => {
    rest.get('/', (req, res) => {
        res.json({ "message": "Welcome To Nodestash-Shipper" });
    });

    rest.get('/start', (req, res) => {
        app.sqs.start();
        res.json({ "message": "Started" });
    });

    rest.get('/stop', (req, res) => {
        app.sqs.stop();
        res.json({ "message": "Stoped" });
    });

    rest.get('/config', (req, res) => {
        res.json({ "message": app.config });
    });

    rest.get('/config/setup', (req, res) => {
        app.setup((err) => {
            if (err) {
                res.json({ "message": 'Failed to setup shipper' });
            } else {
                res.json({ "message": 'Shipper setup' });
            }
        });
    });

    rest.get('/db/mongo/connect', (req, res) => {
        app.mongo.connect((err) => {
            if (err) { res.json({ "message": "Error connecting to Mongo" }); }
            res.json({ "message": "Connected to Mongo" });
        });
    });

    rest.get('/db/mongo/disconnect', (req, res) => {
        app.mongo.disconnect();
        res.json({ "message": "Disconnected from Mongo" });
    });

    rest.get('/config/elastic/batch/:size', (req, res) => {
        let size = parseInt(req.params.size);
        if (isNaN(size) === false) {
            app.config.output.elastic.batchSize = size;
            res.json({ "message": "updated" });
        } else {
            res.json({ "message": "not a number" });
        }
    });

    rest.listen(app.config.rest.port);
};

module.exports = class Rest {
    constructor(app) {
        startServer(app);
    }
};