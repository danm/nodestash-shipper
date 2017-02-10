const pg = require('pg');

module.exports = class Postgresql {
    constructor(c, config) {
        this.c = c;
        this.config = config;
    }

    copy(query, cb) {
        let self = this;
        const pool = new pg.Pool(self.config);
        pool.connect((err, client, done) => {
            if (err) {
                cb('error fetching client from pool');
                console.log(err);
                return;
            }

            self.c.a('Running COPY', 1, 'redshift');
            client.query(query, function(err, result) {
                self.c.a('COPY Complete', 1, 'redshift');
                console.log(err, result);
                done();
                cb(err);
            });
        });

        pool.on('error', function(err, client) {
            cb('idle client error');
            console.log('idle client error', err.message, err.stack);
        });
    }
};