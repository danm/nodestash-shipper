const pg = require('pg');

module.exports = class Postgresql {
    constructor(c, config) {
        this.c = c;
        this.config = config;
        this.pool = new pg.Pool(config);
    }

    connect() {

        //i don't think we need a connection - let pg handle that

        //let self = this;
        //self.pool = new pg.Pool(self.config);

        // pool.connect((err, client, done) => {
        //     if (err) {
        //         self.c.a(JSON.stringify(err), 3, 'Redshift Connect');
        //         cb(new Error('error fetching client from pool'));
        //     } else {
        //         self.client = client;
        //         self.done = done;
        //         cb();
        //     }
        // });

        // pool.on('error', function(err, client) {
        //     cb('idle client error');
        //     console.log('idle client error', err.message, err.stack);
        // });
    }

    query(query, cb) {
        let self = this;
        self.c.a('Running QUERY', 1, 'redshift');
        self.pool.query(query, function(err, result) {
            self.c.a('QUERY Complete', 1, 'redshift');
            console.log(err, result);
            cb(err);
        });
    }
};