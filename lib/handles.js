const Aggregate = require('./Aggregate');

exports.mongo = (self, message, done) => {
    //parse the body of the messager
    const json = JSON.parse(message.Body);

    //check the parsed object for the data we need
    if (json.Records &&
        json.Records[0] &&
        json.Records[0].s3 &&
        json.Records[0].s3.bucket &&
        json.Records[0].s3.bucket.name &&
        json.Records[0].s3.object &&
        json.Records[0].s3.object.key) {

        //s3 read
        let bucket = json.Records[0].s3.bucket.name;
        let key = json.Records[0].s3.object.key;
        let readParams = { Bucket: bucket, Key: key };

        self.aggregate = new Aggregate(self.c, self.mongo, self.config);
        self.aggregate.save(readParams, (err) => {
            done(err);
        });

    } else {
        done();
    }
};

exports.redshift = (self, message, done) => {
    //parse the body of the messager
    const json = JSON.parse(message.Body);

    //check the parsed object for the data we need
    if (json.Records &&
        json.Records[0] &&
        json.Records[0].s3 &&
        json.Records[0].s3.bucket &&
        json.Records[0].s3.bucket.name &&
        json.Records[0].s3.object &&
        json.Records[0].s3.object.key) {

        //s3 read
        let bucket = json.Records[0].s3.bucket.name;
        let key = json.Records[0].s3.object.key;
        let readParams = { Bucket: bucket, Key: key };
        let query;

        if (self.env === 'sandbox') {
            query = `COPY dax from 's3://${readParams.Bucket}/${readParams.Key}'
credentials 'aws_access_key_id=${self.sqsConfig.accessKeyId};aws_secret_access_key=${self.sqsConfig.secretAccessKey}'
gzip delimiter ',' removequotes escape TRUNCATECOLUMNS TRIMBLANKS NULL AS 'NULL';`;
        } else {
            query = `COPY dax from 's3://${readParams.Bucket}/${readParams.Key}'
iam_role '${self.config.output.postgresql.iam}'
gzip delimiter ',' removequotes escape TRUNCATECOLUMNS TRIMBLANKS NULL AS 'NULL';`;
        }

        console.log(query);

        self.postgresql.query(query, (err) => {
            if (err) {
                self.c.a('Error Copying ' + key + ' to redshift', 3, 'Poller');
                self.c.a(JSON.stringify(err), 3, 'Poller-Redshift');
            } else {
                self.c.a('Finsihed Copying ' + key + ' to redshift', 1, 'Poller');
            }
            done();
        });

    } else {
        done();
    }
};