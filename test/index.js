const Aggregate = require('../lib/Aggregate');
const MongoClient = require('mongodb').MongoClient;

const c = {
  a: (x) => {
    console.log(x, 1);
  },
};

const config = {
  output: {
    elastic: {
      active: true,
    },
  },
};

MongoClient.connect('mongodb://192.168.0.16:27017/abacus', function(err, db) {
  if (err) cb(err);
  const aggregate = new Aggregate(c, db, config);
  aggregate.lines('./2017-08-29-22-11-news-v2-128-sampled-pageviews-26384908-0.json', (lines) => {
    console.log(lines);
    aggregate.read('./2017-08-29-22-11-news-v2-128-sampled-pageviews-26384908-0.json', lines, () => {
      console.log('xdonex');
    });
  });
});
