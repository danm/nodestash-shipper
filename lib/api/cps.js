const NodeCache = require('node-cache');
const cpsCache = new NodeCache({ stdTTL: 100, checkperiod: 120 });

module.exports = (cpsid, mongo) => {
  return new Promise((resolve, reject) => {
    // const start = new Date();
    cpsCache.get(cpsid, (err, val) => {
      if (err) {
       // console.log(err.message);
      } else if (val !== undefined && val !== null && val !== 'null') {
        // const now = new Date();
        // console.log('cache', cpsid, now.getTime() - start.getTime());
        resolve(JSON.parse(val));
        return;
      }
      
      const cps = mongo.database.collection('cps');
      const cpsidNumber = parseInt(cpsid, 10);
      if (isNaN(cpsidNumber) === true) {
        resolve(null);
        return;
      }

      cps.findOne({_id: parseInt(cpsid, 10)}, { fields: { section_uri: 1, site_name:1, type: 1 }}, (err, res) => { 
        if (err) {
          reject(err);
          return;
        }
        // const now = new Date();
        // console.log('mongo', cpsid, now.getTime() - start.getTime());
        cpsCache.set(cpsid, JSON.stringify(res));
        resolve(res);
      });
    });
  });
};