const Shipper = require('../index');
const configurator = require('./config.js');

const cargo = new Shipper(configurator.getConfig, configurator.getEnvironment);
cargo.setup((err) => {
    if (err) throw err;
    for (let sqs of cargo.sqs) {
        sqs.start();
    }
});