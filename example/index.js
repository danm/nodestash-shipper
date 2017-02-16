const Shipper = require('../index');
const configurator = require('abacus-pipline-configurator');

const cargo = new Shipper(configurator.getConfig);
cargo.setup((err) => {
    if (err) throw err;
    cargo.sqs.start();
});