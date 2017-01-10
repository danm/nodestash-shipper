const Shipper = require('../index');
const config = require('./configuration-secure.json');
const cargo = new Shipper(config);
cargo.setup((err) => {
    if (err) throw err;
    cargo.sqs.start();
});