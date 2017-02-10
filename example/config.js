let cosmosConfig;
let environment;

try {
    cosmosConfig = require('/config/config.json');
    environment = cosmosConfig.environment;
} catch (e) {
    environment = 'sandbox';
}

let appConfig;

if ('sandbox' === environment) {
    try {
        appConfig = require('./configuration-secure.json');
    } catch (e) {
        throw new Error('Cannot load configuration file');
    }
} else {
    try {
        appConfig = require('./configuration.json');
    } catch (e) {
        throw new Error('Cannot load configuration file');
    }
}

module.exports = {
    getConfig: appConfig,
    getEnvironment: environment
};