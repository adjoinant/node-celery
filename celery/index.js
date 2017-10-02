const Client = require('./client');

exports.createClient = (config, callback) => new Client(config, callback);
