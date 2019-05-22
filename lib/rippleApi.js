const ripple = require('ripple-lib');
const nconf = require('nconf');

const rippleAPI = new ripple.RippleAPI(nconf.get("ripple"));

const Logger = require('./logger');
const log = new Logger({scope : 'rippleAPI'});

rippleAPI.on('error', function(errorCode, errorMessage, data) {
  log.error(errorCode, errorMessage, data)
});

module.exports = rippleAPI;
