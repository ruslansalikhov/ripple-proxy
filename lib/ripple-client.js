/* eslint no-unused-vars: ["error", { "args": "after-used" }] */
'use strict';

const rippleAPI = require('../lib/rippleApi');
const Parser = require('../lib/ledgerParser');
const moment = require('moment');
const binary = require('ripple-binary-codec');

const Logger = require('./logger');
const log = new Logger({scope : 'ripple-client'});

const RippleClient = function () {
    const self = this;

    rippleAPI.connect().then(function () {
        log.info("Connected to ripple proxy");
    });

    self.getLedgerByIndex = function (index, callback) {

        let options = {
            ledgerVersion: index
        };

        getLedger(options, function (err, ledger) {
            if (ledger) {
                handleLedger(ledger, callback);
            } else {
                console.error(err);
                callback(null, err);
            }
        });
    };

    self.getLedgerLast = function (callback) {

        getLedger({}, function (err, ledger) {
            if (ledger) {
                handleLedger(ledger, callback);
            } else {
                console.error(err);
                callback(null, err);
            }
        });
    };

    function prepareForReply(ledger) {
        let converted = Object.assign({}, ledger.ledger);

        converted.transactions = [];

        ledger.ledger.transactions.forEach((txhash) => {
            let tx = {};
            let origTx = ledger.transactions.find((tx, index, arr) => {
                return tx.hash === txhash;
            });
            tx.hash = txhash;
            tx.ledger_index = Number(origTx.ledger_index);
            tx.date = moment.unix(origTx.executed_time).utc();
            tx.tx = binary.decode(origTx.raw);
            tx.meta = binary.decode(origTx.meta);

            converted.transactions.push(tx);
        });
        return converted;
    }

    function handleLedger(ledger, callback) {
        let parsed = Parser.parseLedger(ledger);
        callback({
            result: "success",
            ledger: prepareForReply(parsed),
            extra: {
                affectedAccounts: parsed.affectedAccounts,
                accountsCreated: parsed.accountsCreated,
                exchanges: parsed.exchanges,
                offers: parsed.offers,
                balanceChanges: parsed.balanceChanges,
                payments: parsed.payments,
                escrows: parsed.escrows,
                paychan: parsed.paychan,
                memos: parsed.memos,
                feeSummary: parsed.feeSummary,
            }
        }, null);

    }

    function getLedger(options, callback) {

        if (!options) options = {};

        options.includeAllData = true;
        options.includeTransactions = true;

        if (rippleAPI.isConnected()) {
            requestLedger(options, callback);
        } else {
            rippleAPI.connect().then(function () {
                requestLedger(options, callback);
            });
        }
    }

    function requestLedger(options, callback) {

        log.debug('requesting ledger:', options.ledgerVersion);

        rippleAPI.getLedger(options)
            .then(processLedger)
            .catch(function (e) {
                log.error("error requesting ledger:", options.ledgerVersion, e);
                callback(e, null)
            });

        function processLedger(ledger) {
            let hash;

            // check hash but dont require
            try {
                hash = rippleAPI.computeLedgerHash(ledger);

            } catch (err) {
                log.error("Error calculating ledger hash: ", ledger.ledgerVersion, err);
                log.error(ledger.ledgerVersion, err.toString());
                callback('unable to validate ledger: ' + ledger.ledgerVersion, null);
                return;
            }

            // check but dont require
            if (hash !== ledger.ledgerHash) {
                log.error('hash does not match:',
                    hash,
                    ledger.ledgerHash,
                    ledger.ledgerVersion);
                callback('unable to validate ledger: ' + ledger.ledgerVersion, null);
                return;
            }

            log.info('Got ledger: ' + ledger.ledgerVersion);
            callback(null, convertLedger(ledger));
        }
    }


    function convertLedger(ledger) {
        let converted = {
            account_hash: ledger.stateHash,
            close_time: moment.utc(ledger.closeTime).unix(),
            close_time_human: moment.utc(ledger.closeTime).format('YYYY-MMM-DD hh:mm:ss'),
            close_time_resolution: ledger.closeTimeResolution,
            close_flags: ledger.closeFlags,
            hash: ledger.ledgerHash,
            ledger_hash: ledger.ledgerHash,
            ledger_index: ledger.ledgerVersion.toString(),
            seqNum: ledger.ledgerVersion.toString(),
            parent_hash: ledger.parentLedgerHash,
            parent_close_time: moment.utc(ledger.parentCloseTime).unix(),
            total_coins: ledger.totalDrops,
            totalCoins: ledger.totalDrops,
            transaction_hash: ledger.transactionHash,
            transactions: []
        };

        if (ledger.rawTransactions) {
            converted.transactions = JSON.parse(ledger.rawTransactions);
        }

        return converted;
    }

    return this;
};

module.exports = new RippleClient();
