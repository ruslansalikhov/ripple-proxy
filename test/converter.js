const orig = require("./orig-40000057");
const binary = require('ripple-binary-codec');
var moment = require('moment');
var smoment = require('../lib/smoment');

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


let converted = prepareForReply(orig);


console.dir(converted);
//console.log(JSON.stringify(converted.transactions[0], null, " "));
