/* eslint no-unused-vars: ["error", { "args": "after-used" }] */
"use strict";

const config = require('../config');
const express = require("express");
const bodyParser = require("body-parser");
const compression = require("compression");
const cors = require("cors");
const json2csv = require("nice-json2csv");
const rippleClient = require("../lib/ripple-client");

const Logger = require('../lib/logger');
const log = new Logger({scope : 'ripple-client'});

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));
app.use(json2csv.expressDecorator);
app.use(cors());
app.use(compression());

function errorResponse(res, err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === "4") {
        res.status(err.code).json({
            result: "error",
            message: err.error
        });
    } else {
        res.status(500).json({
            result: "error",
            message: "unable to retrieve ledger"
        });
    }
}

app.get("/ledger/:index?", function (req, res, next) {

    let index = req.params.index;

    if (index) {
        rippleClient.getLedgerByIndex(Number(index), (ledger, error) => {
            if (error) {
                errorResponse(res, error);
            } else {
                res.send(ledger);
            }
        });
    } else {
        rippleClient.getLedgerLast((ledger, error) => {
            if (error) {
                errorResponse(res, error);
            } else {
                res.send(ledger);
            }
        });
    }


});

const host = config.get("proxy:host");
const port = config.get("proxy:port");

// start the server
const server = app.listen(port, host);

server.on('listening', function() {
    log.info('Ripple Proxy API running on port %s at %s', server.address().port, server.address().address);
});

// log error
server.on("error", function (err) {
    log.error(err)
});

// log close
server.on("close", function () {
    log.info("server on port: " + port + " closed")
});

function close() {
    if (server) {
        server.close();
        log.info("Closing Proxy API")
    }
}
