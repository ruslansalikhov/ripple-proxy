'use strict'

var Promise = require('bluebird')
var moment = require('moment')
var smoment = require('../../smoment')
var utils = require('../../utils')
var Parser = require('../../ledgerParser')
var binary = require('ripple-binary-codec')

var isoUTC = 'YYYY-MM-DDTHH:mm:ss[Z]'
var EPOCH_OFFSET = 946684800
var LI_PAD = 12
var I_PAD = 5
var S_PAD = 12

var exchangeIntervals = [
  '1minute',
  '5minute',
  '15minute',
  '30minute',
  '1hour',
  '2hour',
  '4hour',
  '1day',
  '3day',
  '7day',
  '1month',
  '1year'
]

var HbaseClient = {}

/**
 * getLedger
 */

HbaseClient.getLedger = function(options, callback) {
  var self = this

  function getLedgerByHash(opts) {
    var hashes = []

    self.getRow({
      table: 'ledgers',
      rowkey: opts.ledger_hash
    }, function(err, ledger) {

      if (err || !ledger) {
        callback(err, null)
        return
      }

      delete ledger.rowkey

      if (ledger.parent_close_time) {
        ledger.parent_close_time = Number(ledger.parent_close_time)
        if (ledger.parent_close_time < EPOCH_OFFSET) {
          ledger.parent_close_time += EPOCH_OFFSET
        }
      }

      ledger.ledger_index = Number(ledger.ledger_index)
      ledger.close_time = Number(ledger.close_time)
      ledger.close_time_human = moment.unix(ledger.close_time).utc()
        .format('YYYY-MMM-DD HH:mm:ss')
      ledger.transactions = JSON.parse(ledger.transactions)

      // get transactions
      if (ledger.transactions.length &&
          (opts.expand || opts.binary)) {
        hashes = ledger.transactions
        ledger.transactions = []
        self.getTransactions({
          hashes: hashes,
          binary: opts.binary,
          include_ledger_hash: opts.include_ledger_hash

        }, function(e, resp) {

          if (e) {
            callback(e, null)
            return

          } else if (hashes.length !== resp.rows.length && !opts.invalid) {
            callback('missing transaction: ' +
                   resp.rows.length + ' of ' +
                   hashes.length + ' found')
            return
          }

          ledger.transactions = resp.rows
          callback(e, ledger)
        })

      // return the ledger as is
      } else if (opts.transactions) {
        callback(null, ledger)

      // remove tranactions array
      } else {
        delete ledger.transactions
        callback(null, ledger)
      }
    })
  }

  // get by hash
  if (options.ledger_hash) {
    getLedgerByHash(options)

  // get ledger by close time
  } else if (options.closeTime) {
    self.getLedgersByTime({
      start: moment.utc(0),
      end: options.closeTime,
      descending: true,
      limit: 1
    }, function(err, resp) {
      if (err || !resp || !resp.length) {
        callback(err, null)
        return
      }

      // use the ledger hash to get the ledger
      options.ledger_hash = resp[0].ledger_hash
      getLedgerByHash(options)
    })

  // get by index, or get latest
  } else {
    self.getLedgersByIndex({
      startIndex: options.ledger_index || 0,
      stopIndex: options.ledger_index || 999999999999,
      descending: options.ledger_index ? false : true,
      limit: options.pad || 2
    }, function(err, resp) {

      if (err || !resp || !resp.length) {
        callback(err, null)
        return

      //  submit error on duplicate ledger index
      } else if (resp.length > 1 && options.ledger_index) {
        callback('duplicate ledger index: ' + options.ledger_index, null)
        return

      // latest + padded leeway
      } else if (options.pad) {
        options.ledger_hash = resp[resp.length - 1].ledger_hash

      } else {
        options.ledger_hash = resp[0].ledger_hash
      }

      getLedgerByHash(options)
    })
  }
}

/**
 * getTransaction
 */

HbaseClient.getTransaction = function(options, callback) {
  options.hashes = [options.tx_hash]

  this.getTransactions(options, function(err, resp) {
    if (resp) {
      callback(null, resp.rows ? resp.rows[0] : undefined)
    } else {
      callback(err)
    }
  })
}

/**
 * getTransactions
 */

HbaseClient.getTransactions = function(options, callback) {
  var self = this

  function clone(d) {
    return JSON.parse(JSON.stringify(d))
  }

  function isPartialPayment(flags) {
    return 0x00020000 & flags
  }

  function compare(a, b) {
    if (Number(a.tx_index) < Number(b.tx_index)) {
      return -1
    } else {
      return 1
    }
  }

  function getTransactionsByTime(opts, cb) {
    var filters = []

    if (opts.type) {
      filters.push({
        qualifier: 'type',
        value: opts.type,
        family: 'f',
        comparator: '='
      })
    }

    if (opts.result) {
      filters.push({
        qualifier: 'result',
        value: opts.result,
        family: 'f',
        comparator: '='
      })
    }

    self.getScanWithMarker(self, {
      table: 'lu_transactions_by_time',
      startRow: opts.start.hbaseFormatStartRow(),
      stopRow: opts.end.hbaseFormatStopRow(),
      marker: opts.marker,
      descending: opts.descending,
      limit: opts.limit,
      filterString: self.buildSingleColumnValueFilters(filters),
      columns: ['d:tx_hash', 'f:type', 'f:result']
    }, function(err, resp) {

      if (resp) {
        resp.rows.forEach(function(row, i) {
          resp.rows[i] = row.tx_hash
        })
      }

      cb(err, resp)
    })
  }

  function getTransactionsFromHashes(opts, cb) {
    var results = {
      marker: opts.marker,
      rows: []
    }

    function formatTx(d) {
      var tx = { }

      tx.hash = d.rowkey
      tx.ledger_index = Number(d.ledger_index)
      tx.date = moment.unix(d.executed_time).utc()
      .format('YYYY-MM-DDTHH:mm:ssZ')

      if (opts.include_ledger_hash) {
        tx.ledger_hash = d.ledger_hash
      }

      if (opts.binary) {
        tx.tx = d.raw
        tx.meta = d.meta

      } else {
        tx.tx = binary.decode(d.raw)
        tx.meta = binary.decode(d.meta)

        // handle delivered_amount for successful payments
        if (tx.tx.TransactionType === 'Payment' &&
            tx.meta.TransactionResult === 'tesSUCCESS') {

          // DeliveredAmount is present
          if (tx.meta.DeliveredAmount) {
            tx.meta.delivered_amount = tx.meta.DeliveredAmount

          // not a partial payment
          } else if (!isPartialPayment(tx.tx.Flags)) {
            tx.meta.delivered_amount = clone(tx.tx.Amount)

          // partial payment without
          // DeliveredAmount after 4594094
          } else if (tx.ledger_index > 4594094) {
            tx.meta.delivered_amount = clone(tx.tx.Amount)

          // partial payment before 4594094
          } else {
            tx.meta.delivered_amount = 'unavailable'
          }
        }
      }

      return tx
    }

    self.getRows({
      table: 'transactions',
      rowkeys: opts.hashes,
      columns: [
        'f:executed_time',
        'f:ledger_index',
        'f:ledger_hash',
        'd:raw',
        'd:meta',
        'd:tx_index'
      ]
    }, function(err, resp) {

      if (err) {
        cb(err)
        return
      }

      if (resp) {

        if (opts.ledger) {
          resp.sort(compare)
        }


        try {
          results.rows = resp.map(formatTx)

        } catch (e) {
          cb(e)
          return
        }
      }

      cb(null, results)
    })
  }

  if (options.hashes) {
    getTransactionsFromHashes(options, callback)

  } else {
    getTransactionsByTime(options, function(err, resp) {

      if (err) {
        callback(err)

      } else if (resp && resp.rows) {
        options.marker = resp.marker // replace/add marker
        options.hashes = resp.rows
        getTransactionsFromHashes(options, callback)

      } else {
        callback(null, {rows: []})
      }
    })
  }
}

/**
 * saveLedger
 */

HbaseClient.saveLedger = function(ledger, callback) {
  var self = this
  var tableNames = []
  var tables = self.prepareLedgerTables(ledger)

  tableNames = Object.keys(tables)

  Promise.map(tableNames, function(name) {
    return self.putRows({
      table: name,
      rows: tables[name]
    })
  })
  .nodeify(function(err, resp) {
    if (err) {
      self.log.error('error saving ledger:', ledger.ledger_index, err)
    } else {
      self.log.info('ledger saved:', ledger.ledger_index)
    }

    if (callback) {
      callback(err, resp)
    }
  })
}

/**
 * saveTransaction
 */

HbaseClient.saveTransaction = function(tx, callback) {
  this.saveTransactions([tx], callback)
}

/**
 * saveTransactions
 */

HbaseClient.saveTransactions = function(transactions, callback) {
  var self = this
  var tables = self.prepareTransactions(transactions)
  var tableNames = Object.keys(tables)

  Promise.map(tableNames, function(name) {
    return self.putRows({
      table: name,
      rows: tables[name]
    })
  })
  .nodeify(function(err) {
    if (err) {
      self.log.error('error saving transaction(s)', err)
    } else {
      self.log.info(transactions.length + ' transaction(s) saved')
    }

    if (callback) {
      callback(err, transactions.length)
    }
  })
}

/**
 * prepareLedgerTables
 */

HbaseClient.prepareLedgerTables = function(ledger) {
  var tables = {
    ledgers: { },
    lu_ledgers_by_index: { },
    lu_ledgers_by_time: { }
  }

  var ledgerIndexKey = utils.padNumber(ledger.ledger_index, LI_PAD) +
    '|' + ledger.ledger_hash

  var ledgerTimeKey = utils.formatTime(ledger.close_time) +
    '|' + utils.padNumber(ledger.ledger_index, LI_PAD)

  // add formated ledger
  tables.ledgers[ledger.ledger_hash] = ledger

  // add ledger index lookup
  tables.lu_ledgers_by_index[ledgerIndexKey] = {
    ledger_hash: ledger.ledger_hash,
    parent_hash: ledger.parent_hash,
    'f:ledger_index': ledger.ledger_index,
    'f:close_time': ledger.close_time
  }

  // add ledger by time lookup
  tables.lu_ledgers_by_time[ledgerTimeKey] = {
    ledger_hash: ledger.ledger_hash,
    parent_hash: ledger.parent_hash,
    'f:ledger_index': ledger.ledger_index,
    'f:close_time': ledger.close_time
  }

  return tables
}

/*
 * prepareTransactions
 */

HbaseClient.prepareTransactions = function(transactions) {
  var data = {
    transactions: { },
    lu_transactions_by_time: { },
    lu_account_transactions: { }
  }

  transactions.forEach(function(tx) {
    var key

    // transactions by time
    key = utils.formatTime(tx.executed_time) +
      '|' + utils.padNumber(tx.ledger_index, LI_PAD) +
      '|' + utils.padNumber(tx.tx_index, I_PAD)

    data.lu_transactions_by_time[key] = {
      tx_hash: tx.hash,
      tx_index: tx.tx_index,
      'f:executed_time': tx.executed_time,
      'f:ledger_index': tx.ledger_index,
      'f:type': tx.TransactionType,
      'f:result': tx.tx_result
    }

    // transactions by account sequence
    key = tx.Account + '|' + utils.padNumber(tx.Sequence, S_PAD)

    data.lu_account_transactions[key] = {
      tx_hash: tx.hash,
      sequence: tx.Sequence,
      'f:executed_time': tx.executed_time,
      'f:ledger_index': tx.ledger_index,
      'f:type': tx.TransactionType,
      'f:result': tx.tx_result
    }

    tx['f:Account'] = tx.Account
    tx['f:Sequence'] = tx.Sequence
    tx['f:tx_result'] = tx.tx_result
    tx['f:TransactionType'] = tx.TransactionType
    tx['f:executed_time'] = tx.executed_time
    tx['f:ledger_index'] = tx.ledger_index
    tx['f:ledger_hash'] = tx.ledger_hash
    tx['f:client'] = tx.client

    delete tx.Account
    delete tx.Sequence
    delete tx.tx_result
    delete tx.TransactionType
    delete tx.executed_time
    delete tx.ledger_index
    delete tx.ledger_hash
    delete tx.client

    // add transaction
    data.transactions[tx.hash] = tx
  })

  return data
}

/**
 * prepareParsedData
 */

HbaseClient.prepareParsedData = function(data) {
  var tables = {
    exchanges: { },
    account_offers: { },
    account_exchanges: { },
    balance_changes: { },
    payments: { },
    payments_by_currency: { },
    escrows: { },
    payment_channels: { },
    account_escrows: { },
    account_payments: { },
    accounts_created: { },
    account_payment_channels: { },
    memos: { },
    lu_account_memos: { },
    lu_affected_account_transactions: { },
    lu_account_offers_by_sequence: { }
  }

  // add exchanges
  data.exchanges.forEach(function(ex) {
    var suffix = utils.formatTime(ex.time) +
      '|' + utils.padNumber(ex.ledger_index, LI_PAD) +
      '|' + utils.padNumber(ex.tx_index, I_PAD) +
      '|' + utils.padNumber(ex.node_index, I_PAD) // guarantee uniqueness

    var key = ex.base.currency +
      '|' + (ex.base.issuer || '') +
      '|' + ex.counter.currency +
      '|' + (ex.counter.issuer || '') +
      '|' + suffix

    var key2 = ex.buyer + '|' + suffix
    var key3 = ex.seller + '|' + suffix
    var row = {
      'f:base_currency': ex.base.currency,
      'f:base_issuer': ex.base.issuer || undefined,
      base_amount: ex.base.amount,
      'f:counter_currency': ex.counter.currency,
      'f:counter_issuer': ex.counter.issuer || undefined,
      counter_amount: ex.counter.amount,
      rate: ex.rate,
      'f:buyer': ex.buyer,
      'f:seller': ex.seller,
      'f:taker': ex.taker,
      'f:provider': ex.provider,
      'f:offer_sequence': ex.sequence,
      'f:tx_hash': ex.tx_hash,
      'f:executed_time': ex.time,
      'f:ledger_index': ex.ledger_index,
      'f:tx_type': ex.tx_type,
      'f:client': ex.client,
      tx_index: ex.tx_index,
      node_index: ex.node_index
    }

    if (ex.autobridged) {
      row['f:autobridged_currency'] = ex.autobridged.currency
      row['f:autobridged_issuer'] = ex.autobridged.issuer
    }

    tables.exchanges[key] = row
    tables.account_exchanges[key2] = row
    tables.account_exchanges[key3] = row
  })

  // add offers
  data.offers.forEach(function(o) {

    var key = o.account +
      '|' + utils.formatTime(o.executed_time) +
      '|' + utils.padNumber(o.ledger_index, LI_PAD) +
      '|' + utils.padNumber(o.tx_index, I_PAD) +
      '|' + utils.padNumber(o.node_index, I_PAD)

    tables.account_offers[key] = {
      'f:tx_type': o.tx_type,
      'f:account': o.account,
      'f:offer_sequence': o.offer_sequence,
      'f:node_type': o.node_type,
      'f:change_type': o.change_type,
      'f:pays_currency': o.taker_pays.currency,
      'f:pays_issuer': o.taker_pays.issuer || undefined,
      pays_amount: o.taker_pays.value,
      pays_change: o.pays_change,
      'f:gets_currency': o.taker_gets.currency,
      'f:gets_issuer': o.taker_gets.issuer || undefined,
      gets_amount: o.taker_gets.value,
      gets_change: o.gets_change,
      rate: o.rate,
      'f:book_directory': o.book_directory,
      'f:expiration': o.expiration,
      'f:next_offer_sequence': o.next_offer_sequence,
      'f:prev_offer_sequence': o.prev_offer_sequence,
      'f:executed_time': o.executed_time,
      'f:ledger_index': o.ledger_index,
      'f:client': o.client,
      tx_index: o.tx_index,
      node_index: o.node_index,
      tx_hash: o.tx_hash
    }

    key = o.account +
      '|' + o.sequence +
      '|' + utils.padNumber(o.ledger_index, LI_PAD) +
      '|' + utils.padNumber(o.tx_index, I_PAD) +
      '|' + utils.padNumber(o.node_index, I_PAD)

    tables.lu_account_offers_by_sequence[o.account + '|' + o.sequence] = {
      'f:account': o.account,
      'f:sequence': o.sequence,
      'f:type': o.type,
      'f:executed_time': o.executed_time,
      'f:ledger_index': o.ledger_index,
      tx_index: o.tx_index,
      node_index: o.node_index,
      tx_hash: o.tx_hash
    }
  })

  // add balance changes
  data.balanceChanges.forEach(function(c) {
    var suffix = '|' + utils.formatTime(c.time) +
      '|' + utils.padNumber(c.ledger_index, LI_PAD) +
      '|' + utils.padNumber(c.tx_index, I_PAD) +
      '|' + (c.node_index === -1 ? '$' : utils.padNumber(c.node_index, I_PAD))

    var row = {
      'f:account': c.account,
      'f:counterparty': c.counterparty,
      'f:currency': c.currency,
      amount_change: c.change,
      final_balance: c.final_balance,
      'f:change_type': c.type,
      'f:tx_hash': c.tx_hash,
      'f:executed_time': c.time,
      'f:ledger_index': c.ledger_index,
      tx_index: c.tx_index,
      node_index: c.node_index,
      'f:client': c.client,
      'f:escrow_counterparty': c.escrow_counterparty,
      escrow_balance_change: c.escrow_balance_change,
      'f:paychannel_counterparty': c.paychannel_counterparty,
      paychannel_fund_change: c.paychannel_fund_change,
      paychannel_fund_final_balance: c.paychannel_fund_final_balance,
      paychannel_final_balance: c.paychannel_final_balance
    }

    tables.balance_changes[c.account + suffix] = row
  })

  data.payments.forEach(function(p) {
    var key = utils.formatTime(p.time) +
      '|' + utils.padNumber(p.ledger_index, LI_PAD) +
      '|' + utils.padNumber(p.tx_index, I_PAD)
    var currency = p.currency + '|' + (p.issuer || '')

    var payment = {
      'f:source': p.source,
      'f:destination': p.destination,
      amount: p.amount,
      delivered_amount: p.delivered_amount,
      'f:currency': p.currency,
      'f:issuer': p.issuer,
      'f:source_currency': p.source_currency,
      fee: p.fee,
      source_balance_changes: p.source_balance_changes,
      destination_balance_changes: p.destination_balance_changes,
      'f:tx_hash': p.tx_hash,
      'f:executed_time': p.time,
      'f:ledger_index': p.ledger_index,
      tx_index: p.tx_index,
      'f:client': p.client
    }

    if (p.max_amount) {
      payment.max_amount = p.max_amount
    }

    if (p.destination_tag) {
      payment['f:destination_tag'] = p.destination_tag
    }

    if (p.source_tag) {
      payment['f:source_tag'] = p.source_tag
    }

    if (p.invoice_id) {
      payment['f:invoice_id'] = p.invoice_id
    }

    tables.payments[key] = payment
    tables.payments_by_currency[currency + '|' + key] = payment
    tables.account_payments[p.source + '|' + key] = payment
    tables.account_payments[p.destination + '|' + key] = payment
  })

  // add escrows
  data.escrows.forEach(function(d) {
    var key = utils.formatTime(d.time) +
      '|' + utils.padNumber(d.ledger_index, LI_PAD) +
      '|' + utils.padNumber(d.tx_index, I_PAD)

    var escrow = {
      'f:tx_type': d.tx_type,
      'f:account': d.account,
      'f:owner': d.owner,
      'f:destination': d.destination,
      'f:destination_tag': d.destination_tag,
      'f:source_tag': d.source_tag,
      create_tx: d.create_tx,
      create_tx_seq: d.create_tx_seq,
      condition: d.condition,
      fulfillment: d.fulfillment,
      amount: d.amount,
      flags: d.flags,
      fee: d.fee,
      'f:tx_hash': d.tx_hash,
      'f:executed_time': d.time,
      'f:cancel_after': d.cancel_after,
      'f:finish_after': d.finish_after,
      'f:ledger_index': d.ledger_index,
      tx_index: d.tx_index,
      'f:client': d.client
    }

    tables.escrows[key] = escrow
    tables.account_escrows[d.owner + '|' + key] = escrow
    tables.account_escrows[d.destination + '|' + key] = escrow
  })

  // add paychan
  data.paychan.forEach(function(d) {
    var key = utils.formatTime(d.time) +
      '|' + utils.padNumber(d.ledger_index, LI_PAD) +
      '|' + utils.padNumber(d.tx_index, I_PAD)

    var paychan = {
      'f:channel': d.channel,
      'f:tx_type': d.tx_type,
      'f:account': d.account,
      'f:owner': d.owner,
      'f:source': d.source,
      'f:destination': d.destination,
      'f:destination_tag': d.destination_tag,
      'f:source_tag': d.source_tag,
      'f:cancel_after': d.cancel_after,
      'f:expiration': d.expiration,
      amount: d.amount,
      balance: d.balance,
      settle_delay: d.settle,
      signature: d.signature,
      pubkey: d.pubkey,
      flags: d.flags,
      fee: d.fee,
      'f:tx_hash': d.tx_hash,
      'f:executed_time': d.time,
      'f:ledger_index': d.ledger_index,
      tx_index: d.tx_index,
      'f:client': d.client
    }

    tables.payment_channels[key] = paychan
    tables.account_payment_channels[d.source + '|' + key] = paychan
    tables.account_payment_channels[d.destination + '|' + key] = paychan
  })

  // add accounts created
  data.accountsCreated.forEach(function(a) {
    var key = utils.formatTime(a.time) +
      '|' + utils.padNumber(a.ledger_index, LI_PAD) +
      '|' + utils.padNumber(a.tx_index, I_PAD)

    tables.accounts_created[key] = {
      'f:account': a.account,
      'f:parent': a.parent,
      balance: a.balance,
      'f:tx_hash': a.tx_hash,
      'f:executed_time': a.time,
      'f:ledger_index': a.ledger_index,
      tx_index: a.tx_index,
      'f:client': a.client
    }
  })

  // add memos
  data.memos.forEach(function(m) {
    var key = utils.formatTime(m.executed_time) +
      '|' + utils.padNumber(m.ledger_index, LI_PAD) +
      '|' + utils.padNumber(m.tx_index, I_PAD) +
      '|' + utils.padNumber(m.memo_index, I_PAD)

    tables.memos[key] = {
      'f:account': m.account,
      'f:destination': m.destination,
      'f:source_tag': m.source_tag,
      'f:destination_tag': m.destination_tag,
      memo_type: m.memo_type,
      memo_data: m.memo_data,
      memo_format: m.memo_format,
      decoded_type: m.decoded_type,
      decoded_data: m.decoded_data,
      decoded_format: m.decoded_format,
      type_encoding: m.type_encoding,
      data_encoding: m.data_encoding,
      format_encoding: m.format_encoding,
      'f:tx_hash': m.tx_hash,
      'f:executed_time': m.executed_time,
      'f:ledger_index': m.ledger_index,
      tx_index: m.tx_index,
      memo_index: m.memo_index
    }

    tables.lu_account_memos[m.account + '|' + key] = {
      rowkey: key,
      'f:is_sender': true,
      'f:tag': m.source_tag,
      'f:tx_hash': m.tx_hash,
      'f:executed_time': m.executed_time,
      'f:ledger_index': m.ledger_index,
      tx_index: m.tx_index,
      memo_index: m.memo_index
    }

    if (m.destination) {
      tables.lu_account_memos[m.destination + '|' + key] = {
        rowkey: key,
        'f:is_source': false,
        'f:tag': m.destination_tag,
        'f:tx_hash': m.tx_hash,
        'f:executed_time': m.executed_time,
        'f:ledger_index': m.ledger_index,
        tx_index: m.tx_index,
        memo_index: m.memo_index
      }
    }
  })

  // add affected accounts
  data.affectedAccounts.forEach(function(a) {
    var key = a.account +
      '|' + utils.formatTime(a.time) +
      '|' + utils.padNumber(a.ledger_index, LI_PAD) +
      '|' + utils.padNumber(a.tx_index, I_PAD)

    tables.lu_affected_account_transactions[key] = {
      'f:type': a.tx_type,
      'f:result': a.tx_result,
      tx_hash: a.tx_hash,
      tx_index: a.tx_index,
      'f:executed_time': a.time,
      'f:ledger_index': a.ledger_index,
      'f:client': a.client
    }
  })

  return tables
}

/**
 * SaveParsedData
 */

HbaseClient.saveParsedData = function(params, callback) {
  var self = this
  var tables = self.prepareParsedData(params.data)
  var tableNames

  tableNames = params.tableNames ? params.tableNames : Object.keys(tables)

  Promise.map(tableNames, function(name) {
    return self.putRows({
      table: name,
      rows: tables[name]
    })
  })
  .nodeify(function(err, resp) {
    var total = 0
    if (err) {
      self.log.error('error saving parsed data', err)

    } else {
      if (resp) {
        resp.forEach(function(r) {
          if (r && r[0]) {
            total += r[0]
          }
        })
      }

      self.log.info('parsed data saved:', total + ' rows')
    }

    if (callback) {
      callback(err, total)
    }
  })
}

/**
 * removeLedger
 */

HbaseClient.removeLedger = function(hash, callback) {
  var self = this

  self.getLedger({
    ledger_hash: hash,
    transactions: true,
    expand: true,
    invalid: true

  }, function(err, ledger) {
    var parsed
    var primary
    var secondary
    var transactions
    var tables
    var table

    if (err) {
      self.log.error('error fetching ledger:', hash, err)
      callback(err)
      return
    }

    if (!ledger) {
      callback('ledger not found')
      return
    }

    // parser expects ripple epoch
    ledger.close_time -= EPOCH_OFFSET
    transactions = ledger.transactions
    ledger.transactions = []

    // ledgers must be formatted according to the output from
    // rippled's ledger command
    transactions.forEach(function(tx) {
      if (tx) {
        var transaction = tx.tx
        transaction.metaData = tx.meta
        transaction.hash = tx.hash
        ledger.transactions.push(transaction)
      }
    })

    parsed = Parser.parseLedger(ledger)
    primary = self.prepareLedgerTables(ledger)
    secondary = self.prepareParsedData(parsed)
    transactions = self.prepareTransactions(parsed.transactions)
    tables = []

    for (table in primary) {
      tables.push({
        table: table,
        keys: Object.keys(primary[table])
      })
    }

    for (table in transactions) {
      tables.push({
        table: table,
        keys: Object.keys(transactions[table])
      })
    }

    for (table in secondary) {
      tables.push({
        table: table,
        keys: Object.keys(secondary[table])
      })
    }

    Promise.map(tables, function(t) {
      return self.deleteRows({
        table: t.table,
        rowkeys: t.keys
      })
    }).nodeify(function(e, resp) {
      if (!e) {
        self.log.info('ledger removed:', ledger.ledger_index, hash)
      }

      callback(err, resp)
    })
  })
}

module.exports = HbaseClient
