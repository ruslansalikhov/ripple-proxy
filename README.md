# Ripple Data Proxy 

**NOTE:** This project based on https://github.com/ripple/rippled-historical-database

The Ripple Data Proxy provides parsed data got for full-history rippled node.

## More Information
* [API Methods](#api-method-reference)
* [Setup (local instance)](#running-the-proxy)

# API Method Reference

The Data API v2 provides a REST API with the following methods:

Ledger Contents Methods:

* [Get Ledger - `GET /ledger/{ledger_index}`](#get-ledger)

## Get Ledger

Retrieve a specific Ledger by index.

#### Request Format

<!-- MULTICODE_BLOCK_START -->

*REST*

```
GET /ledger/{index}
```

<!-- MULTICODE_BLOCK_END -->

#### Response Format

A successful response uses the HTTP code **200 OK** and has a JSON body compatible with https://github.com/ripple/rippled-historical-database#get-ledger


# Running the Proxy

## Installation

### Dependencies

The Proxy requires the following software installed first:

* [Node.js](http://nodejs.org/)
* [npm](https://www.npmjs.org/)
* [git](http://git-scm.com/) (optional) for installation and updating.

### Installation Process

To install the Proxy:

1. Clone the Historical Database Git Repository:

        git clone https://github.com/ruslansalikhov/ripple-proxy.git

    (You can also download and extract a zipped release instead.)

2. Use npm to install additional modules:

        cd ripple-proxy
        npm install

3. Create config/config.json.

At this point, the Proxy is installed. You can start it:

        npm run start    

