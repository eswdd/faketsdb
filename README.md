[![NPM version][npm-version-image]][npm-url]
[![NPM downloads][npm-downloads-image]][npm-url]
[![GPL License][license-image]][license-url]
[![Build Status][travis-image]][travis-url]
[![Coverage][coverage-image]][coverage-url]

# faketsdb

Fake implementation of OpenTSDB API as an Express Router to aid in the development of API consumers.

## Usage

Run standalone from the command line:

    npm start

Or with options:

    node faketsdb.js -p 4242 -v

Or embed in an existing application using Express:

    var app = express();
    var faketsdb = require('faketsdb');
    // config optional, sensible defaults exist
    var config = {
        probabilities: {
            noData: 0.1,
            missingPoint: 0.1
        }
    };
    faketsdb.installFakeTsdb(app, config);

    var server = app.listen(4242, function() {
        var host = server.address().address
        var port = server.address().port

        console.log('FakeTSDB running at http://%s:%s', host, port)
    });

    // add some time series
    faketsdb.addTimeSeries("some.metric", {host:"host01}, "gauge")

Current implemented endpoints:

 * /api/aggregators - GET
 * /api/aggregators - POST
 * /api/query - GET
 * /api/search/lookup - GET
 * /api/search/lookup - POST
 * /api/suggest - GET
 * /api/uid/uidmeta - GET

## License

Faketsdb is freely distributable under the terms of the [GPL license](https://github.com/eswdd/faketsdb/blob/master/LICENSE).

[license-image]: http://img.shields.io/badge/license-GPL-blue.svg?style=flat
[license-url]: LICENSE

[npm-url]: https://npmjs.org/package/faketsdb
[npm-version-image]: http://img.shields.io/npm/v/faketsdb.svg?style=flat
[npm-downloads-image]: http://img.shields.io/npm/dm/faketsdb.svg?style=flat

[travis-url]: http://travis-ci.org/eswdd/faketsdb
[travis-image]: http://img.shields.io/travis/eswdd/faketsdb/master.svg?style=flat

[coverage-url]: https://coveralls.io/r/vkarpov15/fizzbuzz-coverage
[coverage-image]: https://coveralls.io/repos/vkarpov15/fizzbuzz-coverage/badge.svg