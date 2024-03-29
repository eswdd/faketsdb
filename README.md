[![NPM version][npm-version-image]][npm-url]
[![NPM downloads][npm-downloads-image]][npm-url]
[![GPL License][license-image]][license-url]
[![Build Status][github-build-image]][github-build-url]
[![Coverage][coverage-image]][coverage-url]
[![FOSSA Status][fossa-image]][fossa-url]

# faketsdb

Fake implementation of nodetsdb-api's backend interface

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

## License

Faketsdb is freely distributable under the terms of the [GPL license](https://github.com/eswdd/faketsdb/blob/master/LICENSE).

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Feswdd%2Ffaketsdb.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Feswdd%2Ffaketsdb?ref=badge_large)

<!-- Reference style link definitions -->

[license-image]: http://img.shields.io/badge/license-GPL-blue.svg?style=flat
[license-url]: LICENSE

[npm-url]: https://npmjs.org/package/faketsdb
[npm-version-image]: http://img.shields.io/npm/v/faketsdb.svg?style=flat
[npm-downloads-image]: http://img.shields.io/npm/dm/faketsdb.svg?style=flat

[coverage-url]: https://coveralls.io/r/eswdd/faketsdb
[coverage-image]: https://coveralls.io/repos/github/eswdd/faketsdb/badge.svg

[github-build-url]: https://github.com/eswdd/faketsdb/actions/workflows/node.js.yml
[github-build-image]: https://github.com/eswdd/faketsdb/actions/workflows/node.js.yml/badge.svg

[fossa-url]: https://app.fossa.io/projects/git%2Bgithub.com%2Feswdd%2Ffaketsdb?ref=badge_shield
[fossa-image]: https://app.fossa.io/api/projects/git%2Bgithub.com%2Feswdd%2Ffaketsdb.svg?type=shield
