var request = require('supertest');

function errorExpectedActual(msg, expected, actual) {
    var err = new Error(msg);
    err.expected = expected;
    err.actual = actual;
    err.showDiff = true;
    return err;
}
function errorActual(msg, actual) {
    var err = new Error(msg);
    err.actual = actual;
    err.showDiff = true;
    return err;
}
function assertArrayContainsOnly(arrayDesc, expected, actual) {
    if (actual.length != 4) {
        return errorExpectedActual('expected '+arrayDesc+' of length '+expected.length+', got ' + actual.length, expected.length, actual.length);
    }
    for (var i=0; i<expected.length; i++) {
        var lookFor = expected[i];
        if (actual.indexOf(lookFor) < 0) {
            return errorActual('expected '+arrayDesc+' to contain '+JSON.stringify(lookFor)+', but was ' + JSON.stringify(actual), actual);
        }
    }
}
describe('Inline FakeTSDB', function () {
    var server, faketsdb;

    beforeEach(function () {
        var app = require('express')();
        faketsdb = require('../faketsdb');
        faketsdb.reset();
        faketsdb.install(app, {log:false});

        server = app.listen(4242);
    });

    afterEach(function (done) {
        server.close(done);
    });

    it('responds to GET  /api/aggregators', function testAggregators(done) {
        request(server)
            .get('/api/aggregators')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var ret = assertArrayContainsOnly("response body array", ["avg","sum","min","max"], res.body);
                if (ret) {
                    return ret;
                }
            })
            .end(done);
    });

    it('responds to POST /api/aggregators', function testAggregators(done) {
        request(server)
            .post('/api/aggregators')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var ret = assertArrayContainsOnly("response body array", ["avg","sum","min","max"], res.body);
                if (ret) {
                    return ret;
                }
            })
            .end(done);
    });

    it('responds to GET  /api/suggest when no timeseries configured', function testAggregators(done) {
        request(server)
            .get('/api/suggest?type=metrics')
            .expect('Content-Type', /json/)
            .expect(200, [])
            .end(done);
    });

    it('responds to GET  /api/suggest when one timeseries configured', function testAggregators(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics')
            .expect('Content-Type', /json/)
            .expect(200, ["some.metric"])
            .end(done);
    });

    it('responds to GET  /api/suggest when nothing matches query', function testAggregators(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics&q=other')
            .expect('Content-Type', /json/)
            .expect(200, [])
            .end(done);
    });

    it('responds to GET  /api/suggest when two timeseries configured and query should match both', function testAggregators(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");
        faketsdb.addTimeSeries("some.other", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics')
            .expect('Content-Type', /json/)
            .expect(200, ["some.metric","some.other"])
            .end(done);
    });

    it('responds to GET  /api/suggest when two timeseries configured and query should match only one', function testAggregators(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");
        faketsdb.addTimeSeries("some.other", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics&q=some.o')
            .expect('Content-Type', /json/)
            .expect(200, ["some.other"])
            .end(done);
    });
});