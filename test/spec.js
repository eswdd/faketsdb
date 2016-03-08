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

    it('responds to GET  /api/aggregators', function(done) {
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

    it('responds to POST /api/aggregators', function(done) {
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

    it('responds to GET  /api/suggest when no timeseries configured', function(done) {
        request(server)
            .get('/api/suggest?type=metrics')
            .expect('Content-Type', /json/)
            .expect(200, [])
            .end(done);
    });

    it('responds to GET  /api/suggest when one timeseries configured', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics')
            .expect('Content-Type', /json/)
            .expect(200, ["some.metric"])
            .end(done);
    });

    it('responds to GET  /api/suggest when nothing matches query', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics&q=other')
            .expect('Content-Type', /json/)
            .expect(200, [])
            .end(done);
    });

    it('responds to GET  /api/suggest when two timeseries configured and query should match both', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");
        faketsdb.addTimeSeries("some.other", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics')
            .expect('Content-Type', /json/)
            .expect(200, ["some.metric","some.other"])
            .end(done);
    });

    it('responds to GET  /api/suggest when two timeseries configured and query should match only one', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");
        faketsdb.addTimeSeries("some.other", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/suggest?type=metrics&q=some.o')
            .expect('Content-Type', /json/)
            .expect(200, ["some.other"])
            .end(done);
    });

    it('fails    to POST /api/suggest', function(done) {
        request(server)
            .post('/api/suggest?type=metrics')
            .expect(404)
            .end(done);
    });

    it('responds to GET  /api/search/lookup when one time series configured for a metric', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/search/lookup?m=some.metric')
            .expect('Content-Type', /json/)
            .expect(200,
                {
                    type: 'LOOKUP',
                    metric: 'some.metric',
                    time: 1,
                    results: [
                        {metric:'some.metric',tags:{host:"host1"}, tsuid: "000001000001000001"}
                    ],
                    startIndex: 0,
                    totalResults: 1
                })
            .end(done);
    });

    it('responds to GET  /api/search/lookup when two time series configured for a metric', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");
        faketsdb.addTimeSeries("some.metric", {"host":"host2"}, "gauge");

        request(server)
            .get('/api/search/lookup?m=some.metric')
            .expect('Content-Type', /json/)
            .expect(200,
                {
                    type: 'LOOKUP',
                    metric: 'some.metric',
                    time: 1,
                    results: [
                        {metric:'some.metric',tags:{host:"host1"}, tsuid: "000001000001000001"},
                        {metric:'some.metric',tags:{host:"host2"}, tsuid: "000001000001000002"}
                    ],
                    startIndex: 0,
                    totalResults: 2
                })
            .end(done);
    });

    it('responds to GET  /api/uid/uidmeta for type = metric', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/uid/uidmeta?uid=000001&type=metric')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var body = res.body;
                if (body.uid != "000001") {
                    return errorExpectedActual("expected returned uid to be 000001, not "+body.uid, "000001", body.uid);
                }
                if (body.type != "METRIC") {
                    return errorExpectedActual("expected returned type to be METRIC, not "+body.type, "METRIC", body.type);
                }
                if (body.name != "some.metric") {
                    return errorExpectedActual("expected returned name to be some.metric, not "+body.name, "some.metric", body.name);
                }
                if (body.created == 0) {
                    return errorActual("expected returned created time to be non-zero", 0);
                }
            })
            .end(done);
    });

    it('responds to GET  /api/uid/uidmeta for type = tagk', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/uid/uidmeta?uid=000001&type=tagk')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var body = res.body;
                if (body.uid != "000001") {
                    return errorExpectedActual("expected returned uid to be 000001, not "+body.uid, "000001", body.uid);
                }
                if (body.type != "TAGK") {
                    return errorExpectedActual("expected returned type to be TAGK, not "+body.type, "TAGK", body.type);
                }
                if (body.name != "host") {
                    return errorExpectedActual("expected returned name to be host, not "+body.name, "host", body.name);
                }
                if (body.created == 0) {
                    return errorActual("expected returned created time to be non-zero", 0);
                }
            })
            .end(done);
    });

    it('responds to GET  /api/uid/uidmeta for type = tagv', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/uid/uidmeta?uid=000001&type=tagv')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var body = res.body;
                if (body.uid != "000001") {
                    return errorExpectedActual("expected returned uid to be 000001, not "+body.uid, "000001", body.uid);
                }
                if (body.type != "TAGV") {
                    return errorExpectedActual("expected returned type to be TAGV, not "+body.type, "TAGV", body.type);
                }
                if (body.name != "host1") {
                    return errorExpectedActual("expected returned name to be host1, not "+body.name, "host1", body.name);
                }
                if (body.created == 0) {
                    return errorActual("expected returned created time to be non-zero", 0);
                }
            })
            .end(done);
    });

    it('fails    to POST /api/query', function(done) {
        request(server)
            .post('/api/query')
            .expect(404)
            .end(done);
    });
});