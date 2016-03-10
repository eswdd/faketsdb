var request = require('supertest')
     , util = require('util')
   , assert = require('assert');

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
        faketsdb.install(app, {logRequests:false});

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

    it('responds to GET  /api/query', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        request(server)
            .get('/api/query?start=1m-ago&m=sum:10s-avg:some.metric{host=host1}&arrays=true')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var body = res.body;
                if (body.length != 1) {
                    return errorExpectedActual("expected returned metric count to be 1, not "+body.length, 1, body.length);
                }
                if (body[0].metric != "some.metric") {
                    return errorExpectedActual("expected returned metric to be some.metric, not "+body[0].metric, "some.metric", body[0].metric);
                }
                if (JSON.stringify(body[0].tags) != '{\"host\":"host1"}') {
                    return errorExpectedActual("expected returned tags to be {\"host\":\"host1\"}, not "+JSON.stringify(body[0].tags), "{\"host\":\"host1\"}", body[0].tags);
                }
                if (JSON.stringify(body[0].aggregatedTags) != '[]') {
                    return errorExpectedActual("expected returned aggregated tags to be [], not "+JSON.stringify(body[0].aggregatedTags), "[]", body[0].aggregatedTags);
                }
            })
            .end(done);
    });

    it('responds to GET  /api/query for an aggregated call', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1","type":"type1"}, "gauge");
        faketsdb.addTimeSeries("some.metric", {"host":"host2","type":"type1"}, "gauge");

        request(server)
            .get('/api/query?start=1m-ago&m=sum:10s-avg:some.metric{type=type1}&arrays=true')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var body = res.body;
                if (body.length != 1) {
                    return errorExpectedActual("expected returned metric count to be 1, not "+body.length, 1, body.length);
                }
                if (body[0].metric != "some.metric") {
                    return errorExpectedActual("expected returned metric to be some.metric, not "+body[0].metric, "some.metric", body[0].metric);
                }
                if (JSON.stringify(body[0].tags) != '{\"type\":"type1"}') {
                    return errorExpectedActual("expected returned tags to be {\"type\":\"type1\"}, not "+JSON.stringify(body[0].tags), "{\"type\":\"type1\"}", body[0].tags);
                }
                if (JSON.stringify(body[0].aggregatedTags) != '["host"]') {
                    return errorExpectedActual("expected returned aggregated tags to be [\"host\"], not "+JSON.stringify(body[0].aggregatedTags), "[\"host\"]", body[0].aggregatedTags);
                }
            })
            .end(done);
    });

    it('responds to GET  /api/query for an aggregated call with an empty tag string', function(done) {
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");
        faketsdb.addTimeSeries("some.metric", {"host":"host2"}, "gauge");

        request(server)
            .get('/api/query?start=1m-ago&m=sum:10s-avg:some.metric{}&arrays=true')
            .expect('Content-Type', /json/)
            .expect(200)
            .expect(function(res) {
                var body = res.body;
                if (body.length != 1) {
                    return errorExpectedActual("expected returned metric count to be 1, not "+body.length, 1, body.length);
                }
                if (body[0].metric != "some.metric") {
                    return errorExpectedActual("expected returned metric to be some.metric, not "+body[0].metric, "some.metric", body[0].metric);
                }
                if (JSON.stringify(body[0].tags) != '{}') {
                    return errorExpectedActual("expected returned tags to be {}, not "+JSON.stringify(body[0].tags), "{}", body[0].tags);
                }
                if (JSON.stringify(body[0].aggregatedTags) != '["host"]') {
                    return errorExpectedActual("expected returned aggregated tags to be [\"host\"], not "+JSON.stringify(body[0].aggregatedTags), "[\"host\"]", body[0].aggregatedTags);
                }
            })
            .end(done);
    });

    it('responds to GET  /api/query with consistent data each call', function(done) {
        this.timeout(12000);
        faketsdb.addTimeSeries("some.metric", {"host":"host1"}, "gauge");

        var timeToNext10s = 10000 - new Date().getTime() % 10000;

        setTimeout(function () {
            var firstBody = [];
            request(server)
                .get('/api/query?start=1m-ago&m=sum:10s-avg:some.metric{host=host1}&arrays=true')
                .expect('Content-Type', /json/)
                .expect(200)
                .expect(function(res) {
                    firstBody = res.body;
                })
                .end(function () {
                    var secondBody = [];
                    request(server)
                        .get('/api/query?start=1m-ago&m=sum:10s-avg:some.metric{host=host1}&arrays=true')
                        .expect('Content-Type', /json/)
                        .expect(200, firstBody)
                        .end(done);
                });
        }, timeToNext10s+500);
    });

    it('fails    to POST /api/query', function(done) {
        request(server)
            .post('/api/query')
            .expect(404)
            .end(done);
    });
});