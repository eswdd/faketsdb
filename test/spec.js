var request = require('supertest');
describe('loading express', function () {
    var server, faketsdb;
    beforeEach(function () {
        var app = require('express')();
        faketsdb = require('../faketsdb');
        faketsdb.install(app, {log:false});

        server = app.listen(4242, function() {
            var host = server.address().address
            var port = server.address().port

//            console.log('FakeTSDB under test at http://%s:%s', host, port)
        });
    });
    afterEach(function (done) {
//        console.log("Stopping FakeTSDB")
        server.close(done);
    });
    it('responds to /api/aggregators', function testSlash(done) {
        request(server)
            .get('/api/aggregators')
            .expect(200, done);
    });
    it('404 everything else', function testPath(done) {
        request(server)
            .get('/foo/bar')
            .expect(404, done);
    });
});