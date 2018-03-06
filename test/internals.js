var request = require('supertest')
     , util = require('util')
   , assert = require('assert'),
     rewire = require('rewire');

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

describe('Inline FakeTSDB Internals', function () {
    var faketsdb;

    beforeEach(function () {
        var app = require('express')();
        faketsdb = rewire('../faketsdb');
        faketsdb.reset();
        faketsdb.install(app, {logRequests:false});
    });

    afterEach(function () {
    });

    it('removes no results when applying a * filter', function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");
        
        var wildcardFilter = {tagk:"host",type:"wildcard",filter:"*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host1", tagv_uid: "001"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host2", tagv_uid: "003"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };


        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1, rawTimeSeries2 ], rawTimeSeries);
    });

    it("removes results when applying a wildcard filter which doesn't match all timeseries", function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");
        
        var wildcardFilter = {tagk:"host",type:"wildcard",filter:"host1*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };
        
        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1 ], rawTimeSeries);
    });

    it('removes no results when applying a literal_or filter which matches all timeseries', function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");

        var wildcardFilter = {tagk:"host",type:"literal_or",filter:["host1","host2"],group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host1", tagv_uid: "001"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host2", tagv_uid: "003"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };


        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1, rawTimeSeries2 ], rawTimeSeries);
    });

    it('removes no results when applying an illiteral_or filter which matches all timeseries', function() {
        var postBackendFiltering = faketsdb.__get__("postBackendFiltering");

        var wildcardFilter = {tagk:"host",type:"illiteral_or",filter:["Host1","host2"],group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host1", tagv_uid: "001"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "Host2", tagv_uid: "003"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };


        var rawTimeSeries = [ rawTimeSeries1, rawTimeSeries2 ];
        postBackendFiltering(rawTimeSeries, [wildcardFilter]);
        assert.deepEqual([ rawTimeSeries1, rawTimeSeries2 ], rawTimeSeries);
    });

    it('generates a complete set of tags when there are only group_by filters to apply', function() {
        var constructUniqueTagSetsFromRawResults = faketsdb.__get__("constructUniqueTagSetsFromRawResults");

        // we assume that filters have already been applied, so only really need to worry about grouping
        var literal_or = {tagk:"host",type:"literal_or",filter:["host11","host21"],group_by:true};
        var wildcard = {tagk:"type",type:"wildcard",filter:"type*",group_by:true};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };

        var expectedTagsets = [
            {host:"host11", type: "type1"},
            {host:"host21", type: "type1"},
            {host:"host11", type: "type2"},
            {host:"host21", type: "type2"}
        ];
        var tagsets = constructUniqueTagSetsFromRawResults([rawTimeSeries1, rawTimeSeries2, rawTimeSeries3, rawTimeSeries4], [literal_or, wildcard]);
        assert.deepEqual(tagsets, expectedTagsets);
    });

    it('generates an empty set of tags when there are no group_by filters to apply', function() {
        var constructUniqueTagSetsFromRawResults = faketsdb.__get__("constructUniqueTagSetsFromRawResults");

        // we assume that filters have already been applied, so only really need to worry about grouping
        var literal_or = {tagk:"host",type:"literal_or",filter:["host11","host21"],group_by:false};
        var wildcard = {tagk:"type",type:"wildcard",filter:"type*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };

        var expectedTagsets = [{}];
        var tagsets = constructUniqueTagSetsFromRawResults([rawTimeSeries1, rawTimeSeries2, rawTimeSeries3, rawTimeSeries4], [literal_or, wildcard]);
        assert.deepEqual(tagsets, expectedTagsets);
    });

    it('generates a correct set of tags when there are mixed filters to apply', function() {
        var constructUniqueTagSetsFromRawResults = faketsdb.__get__("constructUniqueTagSetsFromRawResults");
        
        // we assume that filters have already been applied, so only really need to worry about grouping
        var literal_or = {tagk:"host",type:"literal_or",filter:["host11","host21"],group_by:true};
        var wildcard = {tagk:"type",type:"wildcard",filter:"type*",group_by:false};

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        
        var expectedTagsets = [
            {host:"host11"},
            {host:"host21"}
        ];
        var tagsets = constructUniqueTagSetsFromRawResults([rawTimeSeries1, rawTimeSeries2, rawTimeSeries3, rawTimeSeries4], [literal_or, wildcard]);
        assert.deepEqual(tagsets, expectedTagsets);
    });

    it('correctly selects the single timeseries which match the given full tagset', function() {
        var rawTimeSeriesForTagSet = faketsdb.__get__("rawTimeSeriesForTagSet");

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        var inputTs = [
            rawTimeSeries1,
            rawTimeSeries2,
            rawTimeSeries3,
            rawTimeSeries4
        ];
        
        var tagset1 = {
            host: "host11",
            type: "type1"
        };
        var tagset2 = {
            host: "host21",
            type: "type1"
        };
        var tagset3 = {
            host: "host11",
            type: "type2"
        };
        var tagset4 = {
            host: "host21",
            type: "type2"
        };
        
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset1), [rawTimeSeries1]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset2), [rawTimeSeries2]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset3), [rawTimeSeries3]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset4), [rawTimeSeries4]);
    });

    it('correctly selects the timeseries which match the given partial tagset', function() {
        var rawTimeSeriesForTagSet = faketsdb.__get__("rawTimeSeriesForTagSet");

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        var inputTs = [
            rawTimeSeries1,
            rawTimeSeries2,
            rawTimeSeries3,
            rawTimeSeries4
        ];
        
        var tagset1 = {
            host: "host11"
        };
        var tagset2 = {
            type: "type2"
        };
        
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset1), [rawTimeSeries1, rawTimeSeries3]);
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset2), [rawTimeSeries3, rawTimeSeries4]);
    });

    it('correctly selects all the timeseries when given an empty tagset', function() {
        var rawTimeSeriesForTagSet = faketsdb.__get__("rawTimeSeriesForTagSet");

        var rawTimeSeries1 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 1],
                [1519926591, 2],
                [1519926592, 3],
                [1519926593, 4]
            ]
        };
        var rawTimeSeries2 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type1", tagv_uid: "002"}
            },
            dps: [
                [1519926590, 5],
                [1519926591, 6],
                [1519926592, 7],
                [1519926593, 8]
            ]
        };

        var rawTimeSeries3 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host11", tagv_uid: "004"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 9],
                [1519926591, 10],
                [1519926592, 11],
                [1519926593, 12]
            ]
        };
        var rawTimeSeries4 = {
            metric: "metric1",
            tags: {
                "host": {tagk:"host", tagk_uid: "001", tagv: "host21", tagv_uid: "005"},
                "type": {tagk:"type", tagk_uid: "002", tagv: "type2", tagv_uid: "006"}
            },
            dps: [
                [1519926590, 13],
                [1519926591, 14],
                [1519926592, 15],
                [1519926593, 16]
            ]
        };
        var inputTs = [
            rawTimeSeries1,
            rawTimeSeries2,
            rawTimeSeries3,
            rawTimeSeries4
        ];
        
        var tagset1 = {};
        
        assert.deepEqual(rawTimeSeriesForTagSet(inputTs, tagset1), inputTs);
    });
    
    
});