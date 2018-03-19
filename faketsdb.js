var express = require('express');
var api = require('nodetsdb-api');
require('seedrandom');

var config = {
    
};

var fakeBackend = {
    metrics: [],
    tagks: [],
    tagvs: [],
    timeseries: []
};

fakeBackend.resetAllState = function() {
    fakeBackend.timeseries = [];
    fakeBackend.metrics = [];
    fakeBackend.tagks = [];
    fakeBackend.tagvs = [];
};

fakeBackend.uidMetaFromName = function(type, name) {
    var arr = [];
    switch (type) {
        case "metric": arr = fakeBackend.metrics; break;
        case "tagk": arr = fakeBackend.tagks; break;
        case "tagv": arr = fakeBackend.tagvs; break;
        default: throw 'Unsupported type: '+type;
    }
    for (var i=0; i<arr.length; i++) {
        if (arr[i].name == name) {
            return arr[i];
        }
    }
    return null;
}

fakeBackend.uidMetaFromUid = function(type, uid) {
    var arr = [];
    switch (type) {
        case "metric": arr = fakeBackend.metrics; break;
        case "tagk": arr = fakeBackend.tagks; break;
        case "tagv": arr = fakeBackend.tagvs; break;
        default: throw 'Unsupported type: '+type;
    }
    for (var i=0; i<arr.length; i++) {
        if (arr[i].uid == uid) {
            return arr[i];
        }
    }
    return null;
}

fakeBackend.assignUidIfNecessary = function(type, name) {
    var arr = [];
    switch (type) {
        case "metric": arr = fakeBackend.metrics; break;
        case "tagk": arr = fakeBackend.tagks; break;
        case "tagv": arr = fakeBackend.tagvs; break;
        default: throw 'Unsupported type: '+type;
    }

    for (var i=0; i<arr.length; i++) {
        if (arr[i].name == name) {
            return arr[i].uid; // nothing to do
        }
    }

    var lastUid = arr.length == 0 ? 0 : parseInt(arr[arr.length-1].uid, 16);
    var nextUid = (lastUid+1).toString(16);
    while (nextUid.length < 6) {
        nextUid = "0" + nextUid;
    }
    arr.push({name: name, uid: nextUid, created: new Date().getTime()/1000});

    return nextUid;
};

fakeBackend.addTimeSeries = function(metric, tags, type, dataConstraints) {
    // see if exists
    for (var i=0; i<fakeBackend.timeseries.length; i++) {
        var match = false;
        if (fakeBackend.timeseries[i].metric == metric) {
            match = true;
            for (var k1 in tags) {
                if (tags.hasOwnProperty(k1) && fakeBackend.timeseries[i].tags.hasOwnProperty(k1)) {
                    if (tags[k1] != fakeBackend.timeseries[i].tags[k1]) {
                        match = false;
                        break;
                    }

                }
            }
            if (match) {
                throw "Metric "+metric+" already exists with tags: "+JSON.stringify(tags);
            }
        }
    }

    // assign uids if needed
    var tsuid = "";
    tsuid += fakeBackend.assignUidIfNecessary("metric", metric);
    for (var tagk in tags) {
        if (tags.hasOwnProperty(tagk)) {
            tsuid += fakeBackend.assignUidIfNecessary("tagk", tagk);
            tsuid += fakeBackend.assignUidIfNecessary("tagv", tags[tagk]);
        }
    }

    if (!dataConstraints) {
        dataConstraints = {};
    }

    // now can add
    fakeBackend.timeseries.push({metric: metric, tags: tags, type: type, constraints: dataConstraints, tsuid: tsuid});
}

fakeBackend.suggestMetrics = function(query) {
    if (!query) {
        return fakeBackend.metrics.map(function(m) {return m.name});
    }
    else {
        var ret = [];
        for (var i=0; i<fakeBackend.metrics.length; i++) {
            if (fakeBackend.metrics[i].name.indexOf(query)==0) {
                ret.push(fakeBackend.metrics[i].name);
            }
        }
        return ret;
    }
};

fakeBackend.searchLookupImpl = function(metric, limit, useMeta) {
    var ret = [];
    for (var i=0; i<fakeBackend.timeseries.length; i++) {
        if (fakeBackend.timeseries[i].metric == metric) {
            var uid = tsuid(metric, fakeBackend.timeseries[i].tags);
            var ts = {
                metric: metric,
                tags: fakeBackend.timeseries[i].tags,
                tsuid: uid
            };
            ret.push(ts);
        }
    }
    return ret;
};

fakeBackend.allTagValues = function(metric, tagk) {
    var ret = [];
    for (var t=0; t<fakeBackend.timeseries.length; t++) {
        if (fakeBackend.timeseries[t].metric == metric) {
            if (fakeBackend.timeseries[t].tags.hasOwnProperty(tagk)) {
                if (ret.indexOf(fakeBackend.timeseries[t].tags[tagk]) < 0) {
                    ret.push(fakeBackend.timeseries[t].tags[tagk]);
                }
            }
        }
    }
    return ret;
};

fakeBackend.performAnnotationsQueries = function(startTime, endTime, downsampleSeconds, ms, participatingTimeSeries) {
    var annotationsArray = [];

    var seed = startTime + (endTime ? endTime : "");
    var rand = new Math.seedrandom(seed);

    var startTimeNormalisedToReturnUnits = ms ? startTime.getTime() : startTime.getTime() / 1000;
    var endTimeNormalisedToReturnUnits = ms ? endTime.getTime() : endTime.getTime() / 1000;

    var firstTimeStamp = (startTimeNormalisedToReturnUnits % downsampleSeconds) == 0 ? startTimeNormalisedToReturnUnits :
        Math.floor((startTimeNormalisedToReturnUnits + downsampleSeconds) / downsampleSeconds) * downsampleSeconds;

    for (var t=firstTimeStamp; t<=endTimeNormalisedToReturnUnits; t+=downsampleSeconds) {
        // chance of inserting an annotation where there's no data point
        if (rand() <= config.probabilities.annotation) {
            var uid = participatingTimeSeries[p].ts_uid;
            var ann = {
                "tsuid": uid,
                "description": "Testing Annotations",
                "notes": "These would be details about the event, the description is just a summary",
                "custom": {
                    "owner": "jdoe",
                    "dept": "ops"
                },
                "endTime": 0,
                "startTime": t
            };
            annotationsArray.push(ann);
        }
    }

    return annotationsArray;
}
fakeBackend.performGlobalAnnotationsQuery = function(startTime, endTime) {
    var seed = startTime + (endTime ? endTime : "");
    var rand = new Math.seedrandom(seed);

    var globalAnnotationsArray = [];
    // populate some global annotations
    var from = startTime.getTime();
    var to = endTime.getTime();
    for (var t=from; t<to; ) {
        if (rand() <= config.probabilities.globalAnnotation) {
            var ann = {
                "description": "Notice",
                "notes": "DAL was down during this period",
                "custom": null,
                "endTime": t+((to-from)/20),
                "startTime": t
            };
            globalAnnotationsArray.push(ann);
        }

        // next time
        var inc = rand() * ((to-from)/3);
        inc += "";
        if (inc.indexOf(".") > -1) {
            inc = inc.substring(0, inc.indexOf("."));
        }

        t += parseInt(inc);
    }
    return globalAnnotationsArray;
}

/**
 * Loads time series data for the given query, applies pre-query filtering where possible
 * @param startTime DateTime
 * @param endTime DateTime
 * @param ms Boolean
 * @param downsample String
 * @param metric String
 * @param filters Array of {tagk:String,type:String,filter:[String],group_by:Boolean}
 * @returns Array of {
 *                     metric:String,
 *                     metric_uid:String,
 *                     tags: { tagk: { tagk:String, tagk_uid:String, tagv:String, tagv_uid:String} }
 *                     dps: [ [ timestamp:Number, value:Number ] ]
 *                   }
 */
fakeBackend.performBackendQueries = function(startTime, endTime, ms, downsample, metric, filters) {
    /*
    var rawTimeSeries = [
        {
            metric: "metric",
            metric_uid: "001",
            ts_uid: "001001001",
            tags: {
                "tag1": { tagk: "tag1", tagk_uid: "001", tagv: "value1", tagv_uid: "001" }
            },
            dps: [
                [ 1002020020, 1.4 ],
                [ 1002020021, 1.5 ]
            ]
        }
    ];*/


    /*
     addTimeSeries("tsd.rpc.received", { host: "host01", type: "put" }, "counter");
     addTimeSeries("tsd.rpc.received", { host: "host01", type: "telnet" }, "counter");
     addTimeSeries("tsd.rpc.errors", { host: "host01", type: "invalid_values" }, "counter");
     addTimeSeries("tsd.rpc.errors", { host: "host01", type: "hbase_errors" }, "counter");
     addTimeSeries("tsd.rpc.errors", { host: "host01", type: "illegal_arguments" }, "counter");
     addTimeSeries("ifstat.bytes", { host: "host01", direction: "in" }, "counter");
     addTimeSeries("ifstat.bytes", { host: "host01", direction: "out" }, "counter");
     addTimeSeries("ifstat.bytes", { host: "host02", direction: "in" }, "counter");
     addTimeSeries("ifstat.bytes", { host: "host02", direction: "out" }, "counter");
     addTimeSeries("cpu.percent", { host: "host01" }, "gauge");
     addTimeSeries("cpu.percent", { host: "host02" }, "gauge");
     addTimeSeries("cpu.queue", { host: "host01" }, "gauge");
     addTimeSeries("cpu.queue", { host: "host02" }, "gauge");
     */

    var seed = startTime + (endTime ? endTime : "");
    var rand = new Math.seedrandom(seed);

    var downsampleNumberComponent = downsample ? downsample.match(/^[0-9]+/) : 10;
    var downsampleStringComponent = downsample ? downsample.split("-")[0].match(/[a-zA-Z]+$/)[0] : "s";
    var msMultiplier = ms ? 1000 : 1;
    switch (downsampleStringComponent) {
        case 's': downsampleNumberComponent *= 1 * msMultiplier; break;
        case 'm': downsampleNumberComponent *= 60 * msMultiplier; break;
        case 'h': downsampleNumberComponent *= 3600 * msMultiplier; break;
        case 'd': downsampleNumberComponent *= 86400 * msMultiplier; break;
        case 'w': downsampleNumberComponent *= 7 * 86400 * msMultiplier; break;
        case 'y': downsampleNumberComponent *= 365 * 86400 * msMultiplier; break;
        default:
            if (config.verbose) {
                console.log("unrecognized downsample unit: "+downsampleStringComponent);
            }
    }


    var startTimeNormalisedToReturnUnits = ms ? startTime.getTime() : startTime.getTime() / 1000;
    var endTimeNormalisedToReturnUnits = ms ? endTime.getTime() : endTime.getTime() / 1000;

    var participatingTimeSeries = [];
    for (var p=0; p<fakeBackend.timeseries.length; p++) {
        if (fakeBackend.timeseries[p].metric == metric) {
            if (config.verbose) {
                console.log("    Participant: "+t);
            }
            var metric_uid = uid("metric", metric);
            var ts_uid = metric_uid;
            var tags = {};
            for (var k in fakeBackend.timeseries[p].tags) {
                if (fakeBackend.timeseries[p].tags.hasOwnProperty(k)) {
                    var v = fakeBackend.timeseries[p].tags[k];
                    var tag = { tagk: k, tagk_uid: uid("tagk", k), tagv: v, tagv_uid: uid("tagv", v) };
                    ts_uid += tag.tagk_uid + tag.tagv_uid;
                    tags[k] = tag;
                }
            }
            var dps = [];

            // chance of no data at all!
            if (rand() >= config.probabilities.noData) {
                var firstTimeStamp = startTimeNormalisedToReturnUnits % downsampleNumberComponent == 0 ? startTimeNormalisedToReturnUnits :
                    Math.floor((startTimeNormalisedToReturnUnits + downsampleNumberComponent) / downsampleNumberComponent) * downsampleNumberComponent;

                if (config.verbose) {
                    console.log("Generating data for p="+p+", starting from "+firstTimeStamp);
                }

                for (var t=firstTimeStamp; t<=endTimeNormalisedToReturnUnits; t+=downsampleNumberComponent) {
                    // chance of missing data point
                    if (rand() >= config.probabilities.missingPoint) {
                        var prevValue = dps.length>0 ? dps[dps.length-1][1] : 0;
                        var newValue = 0;
                        switch (fakeBackend.timeseries[p].type) {
                            case "counter":
                                newValue = prevValue+(rand()*100);
                                break;
                            case "gauge":
                                var inc = (rand()-0.5)*20;
                                newValue = prevValue + inc;
                                if (fakeBackend.timeseries[p].constraints.hasOwnProperty("min")) {
                                    newValue = Math.max(newValue, fakeBackend.timeseries[p].constraints.min);
                                }
                                if (fakeBackend.timeseries[p].constraints.hasOwnProperty("max")) {
                                    newValue = Math.min(newValue, fakeBackend.timeseries[p].constraints.max);
                                }
                                break;
                        }
                        dps.push([t, newValue]);

                    }
                    else if (config.verbose) {
                        console.log("Missing datapoint at "+t);
                    }
                }
            }
            else {
                if (config.verbose) {
                    console.log("excluded as within 10%");
                }
            }
            var ts = {
                ts_uid: ts_uid,
                metric: metric,
                metric_uid: metric_uid,
                tags: tags,
                dps: dps
            };
            participatingTimeSeries.push(ts);
        }
    }

    // uidMetaFromName

    return participatingTimeSeries;
}

var backend = fakeBackend;
// public interface:
//   uidMetaFromName = function(type, name)
//   backend.uidMetaFromUid(queryParams["type"], queryParams["uid"]);
//   backend.searchLookupImpl(req.body.metric, req.body.limit, req.body.useMeta);
//   backend.performBackendQueries(startTime, endTime, ms, downsampled, metric, filters);
//   backend.performAnnotationsQueries(startTime, endTime, participatingTimeSeries);

var uid = function(type, name) {
    var meta = backend.uidMetaFromName(type, name);
    if (meta != null) {
        return meta.uid;
    }
    return null;
};

var tsuid = function(metric, tags) {
    var ret = uid("metric", metric);
    for (var k in tags) {
        if (tags.hasOwnProperty(k)) {
            ret += uid("tagk", k) + uid("tagv", tags[k]);
        }
    }
    return ret;
};

var applyOverrides = function(from, to) {
    for (var k in from) {
        if (from.hasOwnProperty(k)) {
            if (to.hasOwnProperty(k)) {
                switch (typeof from[k]) {
                    case 'number':
                    case 'string':
                    case 'boolean':
                        to[k] = from[k];
                        continue;
                    default:
                        console.log("unhandled: "+(typeof from[k]));
                }
                applyOverrides(from[k], to[k]);
            }
            else {
                to[k] = from[k];
            }
        }
    }
}

var installFakeTsdb = function(app, incomingConfig) {
    if (!incomingConfig) {
        incomingConfig = {};
    }

    var conf = {
        verbose: false,
        logRequests: true,
        probabilities: {
            noData: 0.01,
            missingPoint: 0.05,
            annotation: 0.005,
            globalAnnotation: 0.5
        },
        version: "2.2.0"
    };

    applyOverrides(incomingConfig, conf);

    config = conf;

    api.backend(fakeBackend);
    api.install(app, config);
}

module.exports = {
    addTimeSeries: fakeBackend.addTimeSeries,
    install: installFakeTsdb,
    reset: fakeBackend.resetAllState
}

// command line running
if (require.main === module) {
    var conf = {
        port: 4242
    };

    var args = process.argv.slice(2);
    for (var i=0; i<args.length; i++) {
        switch (args[i]) {
            case '-p':
                conf.port = args[++i];
                break;
            case '-v':
                conf.verbose = true;
                break;
            case '-?':
            case '--help':
                console.log("Usage: node faketsdb.js [options]");
                console.log(" -p [port] : Specify the port to bind to")
                console.log(" -v        : Verbose logging")
                console.log(" -? --help : Show this help page")
                break;
            default:
                console.error("Unrecognised option: "+args[i]);
        }
    }

    fakeBackend.addTimeSeries("tsd.rpc.received", { host: "host01", type: "put" }, "counter");
    fakeBackend.addTimeSeries("tsd.rpc.received", { host: "host01", type: "telnet" }, "counter");
    fakeBackend.addTimeSeries("tsd.rpc.errors", { host: "host01", type: "invalid_values" }, "counter");
    fakeBackend.addTimeSeries("tsd.rpc.errors", { host: "host01", type: "hbase_errors" }, "counter");
    fakeBackend.addTimeSeries("tsd.rpc.errors", { host: "host01", type: "illegal_arguments" }, "counter");
    fakeBackend.addTimeSeries("ifstat.bytes", { host: "host01", direction: "in" }, "counter");
    fakeBackend.addTimeSeries("ifstat.bytes", { host: "host01", direction: "out" }, "counter");
    fakeBackend.addTimeSeries("ifstat.bytes", { host: "host02", direction: "in" }, "counter");
    fakeBackend.addTimeSeries("ifstat.bytes", { host: "host02", direction: "out" }, "counter");
    fakeBackend.addTimeSeries("cpu.percent", { host: "host01" }, "gauge");
    fakeBackend.addTimeSeries("cpu.percent", { host: "host02" }, "gauge");
    fakeBackend.addTimeSeries("cpu.queue", { host: "host01" }, "gauge");
    fakeBackend.addTimeSeries("cpu.queue", { host: "host02" }, "gauge");

    var app = express();
    installFakeTsdb(app, conf);

    var server = app.listen(config.port, function() {
        var host = server.address().address
        var port = server.address().port

        console.log('FakeTSDB running at http://%s:%s', host, port)
    });

}