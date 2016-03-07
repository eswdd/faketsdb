var express = require('express');
var bodyParser = require('body-parser');
var router = express.Router();
router.use(bodyParser.json());
require('seedrandom');
var moment = require('moment');

// middleware specific to this router
router.use(function timeLog(req, res, next) {
    console.log(new Date(Date.now())+': '+req.originalUrl);
//    console.log('Time: ', Date.now());
    next();
})

var metrics = [
];

var tagks = [
]

var tagvs = [
];

var uid = function(type, name) {
    var arr = [];
    switch (type) {
        case "metric": arr = metrics; break;
        case "tagk": arr = tagks; break;
        case "tagv": arr = tagvs; break;
        default: throw 'Unsupported type: '+type;
    }
    for (var i=0; i<arr.length; i++) {
        if (arr[i].name == name) {
            return arr[i].uid;
        }
    }
    return null;
}

var assignUidIfNecessary = function(type, name) {
    var arr = [];
    switch (type) {
        case "metric": arr = metrics; break;
        case "tagk": arr = tagks; break;
        case "tagv": arr = tagvs; break;
        default: throw 'Unsupported type: '+type;
    }

    for (var i=0; i<arr.length; i++) {
        if (arr[i].name == name) {
            return; // nothing to do
        }
    }

    var lastUid = arr.length == 0 ? 0 : parseInt(arr[arr.length-1].uid, 16);
    arr.push({name: name, uid: (lastUid+1).toString(16)});


}

var timeseries = [
];


var addTimeSeries = function(metric, tags, type) {
    // see if exists
    for (var i=0; i<timeseries.length; i++) {
        var match = false;
        if (timeseries[i].metric == metric) {
            match = true;
            for (var k1 in tags) {
                if (tags.hasOwnProperty(k1) && timeseries[i].tags.hasOwnProperty(k1)) {
                    if (tags[k1] != timeseries[i].tags[k1]) {
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
    assignUidIfNecessary("metric", metric);
    for (var tagk in tags) {
        if (tags.hasOwnProperty(tagk)) {
            assignUidIfNecessary("tagk", tagk);
            assignUidIfNecessary("tagv", tags[tagk]);
        }
    }

    // now can add
    timeseries.push({metric: metric, tags: tags, type: type});
}

var suggestImpl = function(req, res) {
    var queryParams = req.query;
    if (queryParams["type"] == "metrics") {
        res.json(metrics.map(function(m) {return m.name}));
        return;
    }
    throw 'unhandled response';
}

var aggregatorsImpl = function(req, res) {
    // if add more here then add support in query implementation
    res.json(["avg","sum","min","max"]);
}

var searchLookupImpl = function(metric, limit, useMeta, res) {
    var ret = {
        "type": "LOOKUP",
        "metric": metric,
        "limit": limit,
        "time": 1,
        "results": [],
        "startIndex": 0,
        "totalResults": 0
    };
    for (var i=0; i<timeseries.length; i++) {
        if (timeseries[i].metric == metric) {
            var tsuid = uid("metric", metric);
            for (var k in timeseries[i].tags) {
                if (timeseries[i].tags.hasOwnProperty(k)) {
                    tsuid += uid("tagk", k) + uid("tagv", timeseries[i].tags[k]);
                }
            }
            var ts = {
                metric: metric,
                tags: timeseries[i].tags,
                tsuid: tsuid
            };
            ret.results.push(ts);
        }
    }
    ret.totalResults = ret.results.length;
    res.json(ret);
};
var searchLookupPost = function(req, res) {
    searchLookupImpl(req.body.metric, req.body.limit, req.body.useMeta,res);
}
var searchLookupGet = function(req, res) {
    var queryParams = req.query;
    searchLookupImpl(queryParams["m"],queryParams["limit"],queryParams["use_meta"],res);
}

router.get('/suggest', suggestImpl);
router.get('/aggregators', aggregatorsImpl);
router.post('/aggregators', aggregatorsImpl);
router.get('/search/lookup', searchLookupGet);
router.post('/search/lookup', bodyParser.json(), searchLookupPost);

// todo: replace all these with new implementations..
var allTagValues = function(metric, tagk) {
    var ret = [];
    for (var t=0; t<timeseries.length; t++) {
        if (timeseries[t].metric == metric) {
            if (timeseries[t].tags.hasOwnProperty(tagk)) {
                if (ret.indexOf(timeseries[t].tags[tagk]) < 0) {
                    ret.push(timeseries[t].tags[tagk]);
                }
            }
        }
    }
    return ret;
}

var toDateTime = function(tsdbTime) {
    if (tsdbTime.indexOf("ago")<0) {
        if (tsdbTime.indexOf("/") >= 0) {
            return moment(tsdbTime, "YYYY/MM/DD HH:mm:ss").toDate();
        }
        return new Date(tsdbTime > 10000000000 ? tsdbTime : tsdbTime * 1000);
    }

    if (tsdbTime == null || tsdbTime == "") {
        return new Date();
    }

    tsdbTime = tsdbTime.split("-")[0];
    var numberComponent = tsdbTime.match(/^[0-9]+/);
    var stringComponent = tsdbTime.match(/[a-zA-Z]+$/);
    if (numberComponent.length == 1 && stringComponent.length == 1) {
        return moment().subtract(numberComponent[0], stringComponent[0]).toDate();
    }
    return new Date();
}

var sum = function(arr) {
    var ret = 0;
    for (var i=0; i<arr.length; i++) {
        ret += arr[i];
    }
    return ret;
}

var min = function(arr) {
    var ret = null;
    for (var i=0; i<arr.length; i++) {
        if (!ret) {
            ret = arr[i];
        }
        else {
            ret = Math.min(arr[i], ret);
        }
    }
    return ret;
}

var max = function(arr) {
    var ret = null;
    for (var i=0; i<arr.length; i++) {
        if (!ret) {
            ret = arr[i];
        }
        else {
            ret = Math.max(arr[i], ret);
        }
    }
    return ret;
}

var constructUniqueTagSets = function(tagsAndValueArrays) {
    var ret = [];
    constructUniqueTagSetsInternal(tagsAndValueArrays, 0, ret, {})
    if (ret.length == 0) {
        ret.push({});
    }
    return ret;
}
var constructUniqueTagSetsInternal = function(tagsAndValueArrays, index, ret, curr) {
    if (index >= tagsAndValueArrays.length) {
        var newMap = {};
        for (var k in curr) {
            if (curr.hasOwnProperty(k)) {
                newMap[k] = curr[k];
            }
            ret.push(newMap);
        }
        return;
    }

    for (var v=0; v<tagsAndValueArrays[index].tagv.length; v++) {
        curr[tagsAndValueArrays[index].tagk] = tagsAndValueArrays[index].tagv[v];
        constructUniqueTagSetsInternal(tagsAndValueArrays, index+1, ret, curr);
        curr[tagsAndValueArrays[index].tagk] = null;
    }
}

var queryImpl = function(start, end, mArray, arrays, ms, res) {
    if (!start) {
        res.json("Missing start parameter");
        return;
    }
    console.log("---------------------------");

    var startTime = toDateTime(start);
    var endTime = end ? toDateTime(end) : new Date();
    console.log("start     = "+start);
    console.log("end       = "+(end?end:""));
    console.log("startTime = "+startTime);
    console.log("endTime   = "+endTime);

    var seed = start + (end ? end : "");
    var rand = new Math.seedrandom(seed);

    var ret = [];

    // m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]]}:][<down_sampler>:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
    for (var a=0; a<mArray.length; a++) {
        var m = mArray[a];
        var colonSplit = m.split(":");
        var aggregator = colonSplit[0];
        var rate = false;
        var downsampled = false;
        if (colonSplit[1].indexOf("rate") == 0) {
            rate = true;
            // todo: consider supporting counters?
            if (colonSplit.length == 4) {
                downsampled = colonSplit[2];
            }
        }
        else {
            if (colonSplit.length == 3) {
                downsampled = colonSplit[1];
            }
        }
        var metricAndTags = colonSplit[colonSplit.length-1];
        var metric = metricAndTags;
        var tags = [];
        var openCurly = metricAndTags.indexOf("{");
        var closeCurly = metricAndTags.indexOf("}");
        if (openCurly >= 0 && (closeCurly - openCurly)>1) {
            metric = metricAndTags.substring(0, openCurly);
            var tagString = metricAndTags.substring(openCurly+1);
            tagString = tagString.substring(0, tagString.length-1);

            var tagArray = tagString.split(",");
            for (var t=0; t<tagArray.length; t++) {
                var kv = tagArray[t].split("=");
                if (kv[1].indexOf("*")>=0) {
                    tags.push({tagk:kv[0],tagv:allTagValues(metric, kv[0])});
                }
                else if (kv[1].indexOf("|")>=0) {
                    tags.push({tagk:kv[0],tagv:kv[1].split("|")});
                }
                else {
                    tags.push({tagk:kv[0],tagv:[kv[1]]});
                }
            }
        }

        console.log("Metric: "+metric);
        console.log("  Agg:  "+aggregator);
        console.log("  Rate: "+rate);
        console.log("  Down: "+(downsampled ? downsampled : false));
        console.log("  Tags: "+JSON.stringify(tags));

        var tagsets = constructUniqueTagSets(tags);

        console.log("  Tsets:"+JSON.stringify(tagsets));

        for (var s=0; s<tagsets.length; s++) {
            var aggregateTags = [];

            var participatingTimeSeries = [];
            for (var t=0; t<timeseries.length; t++) {
                if (timeseries[t].metric == metric) {
                    var participating = true;
                    for (var i=0; i<tags.length; i++) {
                        if (timeseries[t].tags.hasOwnProperty(tags[i].tagk)) {
                            var ind = tags[i].tagv.indexOf(timeseries[t].tags[tags[i].tagk]);
                            if (ind < 0) {
                                participating = false;
                                break;
                            }
                        }
                        else {
                            if (aggregateTags.indexOf(tags[i].tagk) < 0) {
                                aggregateTags.push(tags[i].tagk);
                            }
                        }
                    }
                    if (participating) {
                        console.log("    Participant: "+t);
                        participatingTimeSeries.push(timeseries[t]);
                    }
                }
            }

            var downsampleNumberComponent = downsampled ? downsampled.match(/^[0-9]+/) : 10;
            var downsampleStringComponent = downsampled ? downsampled.split("-")[0].match(/[a-zA-Z]+$/)[0] : "s";
            var msMultiplier = ms ? 1000 : 1;
            switch (downsampleStringComponent) {
                case 's': downsampleNumberComponent *= 1 * msMultiplier; break;
                case 'm': downsampleNumberComponent *= 60 * msMultiplier; break;
                case 'h': downsampleNumberComponent *= 3600 * msMultiplier; break;
                case 'd': downsampleNumberComponent *= 86400 * msMultiplier; break;
                case 'w': downsampleNumberComponent *= 7 * 86400 * msMultiplier; break;
                case 'y': downsampleNumberComponent *= 365 * 86400 * msMultiplier; break;
                default: console.log("unrecognized downsample unit: "+downsampleStringComponent);
            }

            var startTimeNormalisedToReturnUnits = ms ? startTime.getTime() : startTime.getTime() / 1000;
            var endTimeNormalisedToReturnUnits = ms ? endTime.getTime() : endTime.getTime() / 1000;
            console.log("normalised startTime      = "+Math.floor(startTimeNormalisedToReturnUnits));
            console.log("downsampleNumberComponent = "+downsampleNumberComponent);

            if (participatingTimeSeries.length > 0) {

                // now generate some data
                var participantData = new Array(participatingTimeSeries.length);
                for (var p=0; p<participatingTimeSeries.length; p++) {
                    participantData[p] = [];
                    // 10% chance of no data at all!
                    if (rand() >= 0.1) {
                        var firstTimeStamp = startTimeNormalisedToReturnUnits % downsampleNumberComponent == 0 ? startTimeNormalisedToReturnUnits :
                            Math.floor((startTimeNormalisedToReturnUnits + downsampleNumberComponent) / downsampleNumberComponent) * downsampleNumberComponent;
                        console.log("Generating data for p="+p+", starting from "+firstTimeStamp);
                        for (var t=firstTimeStamp; t<=endTimeNormalisedToReturnUnits; t+=downsampleNumberComponent) {
                            // 5% chance of missing data point
                            if (rand() >= 0.05) {
                                if (participatingTimeSeries[p].type=="counter" && participantData[p].length>0) {
                                    participantData[p].push([t, participantData[p][participantData[p].length-1][1]+(rand()*100)]);
                                }
                                else {
                                    participantData[p].push([t, rand()*100]);
                                }

                            }
                        }
                    }
                    else {
                        console.log("excluded as within 10%");
                    }
                    console.log("data = "+JSON.stringify(participantData[p]));
                }

                for (var p=participantData.length-1; p>=0; p--) {
                    if (participantData[p].length == 0) {
                        participantData.splice(p, 1);
                        participatingTimeSeries.splice(p, 1);
                    }
                }

                // now combine data as appropriate
                var combinedDps = arrays ? [] : {};
                var indices = new Array(participatingTimeSeries.length);
                for (var i=0; i<indices.length; i++) {
                    indices[i] = 0;
                }
                console.log("    combining "+indices.length+" participating time series");
                for (var t=firstTimeStamp; t<=endTimeNormalisedToReturnUnits; t+=downsampleNumberComponent) {
                    console.log("     t = "+t);
                    var points = [];
                    for (var i=0; i<indices.length; i++) {
                        while (participantData[i][indices[i]][0]<t && indices[i]<participantData[i].length) {
                            indices[i]++;
                        }
                        console.log("     indices["+i+"] = "+JSON.stringify(indices[i]));
                        if (indices[i]<participantData[i].length) {
                            if (participantData[i][indices[i]][0]==t) {
                                console.log("     a");
                                points.push(participantData[i][indices[i]][1]);
                            }
                            else { // next dp time is greater than time desired
                                console.log("     b");
                                // can't interpolate from before beginning
                                if (indices[i]>0) {
                                    console.log("     c");
                                    var gapSizeTime = participantData[i][indices[i]][0] - participantData[i][indices[i]-1][0];
                                    var gapDiff = participantData[i][indices[i]][1] - participantData[i][indices[i]-1][1];

                                    var datumToNow = t - participantData[i][indices[i]-1][0];
                                    var datumToNowRatio = datumToNow / gapSizeTime;

                                    var gapDiffMultRatio = datumToNowRatio * gapDiff;
                                    var newVal = participantData[i][indices[i]-1][1] + gapDiffMultRatio;
                                    points.push(newVal);
                                }

                            }
                        }
                    }
                    console.log("      For time "+t+", partipating points = "+JSON.stringify(points));
                    // now we have our data points, combine them:
                    var val;
                    switch (aggregator) {
                        case "sum":
                            val = sum(points);
                            break;
                        case "avg":
                            val = sum(points)/participantData.length;
                            break;
                        case "min":
                            val = sum(points);
                            break;
                        case "max":
                            val = sum(points);
                            break;
                        default:
                            throw "unrecognized agg: "+aggregator;
                    }
                    if (arrays) {
                        combinedDps.push([t,val]);
                    }
                    else {
                        combinedDps[t] = val;
                    }
                }

                ret.push({
                    "metric": metric,
                    "tags": tagsets[s],
                    "aggregatedTags": aggregateTags,
                    "dps": combinedDps
                });
            }
        }

    }


    res.json(ret);
}
var queryGet = function(req, res) {
    var queryParams = req.query;
    var arrayResponse = queryParams["arrays"] && queryParams["arrays"]=="true";
    var mArray = queryParams["m"];
    mArray = [].concat( mArray );
    queryImpl(queryParams["start"],queryParams["end"],mArray,arrayResponse,false,res);
}

router.get('/query', queryGet);


var installFakeTsdb = function(app) {
    app.use('/api',router);
}

module.exports = {
    addTimeSeries: addTimeSeries,
    install: installFakeTsdb
}

// command line running
if (require.main === module) {
    var config = {
        port: 4242
    };

    var args = process.argv.slice(2);
    for (var i=0; i<args.length; i++) {
        switch (args[i]) {
            case '-p':
                config.port = args[++i];
                break;
            default:
                console.error("Unrecognised option: "+args[i]);
        }
    }

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

    var app = express();
    installFakeTsdb(app);

    var server = app.listen(config.port, function() {
        var host = server.address().address
        var port = server.address().port

        console.log('FakeTSDB running at http://%s:%s', host, port)
    });

}