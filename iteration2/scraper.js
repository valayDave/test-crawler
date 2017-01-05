var util = require("util");
var cheerio = require('cheerio');
var events = require("events");
var async = require('async');


var Scraper = function () {

}

util.inherits(Scraper, events.EventEmitter);

Scraper.prototype.collectPageData = function (body,selectorArray,websiteName,url) {
    var mainContext = this;
    if (selectorArray && body) {
        var $ = cheerio.load(body);
        var response = [];
        selectorArray.forEach(function(selectorJson){
            var selectionJson = {};
            for (key in selectorJson) {
                selectionJson[key] = $(selectorJson[key]).text();
            }
            response.push(selectionJson);
        });
        process.nextTick(function(){
            mainContext.emit('SCRAPED_DATA', {textData:response,website:websiteName,url:url});
        })
    } else {
        return false
    }
}


module.exports = function () {
    return new Scraper();
}