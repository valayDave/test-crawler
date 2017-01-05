var util = require("util");
var cheerio = require('cheerio');
var events = require("events");
var async = require('async');


var Crawler = function () {

}

util.inherits(Crawler, events.EventEmitter);

Crawler.prototype.collectLinks = function (body,url,website) {
    var mainContext = this;
    var $ = cheerio.load(body);
    var relativeLinks = $("body a");
    //console.log("Found " + relativeLinks.length + " relative links on page");
    var pagesToVisit = [];
    relativeLinks.each(function () {
        pagesToVisit.push($(this).attr('href'));
    });
    process.nextTick(function(){
        mainContext.emit('CRAWLED_LINKS', {pagesToVisit:pagesToVisit,website:website,url : url});
    })
}


module.exports = function () {
    return new Crawler();
}