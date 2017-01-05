/**
 * CrawlManager : the function of this object is to mange the crawling to the sites and then pass along the data to the appropriate Queues. 
 * 
 * The Manager has a consume function which will consume the URL Fed to it. 
 */
var async = require('async');
var events = require("events");
var util = require("util");
var CrawlerClass = require('./crawler');
var ScraperClass = require('./scraper');
var request = require('request');

function CrawlManager() {
    var mainContext = this

    //crawler : this object will be an instance of the Crawler Class
    this.crawler = new CrawlerClass();

    //scraper : this object will be an instance of the Scraper Class
    this.scraper = new ScraperClass();

    this.wireInheritedEvents();

    this.pagesToVisit = []; // Buffer for the temporary pages that get found before they get flushed
    this.crawledPages = {};//Contained all the possible crawled URLS
    
}

util.inherits(CrawlManager, events.EventEmitter);

/**
 * consume : Consumes the URL Crawling and Scraping. Crawler Emits its own events and so does the scraper. These Events can Be caught in the main Thread for 
 * further processing. 
 * 
 * @crawlerMeta : {url : '', selectorArray : [{body:'selector',category:'selector',title:'selector'}],websiteName:''}
 */
CrawlManager.prototype.consume = function (crawlerMeta,callback) {
    var mainContext = this;
    if (!crawlerMeta.url || !crawlerMeta.selectorArray) {
        return callback();
    }
    request(crawlerMeta.url,function(error,response,body){
        mainContext.crawledPages[crawlerMeta.url] = true;
        if(error){
            mainContext.emit('ERROR',{type:'CRAWL_ERROR',err:error});
            callback();
        }else{
            mainContext.crawler.collectLinks(body,crawlerMeta.url,crawlerMeta.websiteName);
            mainContext.scraper.collectPageData(body,crawlerMeta.selectorArray,crawlerMeta.websiteName,crawlerMeta.url);
            callback();
        }
    });
}

/**
 * wireInheritedEvents : Inherits the Events from the scraper and the emits them from this common source.
 */
CrawlManager.prototype.wireInheritedEvents = function () {
    var mainContext = this;
    //Wiring events so that they are emitted by the main Object. 
    this.scraper.on('SCRAPED_DATA', function (data) {
        //Processing of the Data can happen here. 
        mainContext.emit('DATA', data);
    });

    //Wiring events so that they are emitted by the main Object. 
    this.crawler.on('CRAWLED_LINKS', function (data) {
        //Further Processing of the Links can happen here. 
        //Check if the links are present in the Already Crawled Pages 
        if(data.pagesToVisit && data.website){
            var pagesToSend =[];
            data.pagesToVisit.forEach(function(url){
                if(!mainContext.crawledPages[url]){
                    pagesToSend.push(url);
                    mainContext.crawledPages[url] = true;
                }
            });
            mainContext.emit('CRAWLED_LINKS', {pagesToVisit:pagesToSend,website:data.website});
        }
    });

}

module.exports = function (crawler, scraper) {
    return new CrawlManager(crawler, scraper);
}

