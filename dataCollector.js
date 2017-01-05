var request = require('request');

var URL = require('url-parse');
var util = require("util");
var cheerio = require('cheerio');
var events = require("events");
var async = require('async');


function Crawler(siteName, startURL, selectorJson, maxPages) {
    this.pagesVisited = 0; //Counter till we reach Max Pages
    this.siteName = siteName;//Name of the site
    this.pagesToVisit = []; // Buffer for the temporary pages that get found before they get flushed
    this.crawledPages = {};//Contained all the possible crawled URLS
    this.START_URL = startURL;
    this.MAX_PAGES_TO_VISIT = maxPages;
    this.waitingBuffers = 0;
    this.selectorJson = selectorJson;
    this.baseUrl = new URL(startURL).hostname;

    //For Buffers Holding URLs to be processed in Parallel
    this.parallelCrawlBufferSize = 30;
    this.parallelBuffers = [];

    //A Limit to the max number of Pages that will be used for further Crawl
    this.PAGE_BUFFER_LIMIT = 1000;

}

util.inherits(Crawler, events.EventEmitter);
/**
 * initiate : this function will start the crawlling process. 
 */
Crawler.prototype.initiate = function (setupBool) {
    for (var event in this.internalEvents) {
        this.on(event, this.internalEvents[event].bind(this));
    }

    if (setupBool)
        this.emit('CRAWL_SETUP');
}

/**
 * arrayChunkify : splits an array into chucks of different sizes.
 */
Crawler.prototype.arrayChunkify = function (a, n, balanced) {

    if (n < 2)
        return [a];

    var len = a.length,
        out = [],
        i = 0,
        size;

    if (len % n === 0) {
        size = Math.floor(len / n);
        while (i < len) {
            out.push(a.slice(i, i += size));
        }
    }

    else if (balanced) {
        while (i < len) {
            size = Math.ceil((len - i) / n--);
            out.push(a.slice(i, i += size));
        }
    }

    else {

        n--;
        size = Math.floor(len / n);
        if (len % size === 0)
            size--;
        while (i < size * n) {
            out.push(a.slice(i, i += size));
        }
        out.push(a.slice(size * n));

    }

    return out;
}

Crawler.prototype.singleBufferConsumer = function (url, callback) {
    var mainContext = this;
    if (mainContext.pagesVisited <= mainContext.MAX_PAGES_TO_VISIT) {
        //console.log('Crawling URL : ' + url);
        mainContext.visitPage(url, function (err, responseJson) {
            if (responseJson) {
                mainContext.emit('PAGE_CRAWLED', url, responseJson);
            }
            callback();
        })
    } else {
        callback();
    }
}


Crawler.prototype.bulkBufferConsumer = function (urlArr, callback) {
    var mainContext = this;
    console.log('==========Crawling For a Batch of :' + urlArr.length + " ==========================")
    async.each(urlArr, function (url, parallelCallback) {
        if (mainContext.pagesVisited <= mainContext.MAX_PAGES_TO_VISIT) {
            //console.log('Crawling URL : ' + url);
            mainContext.visitPage(url, function (err, responseJson) {
                if (responseJson) {
                    mainContext.emit('PAGE_CRAWLED', url, responseJson);
                }
                parallelCallback();
            })

        } else {
            //URL Has been Visited Before. 
            console.log('URL has been Visited Before');
            parallelCallback();
        }
    }, function (err) {
        mainContext.waitingBuffers--;
        console.log('============================= CRAWL BATCH COMPLETED AT : ' + new Date() + " =============================");
        if (mainContext.MAX_PAGES_TO_VISIT <= mainContext.pagesVisited) {
            mainContext.emit('CRAWL_COMPLETE');
            callback();
        } else {
            if (mainContext.waitingBuffers == 0 && mainContext.pagesToVisit.length > 0) {
                mainContext.resizeBuffer();
            }
            callback()
        }
    });
}


Crawler.prototype.internalEvents = {
    CRAWL_SETUP: function () {
        var mainContext = this;
        this.visitPage(this.START_URL, function (err, responseJson) {
            //mainContext.emit('PAGE_CRAWLED', this.START_URL, responseJson);
            for (var index in responseJson.internalLinks) {
                //console.log( this.pagesToVisit);
                if (mainContext.pagesToVisit.indexOf(responseJson.internalLinks[index]) < 0 && responseJson.internalLinks[index]) {
                    if (responseJson.internalLinks[index].indexOf(mainContext.baseUrl) >= 0 && mainContext.pageRules[mainContext.siteName](responseJson.internalLinks[index])) {
                        mainContext.crawledPages[responseJson.internalLinks[index]] = true;
                        mainContext.pagesToVisit.push(responseJson.internalLinks[index]);
                    }
                }
            }
            console.log('==================== Crawler Being setup & BUFFER BEING RESIZED =======================');
            mainContext.resizeBuffer();
            //split the this.pagesToVisit into arrays into sub arrays and only put data into subarrays
        })
    },

    CRAWL_COMPLETE: function (responseJson) {
        console.log('removing Listeners');
        this.removeListener('PAGE_CRAWLED', this.internalEvents.PAGE_CRAWLED.bind(this));
        //this.removeListener('CRAWL_START', this.internalEvents.CRAWL_START.bind(this));
    },

    PAGE_CRAWLED: function (url, responseJson) {
        var mainContext = this;
        //TODO : Put code here that will recrawl the pages Extracted in the
        var newPageToCrawl = null
        //console.log(responseJson.textData);
        //console.log(url);
        if ((responseJson.textData) && (responseJson.textData.body != '' && responseJson.textData.body) && (responseJson.textData.title != '' && responseJson.textData.title) && (responseJson.textData.category != '' && responseJson.textData.category) && (this.pagesVisited <= this.MAX_PAGES_TO_VISIT)) {
            console.log('Pages Visited Being incremented to ' + this.pagesVisited);
            this.pagesVisited++;
            var pageJson = {};
            pageJson.data = responseJson.textData;
            pageJson.url = url;
            mainContext.emit('CRAWLED_DATA', responseJson, url, mainContext.baseUrl);

        }
        
        if ((this.pagesVisited <= this.MAX_PAGES_TO_VISIT)) {
            for (var index in responseJson.internalLinks) {
                // console.log( responseJson.internalLinks[index]);
                if (this.pagesToVisit.indexOf(responseJson.internalLinks[index]) < 0 && responseJson.internalLinks[index]) {
                    if (responseJson.internalLinks[index].indexOf(this.baseUrl) >= 0 && this.pageRules[this.siteName](responseJson.internalLinks[index]) && !this.crawledPages[responseJson.internalLinks[index]]) {
                        this.crawledPages[responseJson.internalLinks[index]] = true;
                        this.pagesToVisit.push(responseJson.internalLinks[index]);
                        if (this.pagesToVisit.length > this.PAGE_BUFFER_LIMIT) {
                            this.resizeBuffer();
                        }
                    }

                }
            }
        }


        if (!(this.pagesVisited <= this.MAX_PAGES_TO_VISIT)) {
            this.emit('CRAWL_COMPLETE');
        }


    }
}


Crawler.prototype.resizeBuffer = function () {
    var mainContext = this;
    //console.log(this);
    var buffers = Math.ceil(this.pagesToVisit.length / this.parallelCrawlBufferSize);
    console.log('Crawling Started By Creating ' + buffers + ' Buffers of ' + this.parallelCrawlBufferSize + ' Sizes');
    var parallelBuffers = this.arrayChunkify(this.pagesToVisit, buffers, true);
    this.pagesToVisit = [];
    this.waitingBuffers += parallelBuffers.length;
    //var response = {buffers : parallelBuffers};
    this.emit('BUFFER_CLEARED', parallelBuffers);
}

Crawler.prototype.pageRules = {
    indianExpress: function (url) {
        if ((url.indexOf('/section/cities/') > 0) || (url.indexOf('images.indianexpress.com') >= 0) || (url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
            return false
        }
        return true;
    },

    Hindu: function (url) {
        if ((url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
            return false
        }
        return true;
    },
    NDTV: function (url) {
        if ((url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
            return false
        }
        return true;
    },
    TOI: function (url) {
        if ((url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
            return false
        }
        return true;
    }
}


/**
 * visitPage : this function will crawl the page. Use the selector JSON which is a property given at the start and callback that data from the page. It will also pass the related links from that page. 
 */
Crawler.prototype.visitPage = function (url, callback) {
    var mainContext = this;
    request(url, function (error, response, body) {
        //console.log(error);
        // Check status code (200 is HTTP OK)
        if (error) {
            callback(null, { textData: {}, internalLinks: [] })
        } else {
            // console.log("Status code: " + response.statusCode);

            if (response.statusCode !== 200) {
                callback(error);
                return;
            }
            // Parse the document body
            var $ = cheerio.load(body);
            var responseJson = {
                textData: {}
            };
            for (key in mainContext.selectorJson) {
                responseJson.textData[key] = $(mainContext.selectorJson[key]).text();
            }
            responseJson.internalLinks = mainContext.collectInternalLinks($);
            callback(error, responseJson);
        }

    });
}


/**
 * collectInternalLinks : Collects the internal links for a page using the cheerio Object and returns the array.
 */
Crawler.prototype.collectInternalLinks = function ($) {
    var relativeLinks = $("body a");
    //console.log("Found " + relativeLinks.length + " relative links on page");
    var pagesToVisit = [];
    relativeLinks.each(function () {
        pagesToVisit.push($(this).attr('href'));
    });
    return pagesToVisit;
}


module.exports = function (siteName, startURL, selectorJson, maxPages) {
    return new Crawler(siteName, startURL, selectorJson, maxPages);
}
