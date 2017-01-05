/**
 * WebsiteManager : this class is responsible for holding the Website based configurations and other data about manupilating Elements on that website 
 */

var CrawlerClass = require('./crawler');
var async = require('async');
var request = require('request');
var util = require("util");
var events = require("events");

function WebsiteManager(configuration) {
    this.pagesVisited = 0;

    this.cleanUrl = function (url) {
        var x = String(url);
        if (x.charAt(x.length - 1) == '/') {
            return x.substring(0, x.length - 2);
        }
        return x;
    }

    this.startUrl = this.cleanUrl(configuration.startUrl);

    this.name = configuration.name;

    this.selectorArray = configuration.selectorArray;

    this.maxDocuments = configuration.maxDocuments;

    this.Crawler = new CrawlerClass();

    this.finished = false;

    this.initiated = configuration.initiate;
    
    //categoryFilterJson : each Key in this JSON will contain a category that needs to be added to db and the counts in the process. To Maintain Even distribution. 
    //Used to Check if key is less than the thresh hold count.  
    this.categoryFilterJson = {
        //$CategoryName : $count
    };


    this.wireInheritedEvents();
    // if(configuration.initiate){

    //     this.wireInheritedEvents();
    // }
};



WebsiteManager.prototype.eventList = ['CRAWLED_LINKS', 'ERROR', 'FINISHED']

util.inherits(WebsiteManager, events.EventEmitter);



/**
 * wireInheritedEvents : Wires the inherited events from the Crawler object
 */
WebsiteManager.prototype.wireInheritedEvents = function () {
    var mainContext = this;
    this.Crawler.on('CRAWLED_LINKS', function (data) {
        //TODO : Clean the Links Using the Website Manager's Internal functions.

        mainContext.emit('CRAWLED_LINKS', data);
    })
}

WebsiteManager.prototype.pageVisited = function (category) {
    this.pagesVisited++;
    console.log('Page Visited for ' + this.name);
    if (this.pagesVisited >= this.maxDocuments) {
        this.finished = true;
        this.emit('FINISHED');
    }

    if(category){
        if(this.categoryFilterJson[category]){
            this.categoryFilterJson[category]++;
        }else{
            this.categoryFilterJson[category] =1;
        }

        if((this.maxDocuments/Object.keys(this.categoryFilterJson).length) <= this.categoryFilterJson[category]){
            return false //---> Dont store the document
        }else{
            return true;
        }
        
    }

    return false;
}

/**
 * intiateCrawl : If initiate property is mentioned in the constructor then call this function 
 * 
 * This function will take the startUrl and try and feed it to the Crawler which will get the Links  
 */
WebsiteManager.prototype.initiateCrawl = function (callback) {
    var mainContext = this;
    if(this.initiated){
        request(this.startUrl, function (error, response, body) {
            if (error) {
                mainContext.emit('ERROR', { type: 'INITATE_ERROR', err: error });
            } else {
                mainContext.Crawler.collectLinks(body, mainContext.startUrl, mainContext.name);
            }
            callback();
        })
    }
}


WebsiteManager.prototype.configurations = {
    indianExpress: function (url) {
        if(url){
            if ((url.indexOf('/section/cities/') > 0) || (url.indexOf('images.indianexpress.com') >= 0) || (url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
                return false
            }
            if (!(url.indexOf('http://') >= 0)) {
                this.startUrl + url;
            }
            return url;
        }else{
            return false;
        }
    },
    Hindu: function (url) {
        if(url){
            if ((url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
                return false
            }
            return url;
        }else{
            return false;
        }
    },
    NDTV: function (url) {
        if(url){
            if ((url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
                return false
            }
            return url;
        }else{
            return false;
        }
    },
    TOI: function (url) {
        if(url){
            if ((url.indexOf('.gif') >= 0) || (url.indexOf('.png') >= 0) || (url.indexOf('.jpeg') >= 0)) {
                return false
            }
            return url;
        }else{
            return false;
        }
    }
}


module.exports = function (configuration) {
    if (configuration.name && configuration.selectorArray && configuration.maxDocuments && configuration.startUrl) {
        return new WebsiteManager(configuration);
    } else {
        return false;
    }
}