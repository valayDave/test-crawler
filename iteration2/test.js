var RabbitMq = require('./rabbit.utils');
var mongoose = require('mongoose');
mongoose.connect('mongodb://127.0.0.1:27017/data-Crawler');
var CrawlManagerClass = require('./crawl.manager');
var CrawlQueueManager = new CrawlManagerClass();
var WebsiteManagerClass = require('./website.manager');
var WebsiteManagerArr = [];
var QUEUE_CONSUMERS = 4;
var totalPagesScraped = 0;
var MAX_QUEUE_BUFFER_LENGTH = 20000;
var QUEUE_BUFFER_LENGTH = 0;
var async = require('async');
var QueueName = 'CRAWLER_QUEUE';

var websiteMetaArray = [
    {
        startUrl: 'http://indianexpress.com',
        selectorArray: [
            {
                body: '.articles p',
                title: '.heading-part h1',
                category: '.breadcrumb .first span'
            }
        ],
        maxDocuments: 10000,
        name: "indianExpress",
        initiate: true
    },
    {
        startUrl: 'http://www.thehindu.com',
        selectorArray: [
            {
                body: '.article p',
                title: 'h1',
                category: '.breadcrumb li:nth-child(2)'
            }
        ],
        maxDocuments: 10000,
        name: "Hindu",
        initiate: true
    },
];

var Schema = mongoose.Schema;
var crawlCollectionSchema = new Schema({
    //_id: Schema.Types.ObjectId,
    website: String,
    data: {
        body: String,
        category: String,
        title: String
    },
    url: String,
    createdOn: {
        type: Date,
        default: Date.now
    }
});

var DATE_HR_STRING = new Date().getDate() + '-' + new Date().getMonth() + '-' + new Date().getFullYear() + ":" + new Date().getHours(); //Add the hour string;
var collectionName = 'crawlCollection-' + DATE_HR_STRING;
var crawlModel = mongoose.model(collectionName, crawlCollectionSchema, collectionName);
/**---------------------------------------------------------CORE QUEUE MANAGEMENT CODE BEGINS--------------------------------------------------------------------------------------------- */
/**
 * data : {pagesToVisit:[],website : ''}
 */
CrawlQueueManager.on('CRAWLED_LINKS', function (data) {
    //Clean the URLS here so that They can be pushed into the Queue.

    if (data.website && data.pagesToVisit) {
        //TODO : Check from Database or Anystore if that URL has been crawled In the Past Otherwise do the below operation. 
        var queueObjects = [];
        var WebsiteManager = WebsiteManagerArr.find(function (WebsiteManager) {
            return WebsiteManager.name == data.website
        });
        if (!WebsiteManager.finished) {
            data.pagesToVisit.forEach(function (url) {
                //Clean the URL and only if it is valid add it to the queue
                var filter = WebsiteManager.configurations[WebsiteManager.name];
                url = filter.call(WebsiteManager, url);
                if (url)
                    queueObjects.push({ url: url, selectorArray: WebsiteManager.selectorArray, websiteName: WebsiteManager.name });
            });
            if (queueObjects.length > 0) {
                if (QUEUE_BUFFER_LENGTH + queueObjects.length < MAX_QUEUE_BUFFER_LENGTH) {
                    async.each(queueObjects, function (queueObject, iteratorCallback) {
                        QUEUE_BUFFER_LENGTH++;
                        return RabbitMq.push(QueueName, queueObject, true, 1, iteratorCallback);
                    }, function (err) {
                        //console.log('============================== Added ' + queueObjects.length + ' Objects to the QUEUE For Website : ' + data.website + '==============================')
                    })
                } else {
                    console.log('Waiting For Queue To Drain. To Push more Items.');
                }
            }
        } else {
            //Website's Crawling has been done. Dont add any more links from that Site. 
        }

    } else {
        //TODO : Bug handle
    }
});

/**
 * data : {textData:[],website : ''}
 */
CrawlQueueManager.on('DATA', function (data) {
    if (data.textData && data.website && data.url) {
        QUEUE_BUFFER_LENGTH--;
        if (totalPagesScraped++ % 10 == 0)
            console.log('==================== ' + totalPagesScraped + " Pages Scraped=========================")
        var WebsiteManager = WebsiteManagerArr.find(function (WebsiteManager) {
            return WebsiteManager.name == data.website;
        });
        var storeData = null;
        data.textData.forEach(function (textObj) {
            if ((textObj.body != '') && (textObj.category != '') && (textObj.title != '') && !WebsiteManager.finished) {
                var keys = Object.keys(textObj);
                keys.forEach(function (key) {
                    if (storeData == null) {
                        storeData = {};
                    }
                    storeData[key] = new String(textObj[key]).trim();
                })
            }
        });
        if (storeData != null) {
            if (storeData.category)
                if (WebsiteManager.pageVisited(storeData.category)) {
                    //TODO : Store this into Mongo Or some store like that.
                    var crawledDoc = new crawlModel({ website: data.website, data: storeData, url: data.url });
                    crawledDoc.save(function (err) {
                        if (!err) {
                            console.log('Document Saved');
                        }
                    })

                }
        }
    }
});




//This will initiate the Website 
websiteMetaArray.forEach(function (websiteJson) {
    var WebsiteManager = new WebsiteManagerClass(websiteJson);
    WebsiteManager.on('CRAWLED_LINKS', function (data) {
        var WebsiteManagerContext = this;
        var queueObjects = [];
        //Push the Links that have been crawled into Queue Here. 
        data.pagesToVisit.forEach(function (url) {
            var filter = WebsiteManagerContext.configurations[WebsiteManagerContext.name].bind(WebsiteManagerContext);
            url = filter(url);
            if (url)
                queueObjects.push({ url: url, selectorArray: WebsiteManagerContext.selectorArray, websiteName: WebsiteManagerContext.name });
        });

        async.each(queueObjects, function (queueObject, iteratorCallback) {
            return RabbitMq.push(QueueName, queueObject, true, 1, iteratorCallback);
        }, function (err) {
            console.log('============================== Added ' + queueObjects.length + ' Objects to the QUEUE For Website : ' + WebsiteManagerContext.name + '==============================')
        }.bind(WebsiteManager))

    });
    WebsiteManagerArr.push(WebsiteManager);
});


// RabbitMq.connect(function(err){
//     console.log(err);
//     console.log('CrawlQueueManager Subscribing to Queue')
// })
for (var i = 0; i < QUEUE_CONSUMERS; i++) {
    RabbitMq.subscribe(QueueName, 'JSON', function (message, callback) {
        var mC = this;
        CrawlQueueManager.consume.call(CrawlQueueManager, message, function (err) {
            //console.log('ACK Coming from '+ mC.index);
            callback();
        })
    }.bind({ index: i }));
}


console.log('Intiating the Crawl For ')
// WebsiteManagerArr.forEach(function (WebsiteManager) {
//     WebsiteManager.initiateCrawl();
// })
//CrawlManager.consume(testUrl);

async.each(WebsiteManagerArr, function (WebsiteManager, iteratorCallback) {
    WebsiteManager.initiateCrawl(iteratorCallback);
}, function (err) {
    console.log('Crawl Initiated.');
})


/**---------------------------------------------------------CORE QUEUE MANAGEMENT CODE ENDS--------------------------------------------------------------------------------------------- */