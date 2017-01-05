var mongoose = require('mongoose');


var DataCollectionObjects = [
    // {
    //     selectorJson : {
    //         body: '.ins_storybody',
    //         title: 'h1[itemprop="headline"]',
    //         category: '.breadcrums span:nth-child(2) a span',
    //     },
    //     startURL: 'http://www.ndtv.com/',
    //     maxPages: 2000,
    //     name: "NDTV"
    // },
    {
        selectorJson : {
            body: '.articles p',
            title: '.heading-part h1',
            category: '.breadcrumb .first span'
        },
        startURL: 'http://indianexpress.com/',
        maxPages: 10000,
        name: "indianExpress"
    },
    // {
    //     selectorJson : {
    //         body: '.article',
    //         title: 'h1.title',
    //         category: '.breadcrumb li:nth-child(2)',
    //     },
    //     startURL: 'http://www.thehindu.com/',
    //     maxPages: 2000,
    //     name: "Hindu"
    // },

    // {
    //     selectorJson : {
    //         body: '.Normal',
    //         title: 'h1[itemprop="headline"]',
    //         category: 'li.current',
    //     },
    //     startURL: 'http://timesofindia.indiatimes.com/',
    //     maxPages: 2000,
    //     name: "TOI"
    // }

]


var CrawlerClass = require('./dataCollector');
mongoose.connect('mongodb://127.0.0.1:27017/data-Crawler');
var RabbitMq = require('./rabbit.utils');
var async = require('async');
/**
 * The archeitecture : 
 * 
 *  There is one Master Crawl Object That controls that needs to be in
 */




var Schema = mongoose.Schema;

var DATE_HR_STRING = new Date().getDate() + '-' + new Date().getMonth() + '-'+new Date().getFullYear() + ":"+new Date().getHours(); //Add the hour string;
var crawlCollectionSchema = new Schema({
	//_id: Schema.Types.ObjectId,
	website : String,
    data : {
        body : String ,
        category : String ,
        title : String 
    },
    url : String,
    createdOn : {
        type : Date,
        default : Date.now
    }
})

var collectionName = 'actualCrawlCollection2-'+DATE_HR_STRING;
var crawlModel = mongoose.model(collectionName, crawlCollectionSchema, collectionName);


var CrawlerObjArr = [];
for(var index in DataCollectionObjects){

    var Crawler = new CrawlerClass(DataCollectionObjects[index].name,DataCollectionObjects[index].startURL,DataCollectionObjects[index].selectorJson,DataCollectionObjects[index].maxPages);


    Crawler.on('CRAWL_COMPLETE',function(response){
        console.log('CRAWL COMPLETED');
        //TODO : Remove the Buffers Cleared Event; 
        
    });



    Crawler.on('BUFFER_CLEARED',function(URLArrays){
        //Push each buffer into the queue.
        var mainContext = this;
        async.each(URLArrays,function(urlArray,iteratorCallback){
            return RabbitMq.push(mainContext.siteName,{buffer:urlArray},true,1,iteratorCallback);
        },function(err){
            console.log('======================'+URLArrays.length +'  NEW BUFFERS PUSHED TO QUEUE =====================');
        })
    }.bind({siteName : DataCollectionObjects[index].name}));

    Crawler.on('CRAWLED_DATA',function(response,url,baseUrl){
    // console.log(response.textData);
        var doc = {
            data : {
                body :response.textData.body,
                category :response.textData.category,
                title :response.textData.title,     
            },
            url : url,
            website : baseUrl
        }

        var mongooseDoc = new crawlModel(doc);
        mongooseDoc.save(function(err,docs){
            if(err){
                console.log('Error Saving Record to DB');
                console.log(err);
            }else{
                console.log('Record Saved');
            }
        })

    });

    RabbitMq.subscribe(DataCollectionObjects[index].name, 'JSON', function(response,callback){
        Crawler.bulkBufferConsumer(response.buffer,callback);
    });

    //Crawler.initiate(true);

    CrawlerObjArr.push(Crawler);

}


for(var index in CrawlerObjArr){
    console.log('IniTiated '+ index);
    CrawlerObjArr[index].initiate(true);
}
