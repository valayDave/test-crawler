var CrawlerClass = require('./dataCollector');
var mongoose = require('mongoose');
mongoose.connect('mongodb://127.0.0.1:27017/data-Crawler');
var RabbitMq = require('./rabbit.utils');
var QueueName = 'BULK_BUFFER_QUEUE6';
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

var collectionName = 'crawlCollection-'+DATE_HR_STRING;
var crawlModel = mongoose.model(collectionName, crawlCollectionSchema, collectionName);
var selectorJson = {
    body : '.articles p',
    title : '.heading-part h1',
    category : '.breadcrumb .first span'
};

var startURL = 'http://indianexpress.com/';

var Crawler = new CrawlerClass('indianExpress',startURL,selectorJson,2000);


Crawler.on('CRAWL_COMPLETE',function(response){
    console.log('CRAWL COMPLETED');
    //TODO : Remove the Buffers Cleared Event; 
});



Crawler.on('BUFFER_CLEARED',function(URLArrays){
    //Push each buffer into the queue.
    async.each(URLArrays,function(urlArray,iteratorCallback){
        return RabbitMq.push(QueueName,{buffer:urlArray},true,1,iteratorCallback);
    },function(err){
        console.log('======================'+URLArrays.length +'  NEW BUFFERS PUSHED TO QUEUE =====================');
    })
});

Crawler.on('CRAWLED_DATA',function(response,url){
    //console.log(response);
    var doc = {
        data : {
            body :response.textData.body,
            category :response.textData.category,
            title :response.textData.title,     
        },
        url : url,
        website : startURL
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

RabbitMq.subscribe(QueueName, 'JSON', function(response,callback){
    Crawler.bulkBufferConsumer(response.buffer,callback);
});

Crawler.initiate(true);