var RabbitMqManagerClass = require('./rabbit.queue');

var noOfObjects = [1,2,3,4,5,6,7,8,9,10];

var QUEUE_NAME = "TEST_QUEUE";

var URL = 'amqp://127.0.0.1';

var async = require('async');
var QueueManangerArr = [];

RabbitMqManagerClass(URL,function(err,RMQMan){
    console.log('Channel Has been setup. Queue will now start.');
    console.log(RMQMan);
    QueueManangerArr.push(RMQMan);  
    QueueManangerArr.forEach(function(QueueManager){
        QueueManager.subscribe(QUEUE_NAME,'JSON',function(message,callback){
            console.log(message + ' CAME FROM THE OBJECT : '+ this.index)
        }.bind({index : QueueManager.index}));
    });

    async.each(QueueManangerArr,function(QueueManagerObject,iteratorCallback){
        for(var i=0;i<2000;i++){
            QueueManagerObject.push(QUEUE_NAME,{index:i},true,1,function(){});
        }
    },function(err){
        console.log('Data Added to All Queues');
    })
    //iteratorCallback();
})

// async.each(noOfObjects,function(index,iteratorCallback){
    
// },function(err){
//     QueueManangerArr.forEach(function(QueueManager){
//         QueueManager.subscribe(QUEUE_NAME,'JSON',function(message,callback){
//             console.log(message + ' CAME FROM THE OBJECT : '+ this.index)
//         }.bind({index : QueueManager.index}));
//     });

//     async.each(QueueManangerArr,function(QueueManagerObject,iteratorCallback){
//         for(var i=0;i<2000;i++){
//             QueueManagerObject.push(QUEUE_NAME,{index:i},true,1,function(){});
//         }
//     },function(err){
//         console.log('Data Added to All Queues');
//     })
// })

