//var config = appRequire('config');
var amqp = require('amqplib/callback_api');
var URL = 'amqp://127.0.0.1';
var connection = null// require('amqplib').connect('amqp://127.0.0.1');//ADD URL HERE.
//var LogUtils = appRequire('utils.log');
var publishChannel = null;

/**
 * Expects the name of the queue, the function that uses the input data from the queue, the format it wants that
 * data in and the final callback
 */

//var config = appRequire('config');

//var connection = require('amqplib').connect(config.rabbitQ.url);
// todo : check if we should keep a global channel object, or one for push, one for pull


var queueOpts = {
    durable: true,
};

module.exports = {

    connect: connect,

    push: push,

    pull: pull,

    subscribe: subscribe,

    onDrainSubscriber: onDrainSubscriber
};

function connect(callback) {
    amqp.connect(URL, function (err, conn) {
        if (err) {
            return callback(err);
        }
        connection = conn;
        connection.createChannel(function (err, newChannel) {
            if (err) {
                return callback(err);
            }
            console.log("RabbitMQ SUCCESSFULLY CONNECTED");
            publishChannel = newChannel;
            callback();

        });
    })
}

function onDrainSubscriber(fn) {

}

function createChannel(callback) {
    connection.createChannel(callback);
}

/**
 * put an entity into the queue
 */
function push(qName, data, isJson, priority, callback) {
    if (!qName || !data) {
        throw new Error('insufficient data passed for push operation. Make sure a queue name and the data to be pushed are being passed');
    }
    // if the data is of the json type, stringify it
    if (isJson === true) {
        data = JSON.stringify(data);
    }
    // set default priority to 1, or the input value
    priority = priority || 1;
    // if the data is not a string, convert it to string
    if (typeof data !== 'string') {
        try {
            data = data.toString();
        } catch (err) {
            return callback(err);
        }
    }
    // ensure the queue exists
    if (publishChannel != null) {
        publishChannel.assertQueue(qName, queueOpts);
        // add the data to the queue as a buffer
        if (!publishChannel.sendToQueue(qName, new Buffer(data), { persistent: false, priority: priority })) {
            console.log('BUFFER LIMIT REACHED IN THE QUEUE~!!!!!========= STOPADDING ====== DRAIN EVEN ABOUT TO EMIT! ============');
        }
        //channel.close();
        return callback(null);
    }
}

/**
 * fetch an entity from the queue
 */
function pull(qName, isJson, callback) {
    // fetch a channel from the connection
    getChannel(function (err, channel) {
        if (err) {
            return callback(err);
        }
        // assert that the q exists
        channel.assertQueue(qName, queueOpts);
        // consume an item from the queue
        channel.consume(qName,
            // if a message was found successfully
            function (message) {
                if (message === null) { // if no item is returned, just return null in the callback
                    channel.close();
                    return callback();
                } else {
                    // acknowledge that the message was consumed                
                    // channel.ack(message);
                    // convert the message from a buffer to a string
                    message = message.content.toString();
                    // if a message is returned, and the expected message should be of the JSON type, parse it to JSON
                    if (isJson) {
                        message = JSON.parse(message);
                    }
                    //channel.close();
                    // return the message
                    return callback(null, message);
                }
            },
            // if an error occurred in message retrieval from the queue
            function (ok) {
                console.log(err, ok);
                // return callback(err);   
            }
        );
    });
}

/**
 * callback(err, channel)
 */
function getChannel(callback) {
    connection.then(function (conn) {
        return conn.createChannel();
    }).then(function (channel) {
        return callback(null, channel);
    }).catch(callback);
}

function subscribe(qName, format, onMessage) {
    if (!qName || !onMessage || !format) {
        throw new Error('invalid parameters passed to rabbit.utils');
    }
    createChannel(function(err,channel){
        if(err){
            throw new Error('QUEUE NOT CONNECTING');
        }
        if (channel != null) {
            console.log('Channel Found');
            console.log(channel);
            channel.assertQueue(qName, function (err,ok) {
                channel.prefetch(1);
                // consume the message
                console.log('VALUE OF OK')
                console.log(ok);
                channel.consume(qName, function (message) {
                    console.log('Message Has Come');
                    // convert the message sent by the queue to the input format
                    var msg = formatMessage(message, format);
                    // invoke the input onMessage function that does some work with the formatted message and invokes
                    // the callback when it is done
                    onMessage(msg, function (err) {
                        if (err) {
                            console.log(err);
                            return channel.nack(message);
                        }
                        return channel.ack(message);
                    });
                });
            });
        } else {
            console.log('CHANNEL NOT SET!');
        }
    })
};

/**
 * Formats the message(Buffer) into the desired format for the worker module. 
 * By default converts to json (as per our requirement)
 */
function formatMessage(message, format) {
    switch (format.toLowerCase()) {
        case 'string':
            return message.content.toString();
        case 'number':
            return Number(message.content.toString);
        default:
            return JSON.parse(message.content.toString());
    }
}