//var connection = require('amqplib').connect('amqp://127.0.0.1');//ADD URL HERE.
var queueOpts = {
    durable: true,
};

var RabbitMqManager = function (url, callback) {
    var mC = this;

    this.connection = require('amqplib').connect(url);

    this.channel = null;

    //this.index = index;

    this.connection.then(function (conn) {
        return conn.createChannel();
    }).then(function (channel) {
        //console.log(channel);
        mC.channel = channel;
        callback();
    }).catch(callback);


}


RabbitMqManager.prototype.push = function (qName, data, isJson, priority, callback) {
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
    var channel = this.channel;

    // ensure the queue exists
    channel.assertQueue(qName, queueOpts);
    // add the data to the queue as a buffer
    channel.sendToQueue(qName, new Buffer(data), { persistent: false, priority: priority });
    //channel.close();
    return callback(null);

}


RabbitMqManager.prototype.subscribe = function (qName, format, onMessage) {
    if (!qName || !onMessage || !format) {
        throw new Error('invalid parameters passed to rabbit.utils');
    }
    var channel = this.channel;
    // ensure that only one message is allotted to this task at a time
    this.channel.prefetch(1);
    // consume the message
    return this.channel.consume(qName, function (message) {
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

module.exports = function (url, callback) {
    if (url && callback) {
       var x = new RabbitMqManager(url, function(){
            callback(null,x);
        });
    }
}