var URL = 'http://www.ndtv.com/tamil-nadu-news/lawyer-of-expelled-aiadmk-leader-who-raised-questions-on-jayalalithaas-health-attacked-1642704?pfrom=home-lateststories';

var request = require('request');
var cheerio = require('cheerio');

var mainContext = {
    selectorJson: {
        body: '.ins_storybody',
        title: 'h1[itemprop="headline"]',
        category: '.breadcrums span:nth-child(2) a span',
    },
    startURL: 'http://www.ndtv.com/',
    maxPages: 2000,
    name: "NDTV"
};

request(URL, function (error, response, body) {
    //console.log(error);
    // Check status code (200 is HTTP OK)
    if (error) {
        console.log(error);
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
        responseJson.internalLinks = collectInternalLinks($);
        //callback(error, responseJson);
        console.log(responseJson)
    }

});

var collectInternalLinks = function ($) {
    var relativeLinks = $("body a");
    //console.log("Found " + relativeLinks.length + " relative links on page");
    var pagesToVisit = [];
    relativeLinks.each(function () {
        pagesToVisit.push($(this).attr('href'));
    });
    return pagesToVisit;
}