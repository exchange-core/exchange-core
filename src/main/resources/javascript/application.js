$(function () {
    "use strict";

    var header = $('#header');
    var content = $('#content');
    var input = $('#input');
    var status = $('#status');
    var myName = false;
    var author = null;
    var logged = false;
    var socket = atmosphere;
    var subSocket;
    var transport = 'websocket';

    // We are now ready to cut the request
    var request = { url: document.location.protocol + "//" + document.location.host + '/chat',
        contentType: "application/json",
        logLevel: 'debug',
        transport: transport,
        trackMessageLength: true,
        enableProtocol: true,
        fallbackTransport: 'long-polling'};


    request.onOpen = function (response) {
        content.html($('<p>', { text: 'Atmosphere connected using ' + response.transport }));
        input.removeAttr('disabled').focus();
        status.text('Choose name:');
        transport = response.transport;
    };

    // For demonstration of how you can customize the fallbackTransport using the onTransportFailure function
    request.onTransportFailure = function (errorMsg, request) {
        atmosphere.util.info(errorMsg);
        if (window.EventSource) {
            request.fallbackTransport = "sse";
        }
        header.html($('<h3>', { text: 'Atmosphere Chat. Default transport is WebSocket, fallback is ' + request.fallbackTransport }));
    };

    request.onMessage = function (response) {

        var message = response.responseBody;
        try {
            var json = jQuery.parseJSON(message);
        } catch (e) {
            console.log('This doesn\'t look like a valid JSON: ', message.data);
            return;
        }

        if (!logged && myName) {
            logged = true;
            status.text(myName + ': ').css('color', 'blue');
            input.removeAttr('disabled').focus();
        } else {
            input.removeAttr('disabled');
            if(json.msgType == 'chat'){

                var me = json.author == author;
                var date = typeof(json.time) == 'string' ? parseInt(json.time) : json.time;
                addMessage(json.author, json.message, me ? 'blue' : 'black', new Date(date));

            }else if(json.msgType == 'orderbook'){
                updateOrderBook(json.askPrices, json.askVolumes, json.bidPrices, json.bidVolumes);
            }
        }
    };

    request.onClose = function (response) {
        logged = false;
    };

    request.onError = function (response) {
        content.html($('<p>', { text: 'Sorry, but there\'s some problem with your socket or the server is down' }));
    };

    subSocket = socket.subscribe(request);

    input.keydown(function (e) {
        if (e.keyCode === 13) {
            var msg = $(this).val();

            // First message is always the author's name
            if (author == null) {
                author = msg;
            }

            subSocket.push(atmosphere.util.stringifyJSON({ msgType: 'chat', author: author, message: msg }));
            $(this).val('');

            input.attr('disabled', 'disabled');
            if (myName === false) {
                myName = msg;
            }
        }
    });

    function addMessage(author, message, color, datetime) {
        content.append('<p><span style="color:' + color + '">' + author + '</span> @ ' + +(datetime.getHours() < 10 ? '0' + datetime.getHours() : datetime.getHours()) + ':'
            + (datetime.getMinutes() < 10 ? '0' + datetime.getMinutes() : datetime.getMinutes())
            + ': ' + message + '</p>');
    }

    function updateOrderBook(askPrices, askVolumes, bidPrices, bidVolumes) {
        var table = "<div class='divTableBody'>";
        var size = askPrices.length;
        console.log('size='+size);
        var i;
        for(i = size - 1; i >= 0; i--){
            table += ("<div class='divTableRow obCellAsk'><div class='divTableCell'>"+askPrices[i]+"</div><div class='divTableCell'>"+askVolumes[i]+"</div></div>");
        }
        size = bidPrices.length;
        for(i = 0; i < size; i++){
            table += ("<div class='divTableRow obCellBid'><div class='divTableCell'>"+bidPrices[i]+"</div><div class='divTableCell'>"+bidVolumes[i]+"</div></div>");
        }
        table += "</div>"
        orderbook.innerHTML = table; //"<table><tr><td>"+price+"</td><td>"+volume+"</td></tr></table>"
    }

});