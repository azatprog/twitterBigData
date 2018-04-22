(function() {
    console.log('starting..');
    const socket = new WebSocket('ws://localhost:8789/ws');
    // socket.emit('context');
    socket.onmessage = function (data) {
        console.log(data);
        const tweetTable = document.getElementById('tweet-table');
        console.log(tweetTable);
        const row = tweetTable.insertRow(0);
        const parsedData = JSON.parse(data.data);
        row.insertCell(0).innerHTML = parsedData['text'];
        row.insertCell(1).innerHTML = parsedData['prediction'];
    }

})();