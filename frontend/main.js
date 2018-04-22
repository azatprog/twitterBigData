(function() {
    function message(data) {
        console.log(data);
        const tweetTable = document.getElementById('tweet-table');
        console.log(tweetTable);
        const row = tweetTable.insertRow(0);
        const parsedData = JSON.parse(data.data);
        console.log(parsedData);
        row.insertCell(0).innerHTML = parsedData['text'];
        row.insertCell(1).innerHTML = parsedData['prediction'];
    }
    function open(ev) {
        console.log(ev);
        socket = new WebSocket('ws://localhost:8789/ws');
        socket.onmessage = message;
        socket.onclose = open;
        socket.onerror = console.log;
        socket.onopen = console.log;
    }
    open('starting..');
})();