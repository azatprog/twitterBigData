(function() {
    console.log('starting..');
    // const socket = io('http://10.240.21.141:3000');
    // socket.emit('context');
    // socket.on('reply', function (data) {
    //     console.log(data);
    // });
    const tweetTable = document.getElementById('tweet-table');
    console.log(tweetTable);
    const row = tweetTable.insertRow(0);
    row.insertCell(0).innerHTML = 'Tweet 2';
    row.insertCell(1).innerHTML = 'value 2';
})();