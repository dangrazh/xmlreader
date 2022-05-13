
function getDataFromServer() {
    // GET is the default method, so we don't need to set it
    fetch('/ajaxapi')
        .then(function (response) {
            // Getting and passing on the response text
            return response.text();
        })
        .then(function (text) {

            // Main processing - do something with the response text
            // Print the greeting as text
            console.log('GET response text:');
            console.log(text);
        }).catch(function (error) {
            // Error handling - log the error
            console.log(error);
        });
}

function getDataFromServerJson() {
    // send the request
    fetch('/ajaxapi')
        .then(function (response) {

            // Getting and passing on the response text
            return response.json();
        })
        .then(function (json) {

            // Main processing - do something with the response json
            // Do anything with it!
            console.log('GET response as JSON:');
            console.log(json);
        }).catch(function (error) {
            // Error handling - log the error
            console.log(error);
        });
}

function postDataToServer() {
    // POST a request
    fetch('/ajaxapi', {

        // Declare what type of data we're sending
        headers: {
            'Content-Type': 'application/json'
        },

        // Specify the method
        method: 'POST',

        // A JSON payload
        body: JSON.stringify({ "greeting": "Hello from the browser!" })
    })
        .then(function (response) {
            return response.text();
        })
        .then(function (text) {

            console.log('POST response: ');

            // Should be 'OK' if everything was successful
            console.log(text);
        }).catch(function (error) {
            // Error handling - log the error
            console.log(error);
        });
}

function postAndGetDatajQuery() {
    console.log("Calling ajax with jQuery now")
    $.ajax(
        {
            type: 'POST',
            url: "/ajaxapi2",
            contentType: "application/json;charset=UTF-8",
            dataType: 'json',
            data: JSON.stringify({ "messageClient": "didyoureceive?" }),
            success: function (data) {
                console.log("alert2:messageServer = " + data);
                if (data != null) {
                    console.log("alert3:messageServer = " + data);
                    messageServer = data;
                    $('#UID_afficheTest').val(messageServer);
                    document.getElementById('UID_afficheTest').innerHTML = messageServer;

                }
            }/*success : function() {}*/
        });/*$.ajax*/
}