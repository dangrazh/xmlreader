$(function () {

    /* This 'event' is used just to avoid that the table text 
     * gets selected (just for styling). 
     * For example, when pressing 'Shift' keyboard key and clicking 
     * (without this 'event') the text of the 'table' will be selected.
     * You can remove it if you want, I just tested this in 
     * Chrome v30.0.1599.69 */
    $(document).bind('selectstart dragstart', function (e) {
        e.preventDefault(); return false;
    });

});

$("tr").click(function (e) {

    // console.log("*** New Run ***")
    var bStart = false;
    var selectedRow = $(this);
    var table = $(this).closest('table');
    var selector = "#" + table.attr('id');
    var shift = e.shiftKey;
    // console.log("ShiftKey: " + shift);
    // console.log("selectedRow: " + selectedRow);
    // console.log("selectedRow index: " + selectedRow.index());
    // console.log("table selector: " + selector);
    if (shift == true) {
        $(selector + " tr").each(function () {
            // console.log("Row: " + $(this).index() + " has class selected: " + $(this).hasClass('selected'))
            if ($(this).hasClass('selected') == true) {
                if (bStart == false) {
                    // console.log("We are on row " + $(this).index() + " setting bStart to true but not toggling selected")
                    var nextrow = $(this).next('tr')
                    if (nextrow.hasClass('selected') == true) {
                        $(this).toggleClass('selected');
                    }
                }
                else {
                    // console.log("We are on row " + $(this).index() + " setting bStart to true and toggling selected")
                    $(this).toggleClass('selected');
                }
                bStart = true;
            }
            else {
                if (bStart == true && ($(this).index() <= selectedRow.index())) {
                    // console.log("Toggling Row: " + $(this).index() + " which has class selected: " + $(this).hasClass('selected'))
                    $(this).toggleClass('selected');
                }
            }
        });
    }
    else {
        selectedRow.toggleClass('selected');
        bStart = false;
    }
    // var value = $(this).find('td:first').html();
    // alert(value);    
});


$(document).ajaxStop(function () {
    console.log("ajaxStop is executed...");
    alert("ajaxStop is executed...");

    setTimeout("window.location = '/xmlparser/main'", 500);
    // setTimeout("window.location.reload()",500);
});

$('#processFile').on('click', function (e) {

    // show the 'processing' popup with click (backdrop)
    // and keyboard disabled on parent page
    displayModal('Parsing Xml documents. This might take a while...');

    // submit the page
    document.xmlmain.submit();

});

$('#createExcel').on('click', function (e) {
    var selection = new Object();
    $("tr.selected").each(function () {

        tableName = ($(this).closest('table').attr('name'));
        if (tableName in selection) {
            var attributes = selection[tableName]
        }
        else {
            var attributes = new Set();
        }

        attrName = ($('td:first', this).html());
        attributes.add(attrName);
        selection[tableName] = attributes;

    });

    // convert the sets to arrays so the json.stringify works
    for (var tabName in selection) {
        var val = selection[tabName]
        selection[tabName] = Array.from(val)
    }

    // show the 'processing' popup with click (backdrop)
    // and keyboard disabled on parent page
    displayModal('Creating Excel file. This might take a while...');

    // OLD
    // alert("Submitting the following input to the createexcel endpoint:\n" + JSON.stringify(selection));
    // success = postDataToServer(selection);
    // console.log('result is: ' + result);
    // if (result["returnMsg"] == "OK") {
    //     console.log('switching buttons now');
    //     $('#createExcel').hide();
    //     $('#downloadExcel').show();
    // }

    // buttonState = $('#downloadExcel').is(":hidden")

    // if (success == "OK" && buttonState == true) {
    // submit the page
    // document.xmlmain.submit();
    // }

    // reload the page (ignoring the ajax result as the feedback
    // is flash'ed from flask backend)
    // alert("calling setTimeout from createExcel with: window.location = '/xmlparser/main'");
    // setTimeout("window.location = '/xmlparser/main'", 500);

    // NEW
    var dtNow = new Date();
    console.log(dtNow.toLocaleString() + ": calling async postData now");
    postData('http://localhost:5000/xmlparser/createexcel', selection).then(function (v) {
        console.log(dtNow.toLocaleString() + ":  retVal is: " + v);
        console.log(v);
        return v;
    });
    console.log(dtNow.toLocaleString() + ": after calling async postData");
    // console.log(dtNow.toLocaleString() + ":  retVal is: " + retVal);
    // console.log(retVal);

});

async function postData(url = '', data = {}) {
    // Default options are marked with *
    const response = await fetch(url, {
        method: 'POST', // *GET, POST, PUT, DELETE, etc.
        mode: 'cors', // no-cors, *cors, same-origin
        cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
        credentials: 'same-origin', // include, *same-origin, omit
        headers: {
            'Content-Type': 'application/json'
            // 'Content-Type': 'application/x-www-form-urlencoded',
        },
        redirect: 'follow', // manual, *follow, error
        //   referrerPolicy: 'no-referrer', // no-referrer, *no-referrer-when-downgrade, origin, origin-when-cross-origin, same-origin, strict-origin, strict-origin-when-cross-origin, unsafe-url
        body: JSON.stringify(data) // body data type must match "Content-Type" header
    });
    return response.json(); // parses JSON response into native JavaScript objects
}
