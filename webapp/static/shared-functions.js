function displayModal(statusMessage) {

    // adjust the message
    $("#modal_body").html(statusMessage);

    // show the 'processing' popup with click (backdrop)
    // and keyboard disabled on parent page
    $('#loadingModal').modal({
        keyboard: false,
        backdrop: 'static',
        show: true
    });

}

function displayModalWithSrcFileCheck(statusMessage) {

    if ($('#sourcefile').val() != '') {

        // adjust the message
        $("#modal_body").html(statusMessage);

        // show the 'processing' popup with click (backdrop)
        // and keyboard disabled on parent page
        $('#loadingModal').modal({
            keyboard: false,
            backdrop: 'static',
            show: true
        });
    }

}
