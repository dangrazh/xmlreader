function toggleCustomSeparatorInput() {

    // alert($('input[name=use_deffaut_separators]:checked').val());
    if ($('input[name=use_deffaut_separators]:checked').val() == 2) {
        // $('#custom_separators').css('display', 'block');
        // $('label[for="custom_separators"]').show();
        $('#cust_separator_group').show();
    } else {
        // $('#custom_separators').css('display', 'none');
        // $('label[for="custom_separators"]').hide();
        $('#cust_separator_group').hide();
    }
}

$('#uploadFile').on('click', function (e) {

    // show the 'processing' popup with click (backdrop)
    // and keyboard disabled on parent page
    displayModalWithSrcFileCheck('Loading and splittling file. This might take a while...');

    // submit the page
    document.xmlupload.submit();

});
