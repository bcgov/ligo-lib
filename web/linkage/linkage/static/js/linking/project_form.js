$(function () {
    $.ajaxSetup({
        headers: { "X-CSRFToken": Cookies.get('csrftoken') }
    });

    getHeader(left_data_id, 'left-header');

    var total_steps = $("#id_steps-TOTAL_FORMS").val();
    $("#form-steps-container input:checkbox").hide();

    $('.step-seq').each(function(index) {
        $(this).val(index + 1);
    });

});

function getHeader(elmnt, class_name) {
    if (typeof elmnt === 'undefined') return;

    var dataset_id = $("#" + elmnt).val();
    console.log("dataset_id" + dataset_id);
    var processResponse = function(response_data, textStatus_ignored, jqXHR_ignored)  {
        var header =  response_data.header;
        var options = '<option></option>';
        for (i=0; i < header.length; i++) {
            options += '<option value="' + header[i] + '">' + header[i] + '</option>';
        }

        header_options[elmnt] = options;

        $('.' + class_name).each(function() {
            var selected_val = $(this).val();
            $(this).html(options);
            $(this).val(selected_val);
        });

    };
    var data_header_req = {
        url : DATASET_COLUMNS_URL ,
        type : "GET",
        data : {
            id: dataset_id,
        },
        success: processResponse
    };
    $.ajax(data_header_req);
};

$("#" + left_data_id).change(function() {
    getHeader(left_data_id, 'left-header');
});

function blocking_json(index) {

     schema = {left : [], right: [], transformations: [] };

    var left_selector = "#blocking-vars-" + index + " .left-header";

    $(left_selector).not(".deleted").each(function() {
        var selected_val = $(this).val();
        schema.left.push(selected_val);
    });

    var right_selector = "#blocking-vars-" + index + " .right-header";
    $(right_selector).not(".deleted").each(function() {
        var selected_val = $(this).val();
        schema.right.push(selected_val);
    });

    var trans_selector = "#blocking-vars-" + index + " .alg";
    $(trans_selector).not(".deleted").each(function() {
        var selected_val = $(this).val();
        schema.transformations.push(selected_val);
    });

    return JSON.stringify(schema);

}

function linking_json(index) {


     schema = {left : [], right: [], comparisons: [] };

    var left_selector = "#linking-vars-" + index + " .left-header";

    $(left_selector).not(".deleted").each(function() {
        var selected_val = $(this).val();
        schema.left.push(selected_val);
    });

    var right_selector = "#linking-vars-" + index + " .right-header";
    $(right_selector).not(".deleted").each(function() {
        var selected_val = $(this).val();
        schema.right.push(selected_val);
    });

    var trans_selector = "#linking-vars-" + index + " .alg";
    $(trans_selector).not(".deleted").each(function() {
        var selected_val = $(this).val();
        var suffix = this.id.slice(9);

        var comparison = {"name": selected_val};
        args_list = COMPARISON_ARGS[selected_val];
        if (args_list) {
            args = {};
            for (index = 0; index < args_list.length; index++) {
                arg = {};
                arg_id = "link_comp_arg" + suffix + "_" + index;
                var arg_val = $("#" + arg_id).val();
                var arg_name = $('label[for="' + arg_id + '"]').html();
                arg_val = (!isNaN(arg_val)) ? parseFloat(arg_val) : arg_val;
                args[arg_name] = arg_val;
            }

            comparison["args"] = args;
        }
        schema.comparisons.push(comparison);
    });

    return JSON.stringify(schema);

}

$("#linking-form").submit(function() {
    var count = parseInt($('#id_steps-TOTAL_FORMS').val());
    //Reconstruct blocking and linking schema from the input elements
    for (var index = 0; index <count; index++) {

        var field_select = "#id_steps-" + index + "-blocking_schema";
        $(field_select).val(blocking_json(index));

        field_select = "#id_steps-" + index + "-linking_schema";
        $(field_select).val(linking_json(index));

    }

    return true;
});


$('#form-steps-container').on('click', '.blocking-vars .blocking-var-remove', function() {
    var row = $(this).parent().parent();
    row.find("td select").addClass( "deleted" );
    row.hide();

    return false;
});

$('#form-steps-container').on('click', '.linking-vars .linking-var-remove', function() {
    var row = $(this).parent().parent().parent().parent().parent();
    row.find("select, input").addClass( "deleted" );
    row.hide();

    return false;
});

$("#form-steps-container").on('change', '.link-vars-container .link-var-row .alg', function(){

    var select_id = $(this).attr('id');
    suffix = select_id.slice(9);
    var selected_alg = $(this).val();
    $(this).parent().parent().parent().find('.alg-arg').empty();
    var args_list = comparison_args[selected_alg];
    if (args_list) {
        args_html = '';
        for (index = 0; index < args_list.length; index++) {
            arg_id = 'link_comp_arg' + suffix + '_' + index;
            arg_name = args_list[index];
            args_html += '<label for="' + arg_id + '" class="control-label col-sm-2">' + arg_name + '</label>'
                    + '<div class="preview col-sm-4"><input id="' + arg_id + '" type="text" class="form-control"></div>';
        }
        $(this).parent().parent().parent().find('.alg-arg').append(args_html);
    }

});

$("#form-steps-container").on('click', '.step-delete', function() {

    var form_id = $(this).parent().parent().parent().attr('id');
    var form_index = form_id.slice(10);
    var delete_id = "id_steps-" + form_index + "-DELETE" ;
    $("#" + delete_id).prop('checked', true);
    $("#" + form_id).hide();
    return false;
});

/*
    Based on the stackoverflow solution provided here :
    http://stackoverflow.com/questions/21260987/add-a-dynamic-form-to-a-django-formset-using-javascript-in-a-right-way?answertab=votes#tab-top
 */
$("#step-create").click(function() {

    var count = parseInt($('#id_steps-TOTAL_FORMS').val());
    var tmplMarkup = $('#item-template').html();
    var compiledTmpl = tmplMarkup.replace(/__prefix__/g, count);
    $('div#form-steps-container').append(compiledTmpl);

    $('#id_steps-TOTAL_FORMS').val(count+1);
    $("#id_steps-" + count +"-DELETE").hide();
    $("#id_steps-" + count +"-seq").val(count+1);
    return false
});
