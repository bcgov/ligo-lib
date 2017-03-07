$(function () {
    getRightHeader();
});

$("#" + right_data_id).change(function() {
    getRightHeader();
});


$('#form-steps-container').on('click', '.blocking-vars .block-var-create', function() {
    var div_id = $(this).parent().parent().attr('id');
    form_index = div_id.slice(14);
    rows = $("#" + div_id + " tr").length;

    transformation = '';
    for (item in transformation_choices) {
        transformation += '<option value="' + transformation_choices[item][0] + '">' + transformation_choices[item][1] + '</option>';
    }

    row_html = '<tr><td><select id="block_id_left_' + form_index + '_' + rows + '" class="left-header form-control"'
             + 'name="block_id_left_' + form_index + '_' + rows + '">' + header_options[left_data_id] + '</select></td>'
             + '<td><select id="block_id_right_' + form_index + '_' + rows + '" class="right-header form-control"'
             + 'name="block_id_right_' + form_index + '_' + rows + '">' + header_options[right_data_id] + '</select></td>'
             + '<td><select id="block_comp_' + form_index + '_' + rows + '" class="alg form-control"'
             + 'name="block_comp_' + form_index + '_' + rows + '">'
             + '<option>------------</option>' + transformation
             + '</select></td>'
             + '<td><button type="button" class="blocking-var-remove btn btn-danger">'
             + '<i class="glyphicon glyphicon-remove"></i></button></td></tr>';

    $("#" + div_id + " tbody").append(row_html);

    return false;
});

$('#form-steps-container').on('click', '.linking-vars .link-var-create', function() {
    var div_id = $(this).parent().parent().attr('id');
    form_index = div_id.slice(14);
    rows = $("#" + div_id + " .link-var-row").length;


    comparisons = '';
    for (item in comparison_choices) {
        comparisons += '<option value="' + comparison_choices[item][0] + '">' + comparison_choices[item][1] + '</option>';
    }

    row_html = '<div class="link-var-row">'
             + '<div class="row">'
             + '<label for="link_id_left_' + form_index + '_' + rows + '" class="control-label col-sm-2">Left Variable</label>'
             + '<div class="preview col-sm-4">'
             + '<select id="link_id_left_' + form_index + '_' + rows + '" class="left-header form-control"'
             + ' name="link_id_left_' + form_index + '_' + rows + '">' + header_options[left_data_id] + '</select></div>'
             + '<label for="link_id_right_' + form_index + '_' + rows + '" class="control-label col-sm-2">Right Variable</label>'
             + '<div class="preview col-sm-4">'
             + '<select id="link_id_right_' + form_index + '_' + rows + '" class="right-header form-control"'
             + ' name="link_id_right_' + form_index + '_' + rows + '">' + header_options[right_data_id] + '</select></div></div>'
             + '<div class="row">'
             + '<label for="link_comp_' + form_index + '_' + rows + '" class="control-label col-sm-2">Comparison Method</label>'
             + '<div class="col-sm-10"><div class="preview input-group">'
             + '<select id="link_comp_' + form_index + '_' + rows + '" class="alg form-control"'
             + 'name="link_comp_' + form_index + '_' + rows + '">'
             + '<option>------------</option>' + comparisons + '</select>'
             + '<div class="input-group-btn">'
             + '<button type="button" class="linking-var-remove btn btn-danger">'
             + '<i class="glyphicon glyphicon-remove"></i></button>'
             + '</div></div></div>'
             + '<div class="alg-arg"></div>';

    $("#" + div_id + " .link-vars-container").append(row_html);

    return false;
});

