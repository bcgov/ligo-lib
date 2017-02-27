function getDataTypesJson() {

    var d_types = {}
    $('.data-type').each(function() {
        var field_name = $(this).attr('id').slice(0, -6);
        var field_type = $(this).val();
        d_types[field_name] = field_type;
    });
    return JSON.stringify(d_types)

}

$("#dataset-form").submit(function() {

    $("#id_data_types").val(getDataTypesJson());
    return true;

});




$(function () {
    data_types = data_types || {};
    $('.data-type').each(function() {
        var field_name = $(this).attr('id').slice(0, -6);
        var field_type = data_types[field_name] || header_types[field_name];
        $(this).val(field_type);
    });

});
