{% macro get_vendor_data(vendorid) %}

{% 
    set vendors = {
        1: 'Creative Mobile Technologies',
        2: 'VeriFone Inc.',
        4: 'Unknown/Other',
    }
%}

case {{ vendorid }}
    {% for vendor_id, vendor_name in vendors.items() %}
        when {{ vendor_id }} then '{{ vendor_name }}'
    {% endfor %}
end

{% endmacro %}