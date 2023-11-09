{% macro extract(time_type, column_name) %}
    EXTRACT({{ time_type }} FROM TIMESTAMP({{ column_name }}))
{% endmacro %}