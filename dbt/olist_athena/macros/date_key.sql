{% macro date_key(column) %}
    CAST(date_format(TRY_CAST(NULLIF({{ column }}, '') AS timestamp), '%Y%m%d') AS integer)
{% endmacro %}