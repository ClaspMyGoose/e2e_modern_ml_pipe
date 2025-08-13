{% macro delete_duplicate_run_if_exists()  %}

DELETE FROM {{ this }}
WHERE run_date = (SELECT current_date)

{% endmacro %}

