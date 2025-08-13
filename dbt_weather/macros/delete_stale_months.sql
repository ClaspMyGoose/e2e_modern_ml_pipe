{% macro delete_stale_months() %}
DELETE FROM {{ this }}
WHERE pe_date > DATE_TRUNC('month', current_date) - INTERVAL '1' MONTH
{% endmacro %}