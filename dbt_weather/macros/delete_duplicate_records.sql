
{% macro delete_duplicate_records() %}
DELETE FROM {{ this }}
WHERE (weather_date, name, state) 
in (
    SELECT weather_date, name, state 
    from {{ ref('stg_weather_data') }}
    where run_date = (select max(run_date) from {{ ref('stg_weather_data') }})
)
{% endmacro %}