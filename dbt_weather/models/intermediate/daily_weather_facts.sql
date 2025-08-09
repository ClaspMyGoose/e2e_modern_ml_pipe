{{ config(
    materialized='incremental',
    unique_key=['weather_date','name', 'state'],
    merge_update_columns=['max_day_temp_fahr','min_day_temp_fahr','fahr_temp_range','max_day_wind_mph','total_day_precip_inches','avg_day_humidity','weather_severity_index'],
    indexes=[
        {'columns': ['weather_date'], 'type': 'btree'},
        {'columns': ['name'], 'type': 'btree'},
        {'columns': ['state'], 'type': 'bitmap'},
    ]
) }}



SELECT 

    name, 
    state, 
    latitude,
    longitude, 
    weather_date,
    dayofyear(weather_date) as day_of_year,
    sin(day_of_year) as day_of_year_sin,
    cos(day_of_year) as day_of_year_cos, 
    extract(month from weather_date) as month_int,
    CASE 
        WHEN month_int in (1,2,3)
        THEN 1
        WHEN month_int in (4,5,6)
        THEN 2
        WHEN month_int in (7,8,9)
        THEN 3
        ELSE 4 -- 10,11,12
    END as weather_quarter,  
    CASE 
        WHEN month_int in (6,7,8)
        THEN 'Summer'
        WHEN month_int in (9,10,11)
        THEN 'Fall'
        WHEN month_int in (12,1,2)
        THEN 'Winter'
        ELSE 'Spring' -- 3,4,5
    END as weather_season,  
    max_day_temp_fahr,
    min_day_temp_fahr,
    (max_day_temp_fahr - min_day_temp_fahr) as fahr_temp_range,
    max_day_wind_mph,
    total_day_precip_inches,
    avg_day_humidity,
    (
        (fahr_temp_range * 0.4) + 
        (max_day_wind_mph * 0.3) +
        (total_day_precip_inches * 20) +
        (CASE WHEN avg_day_humidity > 80 THEN 10 ELSE 0 END) 
    ) as weather_severity_index, 
    city_population



FROM {{ ref('stg_weather_data') }}

{% if is_incremental() %}
    -- only processes if the table exists (after run 1)
    WHERE weather_date >= (SELECT MAX(weather_date) - INTERVAL '3 days' FROM {{ this }} )  
{% endif %}


