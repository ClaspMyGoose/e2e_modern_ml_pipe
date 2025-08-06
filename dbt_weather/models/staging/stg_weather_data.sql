{{ config(
    materialized='table',
    indexes=[
        {'columns': ['weather_date'], 'type': 'btree'},
        {'columns': ['name'], 'type': 'btree'},
        {'columns': ['state'], 'type': 'bitmap'}
    ]
) }}

SELECT 
    name, 
    state, 
    CAST(lat as DOUBLE) as latitude,
    CAST(lon as DOUBLE) as longitude, 
    CAST(date as DATE) as weather_date, 
    CAST(maxtemp_f as DOUBLE) as max_day_temp_fahr,
    CAST(mintemp_f as DOUBLE) as min_day_temp_fahr,
    CAST(maxwind_mph as DOUBLE) as max_day_wind_mph,
    CAST(totalprecip_in as DOUBLE) as total_day_precip_inches,
    CAST(avghumidity as INT) as avg_day_humidity,
    CAST(population as INT) as city_population 

from read_csv_auto('../processed_data/weather_output/part-*.csv', header=true)
