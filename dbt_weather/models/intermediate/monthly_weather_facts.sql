select 

    name, 
    state, 
    latitude,
    longitude, 
    sin(month_int) as month_sin,
    cos(month_int) as month_cos, 
    month_int,
    weather_quarter,  
    weather_season,
    max(max_day_temp_fahr) as max_month_temp_fahr,
    min(min_day_temp_fahr) as min_month_temp_fahr,  
    avg(fahr_temp_range) as avg_month_fahr_temp_range,
    max(max_day_wind_mph) as max_max_month_wind,
    min(min_day_wind_mph) as min_min_month_wind,
    avg(total_day_precip_inches) as avg_month_precipitation,
    avg(avg_day_humidity) as avg_month_humidity,
    max(weather_severity_index) as max_month_severity_index,
    min(weather_severity_index) as min_month_severity_index, 
    avg(weather_severity_index) as avg_month_severity_index, 
    city_population,
    count(*) as day_cnt, 
    CASE WHEN count(*) > 28 THEN 1 ELSE 0 END as full_month 


from {{ ref(daily_weather_facts) }}


group by 
    name, 
    state, 
    latitude,
    longitude, 
    sin(month_int) as month_sin,
    cos(month_int) as month_cos, 
    month_int,
    weather_quarter,  
    weather_season

