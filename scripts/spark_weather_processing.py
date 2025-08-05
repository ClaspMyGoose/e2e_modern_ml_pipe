
import os 

# was causing the container to not be able to find JAVA :) 
# os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'
os.environ['PYSPARK_PYTHON'] = 'python'
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, arrays_zip, split, regexp_replace, trim, expr

spark = SparkSession.builder \
    .appName("Weather Data Processing") \
    .getOrCreate()


# ! because we're running this script in a separate spark container, I have mounts specified in the DAG that map our host input and output folders 
# ! to the respective input and output folders in the spark container. Here we use the spark containers path 
docker_json_read_path = '/app/data/json/*.json'
docker_csv_read_path = 'app/data/csv/*.csv'
docker_write_path = '/app/processed_data/weather_output'




dirty_weather_df = spark.read.json(docker_json_read_path)


weather_df = dirty_weather_df.select(
            'location.name',
            col('location.region').alias('state'), 
            'location.lat', 
            'location.lon',
            explode(arrays_zip(
                col('forecast.forecastday.date'),
                col('forecast.forecastday.day.maxtemp_f'),
                col('forecast.forecastday.day.mintemp_f'),
                col('forecast.forecastday.day.maxwind_mph'),
                col('forecast.forecastday.day.totalprecip_in'),
                col('forecast.forecastday.day.avghumidity')
            )).alias('daily_data') 
).select(
    'name',
    'state',
    'lat',
    'lon',
    col('daily_data.date').alias('date'),
    col('daily_data.maxtemp_f').alias('maxtemp_f'),
    col('daily_data.mintemp_f').alias('mintemp_f'),
    col('daily_data.maxwind_mph').alias('maxwind_mph'),
    col('daily_data.totalprecip_in').alias('totalprecip_in'),
    col('daily_data.avghumidity').alias('avghumidity')
)

dirty_city_pop_df = spark.read.csv(docker_csv_read_path, header=False)

intermediate_city_pop_df = dirty_city_pop_df.select(
    '_c1', # combined city and state 
    '_c2' # population 
).withColumn(
    'city_dirty',
    split(col('_c1'), ',')[0]
).withColumn(
    'state_dirty',
    split(col('_c1'), ',')[1]
).withColumn(
    'city',
    expr("substr(city_dirty, 1, length(city_dirty) - 5)")
    # regexp_replace(col('city_dirty'), '.{5}$', '')
).withColumn(
    'geo_state',
    trim(col('state_dirty'))
).withColumn(
    'population',
    regexp_replace(col('_c2'), ',', '').cast('integer')
)

# TODO trim state 

city_pop_df = intermediate_city_pop_df.select(
    col('city'),
    col('geo_state'),
    col('population')
)


joined_df = weather_df.repartition('state').join(
    city_pop_df.repartition('geo_state'), 
    [weather_df.name == city_pop_df.city, weather_df.state == city_pop_df.geo_state],
    'inner'
).select(
    'name',
    'state',
    'lat',
    'lon',
    'date',
    'maxtemp_f',
    'mintemp_f',
    'maxwind_mph',
    'totalprecip_in',
    'avghumidity',
    'population'
)



# joined_df.printSchema()
# joined_df.show(5, truncate=False)




# df_explode.printSchema()  # See the structure
# df_explode.show(5, truncate=False)


# TODO bring this back 

joined_df.write.mode('overwrite').csv(docker_write_path, header=True)


spark.stop()


# ! broken out step by step into operations. Wanted to make sure I understood what was happening

# df_zip = df.select(
#             'location.name', 
#             'location.lat', 
#             'location.lon',
#             arrays_zip(
#                 col('forecast.forecastday.date'),
#                 col('forecast.forecastday.day.maxtemp_f'),
#                 col('forecast.forecastday.day.mintemp_f'),
#                 col('forecast.forecastday.day.maxwind_mph'),
#                 col('forecast.forecastday.day.totalprecip_in'),
#                 col('forecast.forecastday.day.avghumidity')
#             ).alias('daily_data') 
# )

# df_explode = df_zip.select(
#             'name', 
#             'lat', 
#             'lon',
#             explode(
#                col('daily_data').alias('daily_data')
#             )
# )

# df_final = df_explode.select(
#         'name',
#         'lat',
#         'lon',
#         col('daily_data.date').alias('date'),
#         col('daily_data.maxtemp_f').alias('maxtemp_f'),
#         col('daily_data.mintemp_f').alias('mintemp_f'),
#         col('daily_data.maxwind_mph').alias('maxwind_mph'),
#         col('daily_data.totalprecip_in').alias('totalprecip_in'),
#         col('daily_data.avghumidity').alias('avghumidity')   
# )




