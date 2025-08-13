
import os 
import shutil 
from dotenv import load_dotenv
# was causing the container to not be able to find JAVA :) 
# os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'
os.environ['PYSPARK_PYTHON'] = 'python'
import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, arrays_zip, split, regexp_replace, trim, expr, count_distinct


spark = SparkSession.builder \
    .appName("Weather Data Processing") \
    .getOrCreate()

load_dotenv()
json_read_path = os.getenv('EXTRACT_MOUNT_PATH_JSON')
csv_read_path = os.getenv('EXTRACT_MOUNT_PATH_CSV')
write_path = os.getenv('PROCESS_OUTPUT_MOUNT_PATH')



dirty_weather_df = spark.read.json(json_read_path)


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

dirty_city_pop_df = spark.read.csv(csv_read_path, header=False)

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


countOfUniqueCities = joined_df.select(count_distinct('name')).collect()[0][0]

if countOfUniqueCities != 50:
    print(f'Expected 50, got: {countOfUniqueCities}')
    sys.exit(1)


# df_explode.printSchema()  # See the structure
# df_explode.show(5, truncate=False)



joined_df.write.mode('overwrite').csv(write_path, header=True)


# for file in os.listdir(json_read_path):
#     filepath = os.path.join(json_read_path, file)
#     if os.path.isfile(filepath) and file[-5:] == '.json':
#         os.remove(filepath)
# # shutil.rmtree(f'{json_read_path}*')


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




