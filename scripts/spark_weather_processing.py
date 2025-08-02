
import os 

# was causing the container to not be able to find JAVA :) 
# os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'
os.environ['PYSPARK_PYTHON'] = 'python'
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, arrays_zip

spark = SparkSession.builder \
    .appName("Weather Data Processing") \
    .getOrCreate()


local_read_path = '../data/*.json'
local_write_path = '../processed_data/weather_output'

docker_read_path = '/opt/airflow/data/*.json'
docker_write_path = '/opt/airflow/processed_data/weather_output'


df = spark.read.json(docker_read_path)


df_explode = df.select(
            'location.name', 
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
    'lat',
    'lon',
    col('daily_data.date').alias('date'),
    col('daily_data.maxtemp_f').alias('maxtemp_f'),
    col('daily_data.mintemp_f').alias('mintemp_f'),
    col('daily_data.maxwind_mph').alias('maxwind_mph'),
    col('daily_data.totalprecip_in').alias('totalprecip_in'),
    col('daily_data.avghumidity').alias('avghumidity')
)


# df_explode.printSchema()  # See the structure
# df_explode.show(5, truncate=False)

df_explode.write.mode('overwrite').csv(docker_write_path, header=True)


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




