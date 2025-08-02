
import os 

os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'
os.environ['PYSPARK_PYTHON'] = 'python'
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Weather Data Processing") \
    .master("local[*]") \
    .getOrCreate()


# local_read_path = '../data/*.json'
# local_write_path = '../processed_data/weather_output'

# docker_read_path = '/opt/airflow/data/*.json'
# docker_write_path = '/opt/airflow/processed_data/weather_output'


# df = spark.read.json(local_read_path)

# df.write.csv(local_write_path, header=True)

# spark.stop()




