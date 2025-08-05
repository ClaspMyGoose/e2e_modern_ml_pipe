FROM apache/spark-py:v3.4.0

WORKDIR /app 

# COPY requirements.txt .
USER root
RUN pip install --no-cache-dir pyspark==3.4.3
# USER spark 

COPY scripts/spark_weather_processing.py . 

CMD ["python3", "spark_weather_processing.py"]