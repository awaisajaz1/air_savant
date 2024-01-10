import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


# Create spark session
def create_spark_session():
    s_session = None

    try:
        s_session = SparkSession.builder.appName("ApiStreamsFromKafka2")\
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
            .getOrCreate()
        
        s_session.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f" Couldn't create the spark session due to exception: {e}")

    return s_session

def connect_kafka_cluster(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe' , 'crowd_realtime_stream') \
            .option('startingOffsets' , 'earliest') \
            .load()
       
        
        logging.info("dataframe from kafak topic has been created!!!!")
    except Exception as e:
        logging.warning(f" It looks like soamething is wrong!!! : {e}")
    
    return spark_df

if __name__ == "__main__":
    spark_connection = create_spark_session()
    
    if spark_connection is not None:
        api_df = connect_kafka_cluster(spark_connection)
        query  = api_df.writeStream \
                .format("console") \
                .outputMode("append") \
                .trigger(processingTime='10 seconds') \
                .option("numRows",10)\
                .option("checkpointLocation", "./checkpoint") \
                .start()

        query.awaitTermination()