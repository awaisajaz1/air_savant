import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, TimestampType
from pyspark.sql import DataFrameWriter

# Create spark session
def create_spark_session():
    s_session = None

    try:
        s_session = SparkSession.builder.appName("ApiStreamsFromKafka-2")\
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
            .getOrCreate()
        
        s_session.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f" Couldn't create the spark session due to exception: {e}")

    return s_session

# connect to kafka cluster
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

# flatten the nested objecteds
def process_topic_payload(spark_df):
    topic_schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("email", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("street_number", StringType(), True),
        StructField("sensor_data", StructType(
                [
                    StructField('sensor_id', StringType(),True),
                    StructField('sensor_type', StringType(),True),
                ]
            )
        ),
        StructField("vital_signs", StructType(
                [
                    StructField('blood_group', StringType(),True),
                    StructField('pulse_rate', StringType(),True),
                    StructField('respiration_rate', StringType(),True),
                    StructField('body_temperature', StringType(),True),
                ]
            )
        ),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("insertion_timestamp", StringType(), True),
    ])
    
    df_selection = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), topic_schema).alias('api_data')) \
            .select("api_data.first_name", 
                    "api_data.last_name",
                    "api_data.gender",
                    "api_data.email",
                    "api_data.dob",
                    "api_data.phone",
                    "api_data.country",
                    "api_data.city",
                    "api_data.postcode",
                    "api_data.street_number",
                    "api_data.sensor_data.sensor_id",
                    "api_data.sensor_data.sensor_type",
                    "api_data.vital_signs.blood_group",
                    "api_data.vital_signs.pulse_rate",
                    "api_data.vital_signs.respiration_rate",
                    "api_data.vital_signs.body_temperature",
                    "api_data.latitude",
                    "api_data.longitude",
                    "api_data.insertion_timestamp"
                    )
    
    return df_selection
    

if __name__ == "__main__":
    spark_connection = create_spark_session()
    
    if spark_connection is not None:
        api_df = connect_kafka_cluster(spark_connection)
        df_selection = process_topic_payload(api_df)
        
        # selected_df = df_selection.writeStream \
        #         .format("console") \
        #         .outputMode("append") \
        #         .trigger(processingTime='10 seconds') \
        #         .option("numRows",10)\
        #         .option("checkpointLocation", "./checkpoint") \
        #         .start()

        def foreach_batch_function(df, epoch_id):
            df.printSchema()
            df.show()
            df.write \
            .format("jdbc") \
            .mode("append") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://127.0.0.1:5432/postgres") \
            .option("dbtable", "api_data") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("truncate", True) \
            .save()

        writing_sink = df_selection.writeStream \
            .trigger(processingTime='12 seconds') \
            .foreachBatch(foreach_batch_function) \
            .start()
                
        writing_sink.awaitTermination()
        writing_sink.stop()