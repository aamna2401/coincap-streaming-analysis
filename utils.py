from pyspark.sql.dataframe import DataFrame
from kafkasparkconfig import MONGO_PKG
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession


def console_streaming(df: DataFrame) -> None:
    _ = df.writeStream.trigger(processingTime='2 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()


def write_row(batch_df: DataFrame, batch_id: int) -> None:
    batch_df = batch_df.withColumn("batch_id", lit(batch_id))
    batch_df.write.format("mongo").mode("append").save()


def create_spark_session(mongodb_uri: str, app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.mongodb.input.uri", mongodb_uri) \
        .config("spark.mongodb.output.uri", mongodb_uri) \
        .config("spark.jars.packages", MONGO_PKG) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_streaming_df(spark: SparkSession, kafka_server: str, topic: str,
                      offset: str) -> DataFrame:
    df_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", topic) \
        .option("startingOffsets", offset) \
        .load()
    df_values = df_stream.selectExpr("CAST(value AS STRING)", "timestamp")
    return df_values
