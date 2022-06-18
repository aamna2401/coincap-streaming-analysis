from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.functions import mean, window, first
from pyspark.sql.types import StructField, ShortType
from pyspark.sql.dataframe import DataFrame
from kafkasparkconfig import MONGO_PKG
from pyspark.sql import SparkSession


def console_streaming(df: DataFrame) -> None:
    _ = df.writeStream.trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()


def write_row(batch_df: DataFrame, batch_id: int) -> None:
    batch_df = batch_df.withColumn("batch_id", lit(batch_id))
    batch_df.write.format("mongo").mode("append").save()


def create_spark_session(mongodb_uri: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName("CoinCap") \
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


def apply_schema(df: DataFrame) -> DataFrame:
    struct_schema = StructType([
        StructField("exchangeId", StringType()),
        StructField("name", StringType()),
        StructField("rank", ShortType()),
        StructField("percentTotalVolume", DoubleType()),
        StructField("volumeUsd", DoubleType()),
        StructField("tradingPairs", ShortType()),
    ])
    df1 = df.select(from_json(col("value"), struct_schema).alias("exchanges"),
                    "timestamp")
    return df1


def main(kafka_server: str, exchange_topic: str, mongodb_uri: str) -> None:
    # creating spark structured streaming session
    spark: SparkSession = create_spark_session(mongodb_uri)
    # stream df
    df = read_streaming_df(spark, kafka_server, exchange_topic, "latest")
    # apply schema
    df = apply_schema(df)

    # aggregation window with wait for late data
    column = "exchanges.exchangeId"
    df2 = df.withWatermark("timestamp", "60 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds", "5 seconds"), column) \
        .agg(first("exchanges.name").alias("name"),
             first("exchanges.rank").alias("rank"),
             mean("exchanges.percentTotalVolume").alias("percentTotalVolume"),
             mean("exchanges.volumeUsd").alias("volumeUsd"),
             first("exchanges.tradingPairs").alias("tradingPairs"))\
        .drop("window")
    df3 = df2.select("*").alias("data")
    console_streaming(df3)

    df2.printSchema()
    df2.writeStream.trigger(processingTime='20 seconds') \
        .foreachBatch(write_row).start().awaitTermination()


if __name__ == '__main__':
    KAFKA_SERVER = 'localhost:9092'
    EXCHANGE_TOPIC = "coincap_exchanges"
    ASSETS_TOPIC = 'coincap_assets'
    MONGODB_USER = 'admin'
    MONGODB_PASSWORD = 'admin'
    MONGODB_HOST = 'localhost:27017'
    MONGODB_DATABASE = 'coincap'
    MONGODB_COLLECTION = 'exchanges'
    MONGODB_URI = "mongodb://" + MONGODB_HOST + "/" + MONGODB_DATABASE + "." \
                  + MONGODB_COLLECTION
    # mongodb_uri = MONGODB_URI
    # kafka_server = KAFKA_SERVER
    # exchange_topic = EXCHANGE_TOPIC
    # asset_topic = ASSETS_TOPIC
    main(KAFKA_SERVER, EXCHANGE_TOPIC, MONGODB_URI)
