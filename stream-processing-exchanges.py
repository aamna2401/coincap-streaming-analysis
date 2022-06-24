from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import mean, window, first
from pyspark.sql.types import StructField, ShortType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import utils


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


def main(kafka_server: str, exchange_topic: str, mongodb_uri: str,
         app_name: str) -> None:
    # creating spark structured streaming session
    spark: SparkSession = utils.create_spark_session(mongodb_uri, app_name)
    # stream df
    df = utils.read_streaming_df(spark, kafka_server, exchange_topic, "latest")
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
             first("exchanges.tradingPairs").alias("tradingPairs")) \
        .drop("window")
    utils.console_streaming(df2)

    df2.printSchema()
    df2.writeStream.trigger(processingTime='20 seconds') \
        .foreachBatch(utils.write_row).start().awaitTermination()


if __name__ == '__main__':
    APP_NAME = "CoinCap-Exchanges"
    KAFKA_SERVER = 'localhost:9092'
    EXCHANGE_TOPIC = "coincap_exchanges"
    MONGODB_HOST = 'localhost:27017'
    MONGODB_DATABASE = 'coincap'
    MONGODB_COLLECTION = 'exchanges'
    MONGODB_URI = "mongodb://" + MONGODB_HOST + "/" + MONGODB_DATABASE + "." \
                  + MONGODB_COLLECTION
    # mongodb_uri = MONGODB_URI
    # kafka_server = KAFKA_SERVER
    # exchange_topic = EXCHANGE_TOPIC
    # asset_topic = ASSETS_TOPIC
    main(KAFKA_SERVER, EXCHANGE_TOPIC, MONGODB_URI, APP_NAME)
