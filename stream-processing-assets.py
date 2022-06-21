from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import mean, window, first
from pyspark.sql.types import StructField, ShortType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import utils


def apply_schema(df: DataFrame) -> DataFrame:
    struct_schema = StructType([
        StructField("id", StringType()),
        StructField("rank", ShortType()),
        StructField("symbol", StringType()),
        StructField("name", StringType()),
        StructField("supply", DoubleType()),
        StructField("maxSupply", DoubleType()),
        StructField("marketCapUsd", DoubleType()),
        StructField("volumeUsd24Hr", DoubleType()),
        StructField("priceUsd", DoubleType()),
        StructField("changePercent24Hr", DoubleType()),
        StructField("vwap24Hr", DoubleType()),
        StructField("explorer", StringType()),
    ])
    df2 = df.select(from_json(col("value"), struct_schema).alias("assets"),
                    "timestamp")
    return df2


def main(kafka_server: str, assets_topic: str, mongodb_uri: str,
         app_name: str) -> None:
    # creating spark structured streaming session
    spark: SparkSession = utils.create_spark_session(mongodb_uri, app_name)
    # stream df
    df = utils.read_streaming_df(spark, kafka_server, assets_topic, "latest")
    # apply schema
    df = apply_schema(df)

    # aggregation window with wait for late data
    column = "assets.id"
    df2 = df.withWatermark("timestamp", "60 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds", "5 seconds"), column) \
        .agg(first("assets.rank").alias("rank"),
             first("assets.symbol").alias("symbol"),
             first("assets.name").alias("name"),
             mean("assets.supply").alias("supply"),
             mean("assets.marketCapUsd").alias("marketCapUsd"),
             mean("assets.volumeUsd24Hr").alias("volumeUsd24Hr"),
             mean("assets.priceUsd").alias("priceUsd"),
             mean("assets.changePercent24Hr").alias("changePercent24Hr"),
             mean("assets.vwap24Hr").alias("vwap24Hr")) \
        .drop("window")
    utils.console_streaming(df2)

    df2.printSchema()
    df2.writeStream.trigger(processingTime='20 seconds') \
        .foreachBatch(utils.write_row).start().awaitTermination()


if __name__ == '__main__':
    APP_NAME = "CoinCap-Assets"
    KAFKA_SERVER = 'localhost:9092'
    ASSETS_TOPIC = "coincap_assets"
    MONGODB_HOST = 'localhost:27017'
    MONGODB_DATABASE = 'coincap'
    MONGODB_COLLECTION = 'assets'
    MONGODB_URI = "mongodb://" + MONGODB_HOST + "/" + MONGODB_DATABASE + "." \
                  + MONGODB_COLLECTION
    # mongodb_uri = MONGODB_URI
    # kafka_server = KAFKA_SERVER
    # assets_topic = ASSETS_TOPIC
    main(KAFKA_SERVER, ASSETS_TOPIC, MONGODB_URI, APP_NAME)
