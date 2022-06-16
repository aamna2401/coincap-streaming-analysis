from kafkasparkconfig import MONGO_PKG
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, array, lit
from pyspark.sql.types import StructType, StringType, ArrayType
from pyspark.sql.types import StructField, IntegerType
from pyspark.sql.dataframe import DataFrame


def console_streaming(df: DataFrame) -> None:
    stream = df.writeStream.trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()


def write_row(batch_df, batch_id):
    batch_df.write.format("mongo").mode("append").save()


def main(kafka_server: str, exchange_topic: str, assets_topic: str,
         mongodb_uri: str):
    spark = SparkSession \
        .builder \
        .appName("CoinCap") \
        .master("local[*]") \
        .config("spark.mongodb.input.uri", mongodb_uri) \
        .config("spark.mongodb.output.uri", mongodb_uri) \
        .config("spark.jars.packages", MONGO_PKG) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df_cc = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", exchange_topic) \
        .option("startingOffsets", "latest") \
        .load()
    df_values = df_cc.selectExpr("CAST(value AS STRING)", "timestamp")
    # console_streaming(df_values)

    struct_schema = StructType([
        StructField("data", ArrayType(StructType([
            StructField("exchangeId", StringType()),
            StructField("name", StringType()),
            StructField("rank", StringType()),
            StructField("percentTotalVolume", StringType()),
            StructField("volumeUsd", StringType()),
            StructField("tradingPairs", StringType()),
        ])))
    ])

    df3 = df_values.select(from_json(col("value"), struct_schema)
                           .alias("exchanges"), "timestamp")
    console_streaming(df3)
    # df3.printSchema()
    
    df3.writeStream.foreachBatch(write_row).start().awaitTermination()

    # df4 = df3.selectExpr("exchanges.data.exchangeId",
    #                      "exchanges.data.name",
    #                      "CAST(exchanges.data.rank AS SHORT)",
    #                      "CAST(exchanges.data.percentTotalVolume AS DOUBLE)",
    #                      "CAST(exchanges.data.volumeUsd AS DOUBLE)",
    #                      "CAST(exchanges.data.tradingPairs AS SHORT)")
    # console_streaming(df4)


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
    main(KAFKA_SERVER, EXCHANGE_TOPIC, ASSETS_TOPIC, MONGODB_URI)
