import findspark
import os

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.1.2'

KAFKA_PKG = f'org.apache.spark:spark-sql-kafka-0-10_' \
            f'{SCALA_VERSION}:{SPARK_VERSION}'
MONGO_PKG = 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {MONGO_PKG},' \
                                    f'{KAFKA_PKG} pyspark-shell'

findspark.init()
