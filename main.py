from pyspark.sql import SparkSession
from svc.helpers import configure_mysql_connectors
from svc import settings
import time


if __name__ == '__main__':
    configure_mysql_connectors(
        hostname=settings.DEBEZIUM_CONNECTOR_HOST,
        port=settings.DEBEZIUM_CONNECTOR_PORT)
    spark = SparkSession.builder.getOrCreate()
    print(spark.version)
    while True:
        time.sleep(1)
