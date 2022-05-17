import time
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col

from svc.helpers import configure_mysql_connectors, KafkaTopic, spark_avro_deserializer
from svc import settings
from svc import writer
from schemas import schema_registry
from svc.schema import get_cdc_schema


logging.basicConfig(level=logging.INFO)


def cdc_subscriber(table_name: str) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOSTRAP_SERVERS)
        .option("subscribe", KafkaTopic.from_table(table_name))
        .option("failOnDataLoss", "false")
        .load()
    )
    logging.info(f"[{table_name} schema] {df.printSchema()}")
    return df


def cdc_process(source_table_name):
    df = cdc_subscriber(source_table_name)
    cdc_schema = get_cdc_schema(source_table_name)
    deserialized_df = (
        df
        .select(spark_avro_deserializer(col('value')).alias('value'))
        .withColumn('value', from_json('value', cdc_schema))
        .select('value.op', 'value.after.*', 'value.ts_ms')
    )

    mysql_writer = writer.get_jdbc_stream_writer(
        deserialized_df,
        f"{source_table_name}_cdc",
        "com.mysql.jdbc.Driver",
        "jdbc:mysql://mysql:3306/inventory?rewriteBatchedStatements=true",
        os.environ.get('MYSQL_USER'),
        os.environ.get('MYSQL_PASSWORD'),
    )
    mysql_writer.start()

    console_writer = writer.get_console_stream_writer(deserialized_df)
    console_writer.start()
    return


def main():
    configure_mysql_connectors(
        settings.MYSQL_CONNECTOR_CONF,
        hostname=settings.DEBEZIUM_CONNECTOR_HOST,
        port=settings.DEBEZIUM_CONNECTOR_PORT)

    schema_registry.fetch_all_schemas()

    for tb in ("customers", "addresses",):
        cdc_process(tb)

    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
