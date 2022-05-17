import time
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType

from svc.helpers import configure_mysql_connectors, KafkaTopic, spark_avro_deserializer
from svc import settings
from schemas import schema_registry


logging.basicConfig(level=logging.INFO)


def get_customer_df(spark_session: SparkSession):
    df = (
        spark_session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOSTRAP_SERVERS)
        .option("subscribe", KafkaTopic.from_table('customers'))
        .option("failOnDataLoss", "false")
        .load()
    )
    print(f"customer schema {df.printSchema()}")
    return df


def process(df):
    json_schema = (
        StructType()
        .add("op", StringType())
        .add("ts_ms", LongType())
        .add("after", (
            StructType()
            .add("id", StringType())
            .add("first_name", StringType())
            .add("last_name", StringType())
            .add("email", StringType())))
    )

    query = (
        df.select(spark_avro_deserializer(col('value')).alias('value'))
        .withColumn('json_value', from_json('value', json_schema))
        .select('json_value.op', 'json_value.after.*', 'json_value.ts_ms')
    )

    def foreach_batch_function(df, epoch_id):
        properties = {
            "user": os.environ.get('MYSQL_USER'),
            "password": os.environ.get('MYSQL_PASSWORD'),
        }
        return (
            df.write
            .mode('append')
            .option("driver", "com.mysql.jdbc.Driver")
            .jdbc(url='jdbc:mysql://mysql:3306/inventory?rewriteBatchedStatements=true',
                  table="mysql_cdc",
                  properties=properties))

    sql_writer = (
        query
        .writeStream
        .outputMode("append")
        .foreachBatch(foreach_batch_function)
        .start()
    )

    console_writer = (
        query
        .writeStream
        .format("console")
        .option("truncate", False)
        .start()
    )

    return query


def main():
    configure_mysql_connectors(
        settings.MYSQL_CONNECTOR_CONF,
        hostname=settings.DEBEZIUM_CONNECTOR_HOST,
        port=settings.DEBEZIUM_CONNECTOR_PORT)

    schema_registry.fetch_all_schemas()

    spark = (
        SparkSession
        .builder
        .getOrCreate())
    print(f"running spark version {spark.version}")

    process(get_customer_df(spark))

    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
