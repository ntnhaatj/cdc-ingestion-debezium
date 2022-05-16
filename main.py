from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

from svc.helpers import configure_mysql_connectors, KafkaTopic, spark_avro_deserializer
from svc import settings


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


def write_to(df, fmt):
    json_schema = (
        StructType()
            .add("op", StringType())
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
            .select('json_value.op', 'json_value.after.*')
            .writeStream
            .format(fmt)
            .option("truncate", False)
            .start()
    )
    return query


def main():
    configure_mysql_connectors(
        hostname=settings.DEBEZIUM_CONNECTOR_HOST,
        port=settings.DEBEZIUM_CONNECTOR_PORT)

    spark = SparkSession.builder.getOrCreate()
    print(f"running spark version {spark.version}")

    customers_df = get_customer_df(spark)
    query = write_to(customers_df, 'console')
    query.awaitTermination()


if __name__ == '__main__':
    main()
