from . import settings
import requests
import backoff
import json
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=5,
                      max_time=60,
                      jitter=None)
def configure_mysql_connectors(hostname='localhost', port='8083'):
    url = f"http://{hostname}:{port}/connectors/"
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    return requests.post(url, data=json.dumps(settings.MYSQL_CONNECTOR_SETTING), headers=headers)


class KafkaTopic:
    @classmethod
    def from_table(cls, table) -> str:
        return ".".join([settings.CONNECTOR_NAMESPACE, settings.INCLUDED_DATABASE, table])


class StreamFactory:
    @classmethod
    def from_kafka_topic_with_schema(cls, spark_session: SparkSession, topic: str, schema: dict) -> DataFrame:
        # without inferring the schema from data, defined schema help to stream faster
        return (
            spark_session.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", settings.KAFKA_BOOSTRAP_SERVERS)
                .option("subscribe", topic)
                .schema(StructType.fromJson(schema))
                .load()
        )
