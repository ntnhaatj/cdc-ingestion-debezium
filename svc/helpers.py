import requests
import backoff
import json
from io import BytesIO
from struct import unpack
import logging

from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType
from fastavro import (parse_schema, schemaless_reader)

from svc import settings
from svc.exceptions import SerializationError
from schemas import schema_registry


@backoff.on_exception(backoff.expo,
                      (Exception,),
                      factor=10,
                      max_tries=5)
def configure_mysql_connectors(conf, hostname='localhost', port='8083'):
    logging.info("configuring MySQL Kafka connectors")
    logging.info(conf)

    url = f"http://{hostname}:{port}/connectors/"
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    return requests.post(url, data=json.dumps(conf), headers=headers)


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


class __ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


@udf(returnType=StringType())
def spark_avro_deserializer(value) -> json:
    _MAGIC_BYTE = 0

    if value is None:
        return None

    if len(value) <= 5:
        raise SerializationError("Message too small. This message was not"
                                 " produced with a Confluent"
                                 " Schema Registry serializer")
    with __ContextStringIO(value) as payload:
        magic, schema_id = unpack('>bI', payload.read(5))
        if magic != _MAGIC_BYTE:
            raise SerializationError("Unknown magic byte. This message was"
                                     " not produced with a Confluent"
                                     " Schema Registry serializer")
        parsed_schema = parse_schema(schema_registry.get_schema(schema_id))
        obj_dict = schemaless_reader(payload, parsed_schema)
        return json.dumps(obj_dict)