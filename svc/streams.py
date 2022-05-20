import logging
from typing import Tuple
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import TimestampType, StructType

from svc.helpers import KafkaTopic, spark_avro_deserializer, configure_mysql_connectors
from svc.schema import get_cdc_schema
from schemas import schema_registry
from svc import settings, writer


class ETLCDCPipeline(ABC):
    """ ETL Pipeline for CDC

    Extract from multiple streams
    Transform and Load to sink
    """

    def __init__(self, app_name, *args, load_to_console: bool = False, **kwargs):
        """
        :param app_name: spark session name
        :param load_to_console: to load transformed result to console
        """
        self.app_name = app_name
        self.load_to_console = load_to_console
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

        configure_mysql_connectors(
            settings.MYSQL_CONNECTOR_CONF,
            hostname=settings.DEBEZIUM_CONNECTOR_HOST,
            port=settings.DEBEZIUM_CONNECTOR_PORT)

        schema_registry.try_to_fetch_all_schemas()

    def _extract_from_kafka(self, table_name):
        df = (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", settings.KAFKA_BOOSTRAP_SERVERS)
                .option("subscribe", KafkaTopic.from_table(table_name))
                .option("failOnDataLoss", "false")
                .load()
        )
        logging.info(f"[{table_name} schema] {df.printSchema()}")
        return df

    @staticmethod
    def _deserialize_cdc_msg(df: DataFrame, cdc_schema: StructType):
        return (
            df.select(spark_avro_deserializer(col('value')).alias('value'))
                .withColumn('value', from_json('value', cdc_schema))
                .select(
                'value.op',
                'value.after.*',
                (col('value.ts_ms') / 1000).cast(TimestampType()).alias('ts'))
        )

    @abstractmethod
    def extract(self) -> Tuple[DataFrame, ...]:
        pass

    @abstractmethod
    def transform(self, dfs: Tuple[DataFrame, ...]) -> DataFrame:
        pass

    @abstractmethod
    def load(self, df: DataFrame):
        pass

    def __load(self, df: DataFrame):
        if self.load_to_console:
            console_writer = writer.get_console_stream_writer(df)
            console_writer.start()
        self.load(df)

    def process(self):
        self.__load(self.transform(self.extract()))


class CDCMySQLTable(ETLCDCPipeline):
    """ ETL Pipeline for CDC Single Source MySQL Table """

    def __init__(self, app_name: str, table_name: str):
        self.table_name = table_name
        super().__init__(app_name)

    def extract(self) -> Tuple[DataFrame, ...]:
        """ extract cdc messages """
        single_stream_df = self._extract_from_kafka(self.table_name)
        return single_stream_df,

    def transform(self, dfs: Tuple[DataFrame, ...]) -> DataFrame:
        cdc_schema = get_cdc_schema(self.table_name)
        deserialized_df = self._deserialize_cdc_msg(dfs[0], cdc_schema)
        return deserialized_df

    def load(self, df: DataFrame):
        mysql_writer = writer.get_jdbc_stream_writer(
            df,
            f"{self.table_name}_cdc",
            settings.JDBC_CONFIG['driver'],
            settings.JDBC_CONFIG['url'],
            settings.DB_CONFIG['user'],
            settings.DB_CONFIG['password'],
        )
        mysql_writer.start()


class ClickThroughRateStreaming(ETLCDCPipeline):
    """ ETL Pipeline for Click Through Rate Streaming Use Case using CDC """

    def __init__(self, app_name: str, impression_table: str, click_table: str):
        self.impression_table = impression_table
        self.click_table = click_table
        super().__init__(app_name)

    def extract(self) -> Tuple[DataFrame, ...]:
        """ Extract data from 2 CDC streams """
        impression_stream = self._extract_from_kafka(self.impression_table)
        click_stream = self._extract_from_kafka(self.click_table)
        return impression_stream, click_stream,

    def transform(self, dfs: Tuple[DataFrame, ...]) -> DataFrame:
        impression_df, click_df = dfs
        impression_df = impression_df.withWatermark("ts", "2 hours")
        click_df = click_df.withWatermark("ts", "3 hours")
        joined_df = (
            impression_df
                .alias('imp')
                .join(
                click_df.alias('clk'),
                expr("""
                imp.ads_id == clk.ads_id AND
                clk.ts >= imp.ts AND
                clk.ts <= imp.ts + interval 1 hour
                """),
                "leftOuter")
                .withColumnRenamed('clk.ts', 'click_ts')
                .withColumnRenamed('imp.ts', 'impression_ts')
                .drop('clk.ads_id')
                .select('imp.*', 'clk.*', 'click_ts', 'impression_ts'))
        return joined_df

    def load(self, df: DataFrame):
        writer.get_jdbc_stream_writer(
            df,
            "click_per_impression",
            settings.JDBC_CONFIG['driver'],
            settings.JDBC_CONFIG['url'],
            settings.DB_CONFIG['user'],
            settings.DB_CONFIG['password'],
        ).start()
