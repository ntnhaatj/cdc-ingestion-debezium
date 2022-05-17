from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import DataStreamWriter


def save_df_to_jdbc(df: DataFrame, to_table: str, driver: str,
                    jdbc_url: str, user: str, password: str) -> None:

    (
        df
        .write
        .mode('append')
        .option("driver", driver)
        .jdbc(url=jdbc_url,
              table=to_table,
              properties={"user": user, "password": password})
    )


def get_jdbc_stream_writer(df: DataFrame, to_table: str, driver: str,
                           jdbc_url: str, user: str, password: str) -> DataStreamWriter:

    return (
        df
        .writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, _: save_df_to_jdbc(
                batch_df, to_table, driver, jdbc_url, user, password))
    )


def get_console_stream_writer(df: DataFrame, truncate=False):
    return (
        df
        .writeStream
        .format("console")
        .option("truncate", truncate)
    )
