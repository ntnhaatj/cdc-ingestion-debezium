import os

# Already downloaded in Dockerfile
SCALA_VERSION = "2.12"
SPARK_VERSION = "3.1.2"
os.environ['PYSPARK_SUBMIT_ARGS'] = f"--packages " \
                                    f"org.apache.spark:spark-avro_{SCALA_VERSION}:{SPARK_VERSION}," \
                                    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}," \
                                    f"mysql:mysql-connector-java:8.0.29 " \
                                    f"pyspark-shell"

# Debezium Connector Settings
DEBEZIUM_CONNECTOR_HOST = str(os.environ.get('DEBEZIUM_CONNECTOR_HOST'))
DEBEZIUM_CONNECTOR_PORT = str(os.environ.get('DEBEZIUM_CONNECTOR_PORT'))
KAFKA_BOOSTRAP_SERVERS = str(os.environ.get('BOOTSTRAP_SERVERS'))

# Do not change the value of this property.
# https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-database-server-name
CONNECTOR_NAMESPACE = "dbserver1"
INCLUDED_DATABASE = "inventory"
MYSQL_CONNECTOR_CONF = {
    "name": "inventory-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": str(os.environ.get('MYSQL_HOST')),
        "database.port": str(os.environ.get('MYSQL_PORT')),
        "database.user": "debezium",
        "database.password": "dbz",
        "database.server.id": "184054",
        "database.server.name": CONNECTOR_NAMESPACE,
        "database.include.list": INCLUDED_DATABASE,
        "database.history.kafka.bootstrap.servers": KAFKA_BOOSTRAP_SERVERS,
        "database.history.kafka.topic": "dbhistory.inventory",
    }
}
