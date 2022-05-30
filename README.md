# CDC Ingestion Architecture
- This repo aims for implementing `Capturing Data Change` system by using [Debezium](https://debezium.io/) and Structured Spark Streaming.
- Written in Python and used Docker Compose for demonstration.

![](./docs/CDCIngestion.png)

## Demo directories
```
.
├── docs/               # attached documentation
├── env/                # service environments
├── main.py             # main spark streaming service
├── pyspark.Dockerfile  # base image for running spark streaming service
├── schemas/            # avro schemas registry interfaces
└── svc/                # service utility package
```

## Instructions
- to launch POC demo
```shell script
$ docker-compose build && docker-compose up -d
```

- to observe the logging of particular service
```shell script
$ docker-compose logs -f <service>
```

- tear down all services
```shell script
$ docker-compose down
```

## Use cases
1) Table CDC Streaming
- Usage
```shell script
$ python main.py \
      --cls "svc.streams.CDCMySQLTable" \
      --opts "{\"app_name\": \"Customers Table CDC Stream\", \"table_name\": \"customers\"}"
```

- Tables with suffix `_cdc` is for capturing data changes on each source respectively
<img width="214" alt="Screen Shot 2022-05-18 at 09 27 15" src="https://user-images.githubusercontent.com/35696768/168944984-c93b83de-8f37-4b5e-adb2-3d93f71dc78e.png">

- For instance `customers` table schema
<img width="406" alt="Screen Shot 2022-05-18 at 09 27 40" src="https://user-images.githubusercontent.com/35696768/168945038-32464591-a78c-4e4a-b87b-dbda9070c8a8.png">

- CDC schema of `customers` table
<img width="671" alt="Screen Shot 2022-05-18 at 09 27 34" src="https://user-images.githubusercontent.com/35696768/168945075-e6ba4cc8-6580-4652-9ac1-be2eb31d7241.png">

- Notes:
  * `op` field captured the action on source table: `u` -> update, `c` -> create, `d` -> update

## Notes
- to launch a concrete stream which attached to existing systems
```shell script
$ docker-compose build  # to build cdc-ingestion image 
$ docker run -it --rm --name cdc-addresses-stream \
    --network your-compose-network \
    --volume ${PWD}:/app \
    --env-file env/kafka.env \
    --env-file env/mysql.env \
    --env-file env/connectors.env \
    --env-file env/streamsvc.env \
    cdc-ingestion \
    main.py \
      --cls "svc.streams.CDCMySQLTable" \
      --opts "{\"app_name\": \"arbitrary stream\", \"table_name\": \"addresses\"}"
```

- to run kafka consumer on particular topic for debugging
```shell script
$ docker run -it --rm --name avro-consumer \
    --network your-compose-network \
    debezium/connect:1.9 \
    /kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 \
      --property print.key=true \
      --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
      --property schema.registry.url=http://schema-registry:8081 \
      --topic dbserver1.inventory.customers
```

- this solution haven't synchronous the startup progress for services, as well as auto reconnect if some cases, thus some unexpected cases could be happened without the error, just need to restart services below `in order`:
```shell script
$ docker-compose restart connect            # http://localhost:8083/connectors/ is empty
$ docker-compose restart schema-registry    # http://localhost:8081/schemas/ is empty
$ docker-compose restart streamsvc          # to configure mysql connector
```
