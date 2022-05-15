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
├── schemas/            # predefined avro schemas
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

## Notes
- to run `watcher` locally out of docker compose network, we should define `kafka` host in `/etc/hosts` to resolve name domain as kafka broker only setting for advertising `kafka:9092`.
```shell script
[14:14]nhat.nguyen: test > cat /etc/hosts | grep kafka
127.0.0.1 kafka
```
