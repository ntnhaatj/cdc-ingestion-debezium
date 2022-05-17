ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

RUN python -m pip install --upgrade pip pipenv

WORKDIR /app
COPY ./Pipfile* /app/
RUN pipenv install --system --ignore-pipfile

RUN  apt-get update \
  && apt-get install -y curl \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir /jars \
    && cd /jars \
    && curl -L -o /jars/spark-avro_2.12-3.2.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.2.1/spark-avro_2.12-3.2.1.jar \
    && curl -L -o /jars/spark-sql-kafka-0-10_2.12-3.2.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar \
    && curl -L -o /jars/mysql-connector-java-8.0.29.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar
ENV CLASSPATH="/jars/*"

ENTRYPOINT ["python"]
