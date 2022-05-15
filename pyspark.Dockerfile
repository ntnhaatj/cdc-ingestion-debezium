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

ENTRYPOINT ["python"]
