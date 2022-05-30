import os
import backoff
import requests

from schemas.schemas_registry import AvroSchemaRegistry, SchemaRegistryError


__all__ = ['try_to_fetch_all_schemas', 'get_schema']


schema_registry_host = os.environ.get('CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL', 'http://localhost:8083')
schema_registry = AvroSchemaRegistry(schema_registry_host)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException, SchemaRegistryError, ),
                      factor=5,
                      max_tries=5)
def try_to_fetch_all_schemas():
    schema_registry.fetch_all_schemas()


def get_schema(schema_id: str) -> dict:
    if not schema_registry.schemas:
        try_to_fetch_all_schemas()
    return schema_registry.get_schema(schema_id)
