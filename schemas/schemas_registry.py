import requests
import json
import logging


class SchemaRegistryError(Exception):
    pass


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
            print(cls._instances)
        return cls._instances[cls]


class AvroSchemaRegistry(metaclass=SingletonMeta):
    def __init__(self, base_url):
        self.base_url = base_url
        self.schemas = None

    def fetch_all_schemas(self):
        logging.info(f"fetching schema registry")

        endpoint_url = f'{self.base_url}/schemas'
        resp = requests.get(endpoint_url)

        if resp.status_code != 200:
            raise requests.exceptions.RequestException(f"unexpected response {resp.content}")

        self.schemas = json.loads(resp.content)

        if not self.schemas:
            raise SchemaRegistryError()

        logging.info(f"done!\n{self.schemas}")

        return self.schemas

    def get_schema(self, schema_id) -> dict:
        try:
            schema_str = list(filter(lambda s: s['id'] == schema_id, self.schemas))[0]['schema']
            return json.loads(schema_str)
        except Exception:
            raise LookupError(f"cannot find schema_id {schema_id} in registry")
