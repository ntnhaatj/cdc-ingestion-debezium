import requests
import backoff
import json


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
            print(cls._instances)
        return cls._instances[cls]


class AvroSchemaRegistry(metaclass=SingletonMeta):
    def __init__(self, url):
        print(f"initializing local schema registry")
        self.schemas = self._get_all_schemas(url)
        print(f"done!\n{self.schemas}")

    @staticmethod
    @backoff.on_exception(backoff.expo,
                          (Exception,),
                          factor=10,
                          max_tries=5)
    def _get_all_schemas(base_url):
        endpoint_url = f'{base_url}/schemas'
        resp = requests.get(endpoint_url)
        return json.loads(resp.content)

    def get_schema(self, schema_id) -> dict:
        try:
            schema_str = list(filter(lambda s: s['id'] == schema_id, self.schemas))[0]['schema']
            return json.loads(schema_str)
        except Exception:
            raise LookupError(f"cannot find schema_id {schema_id} in registry")
