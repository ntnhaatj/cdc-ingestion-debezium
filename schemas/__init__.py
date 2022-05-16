import os
from schemas.schemas_registry import *

schema_registry_host = os.environ.get('CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL', 'http://localhost:8083')
schema_registry = AvroSchemaRegistry(schema_registry_host)
