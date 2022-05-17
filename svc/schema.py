from pyspark.sql.types import StructType, StringType, LongType


__OBJ_SCHEMA_MAP = {
    'customers': (
        StructType()
        .add("id", StringType())
        .add("first_name", StringType())
        .add("last_name", StringType())
        .add("email", StringType())),
    'addresses': (
        StructType()
        .add("id", StringType())
        .add("customer_id", StringType())
        .add("street", StringType())
        .add("city", StringType())
        .add("state", StringType())
        .add("zip", StringType())
        .add("type", StringType())),
}


def get_cdc_schema(obj_name: str):
    if obj_name not in __OBJ_SCHEMA_MAP.keys():
        raise NotImplementedError
    obj_schema = __OBJ_SCHEMA_MAP[obj_name]
    return (
        StructType()
        .add("op", StringType())
        .add("ts_ms", LongType())
        .add("before", obj_schema)
        .add("after", obj_schema)
    )
