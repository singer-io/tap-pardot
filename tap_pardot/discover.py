import os
import json
from singer import metadata, Catalog
from .streams import STREAM_OBJECTS

STRING_TYPES = set(["text", "dropdown", "textarea"])
INTEGER_TYPES = set()
NUMBER_TYPES = set()
DATETIME_TYPES = set() # TODO: Datetime types may not actually come through the `describe` endpoint.

def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def _translate_type_to_schema(type):
    if type in STRING_TYPES:
        return {"type": ["null", "string"]}
    elif type in INTEGER_TYPES:
        return {"type": ["null", "integer"]}
    elif type in NUMBER_TYPES:
        return {"type": ["null", "number"]} # TODO: Precision?
    elif type in DATETIME_TYPES:
        return {"type": ["null", "string"],
                "format": "date-time"}
    else:
        raise Exception("Bad schema type {}".format(type))

def _parse_schema_description(description):
    subschemas = {}
    for field in description['result']['field']:
        subschemas[field["@attributes"]["id"]] = _translate_type_to_schema(field["@attributes"]["type"])
    return subschemas

# Load schemas from schemas folder
def _load_schemas(client):
    schemas = {}

    for filename in os.listdir(_get_abs_path('schemas')):
        path = _get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    for stream in schemas.keys():
        if STREAM_OBJECTS[stream].is_dynamic:
            # Client describe
            schema_response = client.describe(stream)
            # Parse Result into JSON Schema
            dynamic_schema_parts = _parse_schema_description(schema_response)
            # Add to schemas
            schemas[stream] = {"type": "object",
                               "properties": {**schemas[stream]["properties"], **dynamic_schema_parts}}

    return schemas

def discover(client):
    raw_schemas = _load_schemas(client)
    streams = []

    for stream_name, schema in raw_schemas.items():
        # create and add catalog entry
        stream = STREAM_OBJECTS[stream_name]
        catalog_entry = {
            'stream': stream_name,
            'tap_stream_id': stream_name,
            'schema': schema,
            'metadata' : metadata.get_standard_metadata(schema=schema,
                                                        key_properties=stream.key_properties,
                                                        valid_replication_keys=stream.replication_keys),
            'key_properties': stream.key_properties
        }
        streams.append(catalog_entry)

    return Catalog.from_dict({"streams": streams})
