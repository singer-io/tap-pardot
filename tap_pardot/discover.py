import os
import json
from singer import metadata

# TODO: Replace with class when implementing sync
SCHEMA_INFO = {
    "email_clicks": {
        "key_properties": ["id"],
        "valid_replication_keys": ["id"],
    },
    "visitor_activity": {
        "key_properties": ["id"],
        "valid_replication_keys": ["created_at"],
    },
}

DYNAMIC_SCHEMAS = {
    # TODO: prospect_account.assigned_to looks like a full `user` object. Denest to ID or email only?
    "prospect_account": {
        "key_properties": ["id"],
        "valid_replication_keys": ["updated_at"],
    }
}

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

    for stream in DYNAMIC_SCHEMAS.keys():
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

    for schema_name, schema in raw_schemas.items():
        # create and add catalog entry
        schema_entry = SCHEMA_INFO.get(schema_name) or DYNAMIC_SCHEMAS.get(schema_name)
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : metadata.get_standard_metadata(schema=schema,
                                                        key_properties=schema_entry["key_properties"],
                                                        valid_replication_keys=schema_entry["valid_replication_keys"]),
            'key_properties': schema_entry["key_properties"]
        }
        streams.append(catalog_entry)

    return {'streams': streams}
