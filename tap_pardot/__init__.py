#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from .client import Client

REQUIRED_CONFIG_KEYS = ["start_date", "email", "password", "user_key"]
LOGGER = singer.get_logger()

STRING_TYPES = set(["text", "dropdown", "textarea"])
INTEGER_TYPES = set()
NUMBER_TYPES = set()
# TODO: Datetime types may not actually come through the `describe` endpoint.
DATETIME_TYPES = set()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas(client):
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    for stream in DYNAMIC_SCHEMAS.keys():
        # Client describe
        schema_response = client.describe(stream)
        # Parse Result into JSON Schema
        dynamic_schema_parts = parse_schema_description(schema_response)
        # Add to schemas
        schemas[stream] = {"type": "object",
                           "properties": {**schemas[stream]["properties"], **dynamic_schema_parts}}

    return schemas

def parse_schema_description(description):
    subschemas = {}
    for field in description['result']['field']:
        subschemas[field["@attributes"]["id"]] = translate_type_to_schema(field["@attributes"]["type"])
    return subschemas

def translate_type_to_schema(type):
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

def discover(client):
    raw_schemas = load_schemas(client)
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

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams

def sync(config, state, catalog):

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        if stream_id in selected_stream_ids:
            # TODO: sync code for stream goes here...
            LOGGER.info('Syncing stream:' + stream_id)
    return

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Client(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(client)
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog =  discover(client)

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
