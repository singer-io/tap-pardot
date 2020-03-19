import json
import os

import singer
from singer import Catalog, metadata

from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def _load_schemas(client):
    schemas = {}

    for filename in os.listdir(_get_abs_path("schemas")):
        path = _get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    for stream in schemas.keys():
        stream_object = STREAM_OBJECTS[stream]
        if stream_object.is_dynamic:
            # Add to schemas
            schemas[stream] = {
                "type": "object",
                "properties": {**schemas[stream]["properties"]},
            }

    return schemas


def discover(client):
    LOGGER.info("Starting discovery mode")
    raw_schemas = _load_schemas(client)
    streams = []

    for stream_name, schema in raw_schemas.items():
        # create and add catalog entry
        stream = STREAM_OBJECTS[stream_name]
        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "metadata": metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream.key_properties,
                valid_replication_keys=stream.replication_keys,
                replication_method=stream.replication_method,
            ),
            "key_properties": stream.key_properties,
        }
        streams.append(catalog_entry)

    return Catalog.from_dict({"streams": streams})
