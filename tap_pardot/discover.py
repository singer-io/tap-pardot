import json
import os

import singer
from singer import Catalog, metadata

from .client import PardotForbiddenError
from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def _parse_schema_description(description):
    subschemas = {}
    for field in description["result"]["field"]:
        # NB: Some fields have been observed to come through as objects.
        #     This was seen on type of 'dropdown' and 'text' with a value
        #     of either a string or integer, so these schemas are merged.
        subschemas[field["@attributes"]["id"]] = {
            "type": ["null", "string", "object"],
            "properties": {"value": {"type": ["null", "integer", "string"]}},
        }
    return subschemas


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
            try:
                # Client describe
                schema_response = client.describe(stream_object.endpoint)
                # Parse Result into JSON Schema
                dynamic_schema_parts = _parse_schema_description(schema_response)
                # Add to schemas
                schemas[stream] = {
                    "type": "object",
                    "properties": {**schemas[stream]["properties"], **dynamic_schema_parts},
                }
            except PardotForbiddenError:
                LOGGER.warning(
                    "Stream '%s' describe endpoint returned 403, skipping dynamic schema merge.",
                    stream,
                )

    return schemas


def _apply_access_checks(client, schemas):
    """
    Probe each parent stream for read access and remove inaccessible streams
    (and their children) from schemas in place.
    Child streams are not checked individually — their access is governed by
    the parent stream check.
    Raises PardotForbiddenError if no parent streams are accessible.
    """
    dummy_config = {"start_date": "2100-01-01T00:00:00Z"}
    dummy_state = {}

    inaccessible_streams = []

    # Check only parent streams for access
    for stream_name in list(schemas.keys()):
        stream_cls = STREAM_OBJECTS.get(stream_name)
        if stream_cls is None:
            continue
        # Skip child streams — access governed by parent
        if hasattr(stream_cls, 'parent_class') and stream_cls.parent_class is not None:
            continue
        stream_obj = stream_cls(client=client, config=dummy_config, state=dummy_state, emit=False)
        if not stream_obj.check_access():
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)

    # Prune children of inaccessible parents
    _prune_inaccessible_children(schemas)

    if inaccessible_streams:
        # Check if ALL parent streams are inaccessible
        total_parent_streams = len([
            name for name, cls in STREAM_OBJECTS.items()
            if not (hasattr(cls, 'parent_class') and cls.parent_class is not None)
        ])
        inaccessible_parent_count = len([
            name for name in inaccessible_streams
            if not (hasattr(STREAM_OBJECTS[name], 'parent_class') and STREAM_OBJECTS[name].parent_class is not None)
        ])
        if inaccessible_parent_count == total_parent_streams:
            raise PardotForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
                "of the streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            )
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following stream(s): %s. "
            "These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def _prune_inaccessible_children(schemas):
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas in place.
    """
    for name, stream_cls in list(STREAM_OBJECTS.items()):
        if name in schemas and hasattr(stream_cls, 'parent_class') and stream_cls.parent_class:
            parent_stream_name = stream_cls.parent_class.stream_name
            if parent_stream_name not in schemas:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                    name, parent_stream_name,
                )
                schemas.pop(name)


def discover(client):
    LOGGER.info("Starting discovery mode")
    raw_schemas = _load_schemas(client)

    _apply_access_checks(client, raw_schemas)

    streams = []

    for stream_name, schema in raw_schemas.items():
        # create and add catalog entry
        stream = STREAM_OBJECTS[stream_name]
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream.key_properties,
            valid_replication_keys=stream.replication_keys,
            replication_method=stream.replication_method,
        )
        # Mark replication keys as automatic inclusion
        mdata_map = metadata.to_map(mdata)
        for rep_key in (stream.replication_keys or []):
            if ('properties', rep_key) in mdata_map:
                mdata_map[('properties', rep_key)]['inclusion'] = 'automatic'
        mdata = metadata.to_list(mdata_map)

        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "metadata": mdata,
            "key_properties": stream.key_properties,
        }
        streams.append(catalog_entry)

    return Catalog.from_dict({"streams": streams})
