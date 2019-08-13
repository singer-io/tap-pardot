import singer
from singer import metadata
from singer import Transformer
from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()

def sync_page(stream_id, stream_object, transformer, catalog_entry):
    records_synced = 0
    for rec in stream_object.sync():
        singer.write_record(stream_id, transformer.transform(rec,
                                                             catalog_entry.schema.to_dict(),
                                                             metadata.to_map(catalog_entry.metadata)))
        records_synced += 1
    return bool(records_synced)

def sync(client, config, state, catalog):
    selected_streams = catalog.get_selected_streams(state)

    for stream in selected_streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        stream_object = STREAM_OBJECTS.get(stream_id)(client, config, state)

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        singer.write_schema(stream_id, stream_schema.to_dict(), stream_object.key_properties, stream_object.replication_keys)
        LOGGER.info("Starting discovery mode")
        LOGGER.info('Syncing stream: ' + stream_id)
        records_synced = True
        with Transformer() as transformer:
            while records_synced:
                records_synced = sync_page(stream_id, stream_object, transformer, stream)
    return
