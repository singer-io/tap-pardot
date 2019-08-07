import singer
from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()

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
        LOGGER.info('Syncing stream:' + stream_id)
        for rec in stream_object.sync():
            singer.write_record(stream_id, rec)
    return

