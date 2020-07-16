import singer
from singer import Transformer, metadata, utils

from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()


def sync(client, config, state):
    for stream_id in STREAM_OBJECTS:
        stream_object = STREAM_OBJECTS.get(stream_id)(client, config, state)

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        LOGGER.info("Syncing stream: " + stream_id)

        for rec in stream_object.sync():
            singer.write_record(
                stream_id,
                rec
            )
