import singer

LOGGER = singer.get_logger()

def sync(client, config, state, catalog):

    selected_streams = catalog.get_selected_streams(state)

    # Loop over streams in catalog
    for stream in selected_streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        # TODO: sync code for stream goes here...
        LOGGER.info('Syncing stream:' + stream_id)
    return

