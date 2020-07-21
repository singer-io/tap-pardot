import singer
from singer import Transformer, metadata, utils

from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()

def translate_state(client, state, selected_streams):
    """
    Translate the state for streams that have changed their patterns
    """
    selected_stream_names = [s.tap_stream_id for s in selected_streams]
    if "visitor_activities" in selected_stream_names:
        bookmark_id = singer.bookmarks.get_bookmark(state, "visitor_activities", "id")
        if bookmark_id:
            bookmark_activity = client.get_specific("visitorActivity", bookmark_id)
            if bookmark_activity.get('visitor_activity', {}).get('updated_at'):
                new_bookmark = utils.strftime(utils.strptime_to_utc(bookmark_activity['visitor_activity']['updated_at']))
                singer.bookmarks.clear_bookmark(state, "visitor_activities", "id")
                singer.bookmarks.write_bookmark(state, "visitor_activities", "window_start", bookmark_activity['updated_at'])
            else:
                raise Exception("Could not translate state for visitor_activites, bookmarked activity is missing `updated_at` value")

def sync(client, config, state, catalog):
    selected_streams = list(catalog.get_selected_streams(state))
    translate_state(client, state, selected_streams)

    for stream in selected_streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        stream_object = STREAM_OBJECTS.get(stream_id)(client, config, state)

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        singer.write_schema(
            stream_id,
            stream_schema.to_dict(),
            stream_object.key_properties,
            stream_object.replication_keys,
        )

        LOGGER.info("Syncing stream: " + stream_id)

        with Transformer() as transformer:
            for rec in stream_object.sync():
                singer.write_record(
                    stream_id,
                    transformer.transform(
                        rec, stream.schema.to_dict(), metadata.to_map(stream.metadata),
                    ),
                )
