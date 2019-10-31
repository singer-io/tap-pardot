import inspect

import singer


class Stream:
    stream_name = None
    data_key = None
    endpoint = None
    key_properties = ["id"]
    replication_keys = []
    is_dynamic = False

    client = None
    config = None
    state = None

    def __init__(self, client, config, state):
        self.client = client
        self.state = state
        self.config = config

    def get_default_start(self):
        return self.config["start_date"]

    def get_params(self):
        return {}

    def get_bookmark(self):
        return (
            singer.bookmarks.get_bookmark(
                self.state, self.stream_name, self.replication_keys[0]
            )
            or self.get_default_start()
        )

    def update_bookmark(self, bookmark_value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_name, self.replication_keys[0], bookmark_value
        )
        singer.write_state(self.state)

    def sync(self):
        data = self.client.get(self.endpoint, **self.get_params())

        if data["result"] is None or data["result"].get("total_results") == 0:
            return

        last_bookmark_value = None

        records = data["result"][self.data_key]
        if isinstance(records, dict):
            records = [records]

        for rec in records:
            current_bookmark_value = rec[self.replication_keys[0]]
            if last_bookmark_value is None:
                last_bookmark_value = current_bookmark_value

            if current_bookmark_value < last_bookmark_value:
                raise Exception(
                    "Detected out of order data. Current bookmark value {} is less than last bookmark value {}".format(
                        current_bookmark_value, last_bookmark_value
                    )
                )
            self.update_bookmark(current_bookmark_value)
            yield rec


class IdReplicationStream(Stream):
    replication_keys = ["id"]

    def get_default_start(self):
        return 0

    def get_params(self):
        return {
            "created_after": self.config["start_date"],
            "id_greater_than": self.get_bookmark(),
        }


class UpdatedAtReplicationStream(Stream):
    replication_keys = ["updated_at"]

    def get_params(self):
        return {
            "updated_after": self.get_bookmark(),
            "sort_by": "updated_at",
            "sort_order": "ascending",
        }


class EmailClicks(IdReplicationStream):
    stream_name = "email_clicks"
    data_key = "emailClick"
    endpoint = "emailClick"

    is_dynamic = False


class VisitorActivities(IdReplicationStream):
    stream_name = "visitor_activities"
    data_key = "visitor_activity"
    endpoint = "visitorActivity"

    is_dynamic = False


class ProspectAccounts(UpdatedAtReplicationStream):
    stream_name = "prospect_accounts"
    data_key = "prospectAccount"
    endpoint = "prospectAccount"

    is_dynamic = True


class Prospects(UpdatedAtReplicationStream):
    stream_name = "prospects"
    data_key = "prospect"
    endpoint = "prospect"

    is_dynamic = False


class Opportunities(UpdatedAtReplicationStream):
    stream_name = "opportunities"
    data_key = "opportunity"
    endpoint = "opportunity"

    is_dynamic = False


STREAM_OBJECTS = {
    cls.stream_name: cls
    for cls in globals().values()
    if inspect.isclass(cls) and issubclass(cls, Stream) and cls.stream_name
}
