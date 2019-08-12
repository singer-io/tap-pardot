import singer

class Stream():
    stream_name = None
    data_key = None
    key_properties = []
    replication_keys = []
    client = None
    config = None
    state = None
    is_dynamic = False

    def __init__(self, client, config, state):
       self.client = client
       self.state = state
       self.config = config

    def get_default_start(self):
        return self.config["start_date"]

    def get_params(self):
        return {}

    def get_bookmark(self):
        return singer.bookmarks.get_bookmark(self.state, self.stream_name, self.replication_keys[0]) \
            or self.get_default_start()

    def update_bookmark(self, bookmark_value):
        singer.bookmarks.write_bookmark(self.state, self.stream_name, self.replication_keys[0], bookmark_value)
        singer.write_state(self.state)

    def sync(self):
        data = self.client.get(self.stream_name, **self.get_params())

        if data['result'] is None or data['result'].get('total_results') == 0:
            return

        last_bookmark_value = None

        for rec in data['result'][self.data_key]:
            current_bookmark_value = rec[self.replication_keys[0]]
            if last_bookmark_value is None:
                last_bookmark_value = current_bookmark_value

            if current_bookmark_value < last_bookmark_value:
                raise Exception("Detected out of order data. Current bookmark value {} is less than last bookmark value {}".format(current_bookmark_value, last_bookmark_value))
            self.update_bookmark(current_bookmark_value)
            yield rec

class EmailClicks(Stream):
    stream_name = "email_clicks"
    replication_keys = ["id"]
    data_key = "emailClick"
    key_properties = ["id"]
    is_dynamic = False

    def get_default_start(self):
        return 0

    def get_params(self):
        return {"created_after": self.config["start_date"], "id_greater_than": self.get_bookmark()}

class VisitorActivities(Stream):
    stream_name = "visitor_activities"
    data_key = "visitor_activity"
    replication_keys = ["id"]
    key_properties = ["id"]
    is_dynamic = False

    def get_default_start(self):
        return 0

    def get_params(self):
        return {"created_after": self.config["start_date"], "id_greater_than": self.get_bookmark()}

class ProspectAccounts(Stream):
    stream_name = "prospect_accounts"
    data_key = "prospectAccount"
    replication_keys = ["updated_at"]
    key_properties = ["id"]
    is_dynamic = True

    def get_params(self):
        return {"updated_after": self.get_bookmark(), "sort_by": "updated_at", "sort_order": "ascending"}

##############
# NEW STREAMS
##############
class Campaign(Stream):
    stream_name = "campaign"
    data_key = "campaign"
    replication_keys = ["updated_at"]
    key_properties = ["id"]
    is_dynamic = False

    def get_params(self):
        return {"updated_after": self.get_bookmark(), "sort_by": "updated_at", "sort_order": "ascending"}

STREAM_OBJECTS = {
    'email_clicks': EmailClicks,
    'visitor_activities': VisitorActivities,
    'prospect_accounts': ProspectAccounts,
    'campaign': Campaign,
}
