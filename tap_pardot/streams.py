import singer

# TODO: Update client and discovery to use these classes
class Stream():
    stream_name = None
    key_properties = []
    replication_keys = []
    client = None
    is_dynamic = False
    
    def __init__(self, client):
       self.client = client

    def get_bookmark(self):
        pass

    def update_bookmark(self, bookmark_value, bookmark_key=None):
        pass

    def sync(self, state):
        # Make requests using client
        # yield record values
        # sync will write those to output
        pass

class EmailClicks(Stream):
    stream_name = "email_clicks"
    replication_keys = ["id"]
    key_properties = ["id"]

class VisitorActivity(Stream):
    stream_name = "visitor_activity"
    replication_keys = ["created_at"]
    key_properties = ["id"]

class ProspectAccount(Stream):
    stream_name = "prospect_account"
    replication_keys = ["updated_at"]
    key_properties = ["id"]
    is_dynamic = True



STREAM_OBJECTS = {
    'email_clicks': EmailClicks,
    'visitor_activity': VisitorActivity,
    'prospect_account': ProspectAccount,
}
