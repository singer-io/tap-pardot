
# We need environment variables
import os
import requests
import xmltodict

AUTH_URL = "https://pi.pardot.com/api/login/version/3"

ENDPOINT_BASE = "https://pi.pardot.com/api/"

class Client():
    """
    Lightweight Client wrapper to allow switching between version 3 and 4
    API based on availability, if desired.
    """
    api_version = None
    api_key = None
    creds = None
    TEST_URL = None
    # TODO: This could probably be refactored
    endpoint_map = {
        "emailClick": "emailClick/version/{}/do/query",
        "prospectAccount": "prospectAccount/version/{}/do/query",
        "visitorActivity": "visitorActivity/version/{}/do/query",
    }

    def __init__(self, creds):
        # Do login
        self.creds = creds
        self.login(creds)

    def login(self, creds):
        response = requests.post(AUTH_URL,
                                 data={
                                     "email": creds["email"],
                                     "password": creds["password"],
                                     "user_key": creds["user_key"]
                                 },
                                 params={"format":"json"})

        # This will only work if they use HTTP codes. Handling Pardot
        # errors below.
        response.raise_for_status()

        content = response.json()
        
        error_message = content.get("err")
        if error_message:
            error_code = content["@attributes"]["err_code"] # E.g., "15" for login failed
            raise Exception("Pardot returned error code {} while authenticating. Message: {}".format(error_code, error_message))

        self.api_version = content['version']
        self.api_key = content['api_key']

    def get(self, endpoint, format_params=None, **kwargs):
        # Not worrying about a backoff pattern for the spike
        # Error code 1 indicates a bad api_key or user_key
        # If we get error code 1 then re-authenticate login
        # http://developer.pardot.com/kb/error-codes-messages/#error-code-1
        url = ENDPOINT_BASE + self.endpoint_map[endpoint]
        base_formatting = [self.api_version]
        if format_params:
            base_formatting.extend(format_params)
        url = url.format(*base_formatting)
        # TODO: Switch on version between the quirks of each? Out of
        # scope, not sure if this should be in the client or in the stream
        # implementation

        # TODO: In implementation, log the request (sanitized) at this point
        headers = {"Authorization": "Pardot api_key={}, user_key={}".format(self.api_key, self.creds["user_key"])}

        response = requests.get(url,
                                 headers=headers,
                                 params={"format":"json",
                                         "output": "bulk",
                                         **kwargs})

        return response

config = {
    "email": os.getenv("PARDOT_EMAIL"),
    "password": os.getenv("PARDOT_PASSWORD"),
    "user_key": os.getenv("PARDOT_USER_KEY"),
    # Other stuff???
}

# Questions:
# Which version of the API to use? Switch based on version data tag on login

test = Client(config)

#### Email Click Example
# Since it can't be sorted, we _can_ page by a window
#val = test.get('emailClick', **{"created_after":"2019-08-01", "created_before":"2019-08-02"})

# Email Click appears to be sorted by ID, this can be used for pagination and bookmarking
# WARNING: When writing the tap, you MUST assert this is true
#bookmark_id = None
#val = test.get('emailClick', **{"created_after":"2019-08-01", "created_before":"2019-08-02", "id_greater_than":bookmark_id or 0})

# Bookmarking on ID with start_date as the lower-bound on created_after
# This is what that pattern could look like with `start_date: 2018-08-01`
# ipdb> val = test.get('emailClick', **{"created_after":"2018-08-01", "id_greater_than":0})
# ipdb> val.json()["result"]["emailClick"][-1]["id"]
# 434900201
# ipdb> val = test.get('emailClick', **{"created_after":"2018-08-01", "id_greater_than":434900201})
# ipdb> val.json()["result"]["emailClick"][-1]["id"]
# 435007531

#### Prospect Accounts Example
#val = test.get("prospectAccount", **{"updated_after": "2019-08-01", "sort_by": "updated_at", "sort_order": "ascending"})

# WARNING: Validate that the sort order is appropriately applied.
# `update_at` comes back in this format '2019-03-22 11:26:02'

# Bookmark using updated_at and make `updated_after = bookmark or start_date`

# When Email click has a null value the key is omitted. When prospect
# account has a null value it is returned as None

#### Visitor Activity Example
#val = test.get("visitorActivity", **{"created_after": "2019-08-01", "sort_by": "created_at", "sort_order": "ascending"})

# val.json()['result']['visitor_activity']

# These appear to be immutable, so bookmark and sort on created_at

import ipdb
ipdb.set_trace()
