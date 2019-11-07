import inspect

import singer


class Stream:
    stream_name = None
    data_key = None
    endpoint = None
    key_properties = ["id"]
    replication_keys = []
    replication_method = None
    is_dynamic = False

    client = None
    config = None
    state = None

    _last_bookmark_value = None

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

    def pre_sync(self):
        """Function to run arbitrary code before a full sync starts."""

    def post_sync(self):
        """Function to run arbitrary code after a full sync completes."""

    def get_records(self):
        data = self.client.get(self.endpoint, **self.get_params())

        if data["result"] is None or data["result"].get("total_results") == 0:
            return []

        records = data["result"][self.data_key]
        if isinstance(records, dict):
            records = [records]
        return records

    def check_order(self, current_bookmark_value):
        if self._last_bookmark_value is None:
            self._last_bookmark_value = current_bookmark_value

        if current_bookmark_value < self._last_bookmark_value:
            raise Exception(
                "Detected out of order data. Current bookmark value {} is less than last bookmark value {}".format(
                    current_bookmark_value, self._last_bookmark_value
                )
            )

        self._last_bookmark_value = current_bookmark_value

    def sync_page(self):
        for rec in self.get_records():
            current_bookmark_value = rec[self.replication_keys[0]]
            self.check_order(current_bookmark_value)
            self.update_bookmark(current_bookmark_value)
            yield rec

    def sync(self):
        self.pre_sync()

        records_synced = 0
        last_records_synced = -1

        while records_synced != last_records_synced:
            last_records_synced = records_synced
            for rec in self.sync_page():
                records_synced += 1
                yield rec

        self.post_sync()


class IdReplicationStream(Stream):
    """
    Streams where records are immutable and can only be sorted by id.

    Syncing mechanism:

    - use bookmark to keep track of the id
    - sync records since the last bookmarked id
    """

    replication_keys = ["id"]
    replication_method = "INCREMENTAL"

    def get_default_start(self):
        return 0

    def get_params(self):
        return {
            "created_after": self.config["start_date"],
            "id_greater_than": self.get_bookmark(),
            "sort_by": "id",
            "sort_order": "ascending",
        }


class UpdatedAtReplicationStream(Stream):
    """
    Streams where records are mutable, can be sorted by updated_at, and return
    updated_at.

    Syncing mechanism:

    - use bookmark to keep track of last updated_at
    - sync records since the last bookmarked updated_at
    """

    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"

    def get_params(self):
        return {
            "updated_after": self.get_bookmark(),
            "sort_by": "updated_at",
            "sort_order": "ascending",
        }


class ComplexBookmarkStream(Stream):
    """Streams that need to keep track of more than 1 bookmark."""

    def clear_bookmark(self, bookmark_key):
        singer.bookmarks.clear_bookmark(self.state, self.stream_name, bookmark_key)
        singer.write_state(self.state)

    def get_bookmark(self, bookmark_key):
        return singer.bookmarks.get_bookmark(self.state, self.stream_name, bookmark_key)

    def update_bookmark(self, bookmark_key, bookmark_value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_name, bookmark_key, bookmark_value
        )
        singer.write_state(self.state)

    def sync_page(self):
        raise NotImplementedError("ComplexBookmarkStreams need a custom sync method.")


class NoUpdatedAtSortingStream(ComplexBookmarkStream):
    """
    Streams that can't sort by updated_at but have an updated_at field returned.

    Syncing mechanism:

    - get last updated_at bookmark
    - start full sync by id, starting at 0 and using id bookmark for paging
    - only emit records that have been updated since last sync
    - while iterating thorugh records, keep track of the max updated_at
    - when sync is finished, update the updated_at bookmark with max_updated_at
    """

    replication_keys = ["id", "updated_at"]
    replication_method = "INCREMENTAL"

    max_updated_at = None

    def post_sync(self):
        self.clear_bookmark("id")
        self.update_bookmark("updated_at", self.max_updated_at)

    def get_params(self):
        return {
            "created_after": self.config["start_date"],
            "id_greater_than": self.get_bookmark("id") or 0,
            "sort_by": "id",
            "sort_order": "ascending",
        }

    def sync_page(self):
        last_updated_at = self.get_bookmark("updated_at") or self.config["start_date"]
        self.max_updated_at = last_updated_at

        for rec in self.get_records():
            current_id = rec["id"]

            if rec["updated_at"] < last_updated_at:
                continue

            self.check_order(current_id)
            self.max_updated_at = max(self.max_updated_at, rec["updated_at"])
            self.update_bookmark("id", current_id)
            yield rec


class UpdatedAtSortByIdReplicationStream(ComplexBookmarkStream):
    """
    Streams that don't return an updated_at field but can be queried using
    updated_after.

    Syncing mechanism:

    - when a full sync starts, store current time in sync_start_time bookmark
    - if that bookmark exists, then we haven't finished a full sync and it'll
      pick up from where it left off.
    - use a last_updated bookmark to sync items updated_after last sync
    - start each full sync with id = 0 and sync all newly updated records paging
      by an id bookmark
    - when ful sync finishes, delete teh sync_start_time and id bookmarks and update
      last_updated bookmark to the sync_start_time
    """

    replication_keys = ["id"]
    replication_method = "INCREMENTAL"

    start_time = None

    def pre_sync(self):
        self.start_time = self.get_bookmark("sync_start_time")

        if self.start_time is None:
            self.start_time = singer.utils.strftime(singer.utils.now())
            self.update_bookmark("sync_start_time", self.start_time)

    def post_sync(self):
        self.clear_bookmark("sync_start_time")
        self.clear_bookmark("id")
        self.update_bookmark("last_updated", self.start_time)

    def get_params(self):
        return {
            "id_greater_than": self.get_bookmark("id") or 0,
            "updated_after": self.get_bookmark("last_updated")
            or self.config["start_date"],
            "sort_by": "id",
            "sort_order": "ascending",
        }

    def sync_page(self):
        for rec in self.get_records():
            current_id = rec["id"]
            self.check_order(current_id)
            self.update_bookmark("id", current_id)
            yield rec


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


class Opportunities(NoUpdatedAtSortingStream):
    stream_name = "opportunities"
    data_key = "opportunity"
    endpoint = "opportunity"

    is_dynamic = False


class Users(NoUpdatedAtSortingStream):
    stream_name = "users"
    data_key = "user"
    endpoint = "user"

    is_dynamic = False


class Lists(UpdatedAtReplicationStream):
    stream_name = "lists"
    data_key = "list"
    endpoint = "list"

    is_dynamic = False


class ListMemberships(NoUpdatedAtSortingStream):
    stream_name = "list_memberships"
    data_key = "list_membership"
    endpoint = "listMembership"

    is_dynamic = False


class Campaigns(UpdatedAtSortByIdReplicationStream):
    stream_name = "campaigns"
    data_key = "campaign"
    endpoint = "campaign"

    is_dynamic = False


STREAM_OBJECTS = {
    cls.stream_name: cls
    for cls in globals().values()
    if inspect.isclass(cls) and issubclass(cls, Stream) and cls.stream_name
}
