"""Test tap start date behavior."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class PardotStartDateTest(StartDateTest, PardotBaseTest):
    """Test lists and users streams respect start_date.

    Lists (UpdatedAtReplicationStream): start_date becomes the initial
    'updated_after' API param. 3 of 4 lists have updated_at in Oct 2025;
    1 updated in May 2026. start_date_2 = 2026-01-24 → sync 1 = 4, sync 2 = 1.

    Users (NoUpdatedAtSortingStream): start_date becomes the 'created_after'
    API param. The framework validates using updated_at, so start_date_2 must
    be chosen such that no user has updated_at >= start_date_2 without also
    having created_at >= start_date_2. With 2026-01-24, only User 3
    (created Feb 9, updated Feb 9) qualifies → sync 1 = 3, sync 2 = 1.

    Excluded streams:
    - campaigns: wall-clock bookmark, not a record field.
    - prospects: all records updated in May 2026, cannot split.
    - list_memberships: all records created/updated same day, plus compound keys.
    - opportunities/visits: no data + same created_after/updated_at mismatch.
    - email_clicks/visitor_activities/visitors/prospect_accounts: no data.
    """

    start_date_1 = "2020-01-01T00:00:00Z"
    start_date_2 = "2026-01-24T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_pardot_start_date"

    def streams_to_test(self):
        return {"lists", "users"}

    def expected_replication_keys(self, stream=None):
        """Override to return only updated_at for users.

        The stream declares replication_keys = ["id", "updated_at"] but 'id'
        is transient (used for paging, cleared in post_sync). The framework
        requires len == 1, so we expose only the persisted key.
        """
        keys = super().expected_replication_keys()
        keys["users"] = {"updated_at"}
        if stream:
            return keys[stream]
        return keys
