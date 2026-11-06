"""Test tap start date behavior."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class PardotStartDateTest(StartDateTest, PardotBaseTest):
    """Test the lists stream respects start_date.

    Lists (UpdatedAtReplicationStream): start_date becomes the initial
    'updated_after' API param. 3 of 4 lists have updated_at in Oct 2025;
    1 updated in May 2026. start_date_2 = 2026-01-24 → sync 1 = 4, sync 2 = 1.

    Excluded streams:
    - users: API filters by created_after, but the framework compares updated_at.
      Older users updated after start_date make those expectations incompatible.
      Created-after behavior is covered in tests/unittests/test_streams.py.
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
        return {"lists"}
