"""Test tap bookmark behavior."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class PardotBookmarkTest(BookmarkTest, PardotBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream."""

    bookmark_format = "%Y-%m-%d %H:%M:%S"
    initial_bookmarks = {
        "bookmarks": {
            "lists": {"updated_at": "2020-01-01T00:00:00Z"},
            "prospects": {"updated_at": "2020-01-01T00:00:00Z"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_pardot_bookmark_test"

    def streams_to_test(self):
        # Test streams with:
        # - single replication key (required by BookmarkTest base class)
        # - real data available in demo account
        # Excluded:
        # - campaigns: replication_keys=[] (wall-clock bookmark, not a record field)
        # - users/opportunities/visits: compound replication keys ["id", "updated_at"]
        # - list_memberships: compound replication keys ["id", "updated_at", "list_id"]
        # - email_clicks/visitor_activities/visitors/prospect_accounts: no data
        return {"lists", "prospects"}

    def calculate_new_bookmarks(self):
        """Bookmarks that will narrow sync 2 to fewer records.

        These values are chosen to be between the earliest and latest record
        timestamps so that the second sync returns a strict subset.
        """
        new_bookmarks = {
            "lists": {"updated_at": "2026-01-01 00:00:00"},
            "prospects": {"updated_at": "2026-05-06 23:00:00"},
        }
        return new_bookmarks
