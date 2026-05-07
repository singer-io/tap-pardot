"""Test tap bookmark behavior."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class PardotBookmarkTest(BookmarkTest, PardotBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream."""

    bookmark_format = "%Y-%m-%d %H:%M:%S"
    initial_bookmarks = {
        "bookmarks": {
            "lists": {"updated_at": "2020-01-01T00:00:00Z"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_pardot_bookmark_test"

    def streams_to_test(self):
        # Only test 'lists' for bookmark behavior:
        # - campaigns uses complex multi-phase bookmark (id during sync, last_updated after)
        # - users has compound replication keys ["id", "updated_at"]
        # - Other streams excluded due to no data in demo account
        return {"lists"}

    def calculate_new_bookmarks(self):
        """Bookmarks that will narrow sync 2 to fewer records."""
        new_bookmarks = {
            "lists": {"updated_at": "2026-01-01 00:00:00"},
        }
        return new_bookmarks
