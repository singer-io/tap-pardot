"""Test tap bookmark behavior."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class PardotBookmarkTest(BookmarkTest, PardotBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S"
    initial_bookmarks = {
        "bookmarks": {
            "prospects": {"updated_at": "2020-01-01T00:00:00Z"},
            "lists": {"updated_at": "2020-01-01T00:00:00Z"},
            "visitors": {"updated_at": "2020-01-01T00:00:00Z"},
            "prospect_accounts": {"updated_at": "2020-01-01T00:00:00Z"},
            "email_clicks": {"id": 0},
            "visitor_activities": {"id": 0},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_pardot_bookmark_test"

    def streams_to_test(self):
        # Exclude child streams (visits, list_memberships) and prospects (too large)
        streams_to_exclude = {"visits", "list_memberships", "prospects"}
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Bookmarks that will narrow sync 2 to fewer records."""
        new_bookmarks = {
            "prospects": {"updated_at": "2025-01-01T00:00:00Z"},
            "lists": {"updated_at": "2025-01-01T00:00:00Z"},
            "visitors": {"updated_at": "2025-01-01T00:00:00Z"},
            "prospect_accounts": {"updated_at": "2025-01-01T00:00:00Z"},
            "campaigns": {"last_updated": "2025-01-01T00:00:00Z"},
            "opportunities": {"updated_at": "2025-01-01T00:00:00Z"},
            "users": {"updated_at": "2025-01-01T00:00:00Z"},
            "email_clicks": {"id": 999999},
            "visitor_activities": {"id": 999999},
        }
        return new_bookmarks
