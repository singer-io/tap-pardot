"""Test pagination behavior."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


class PardotPaginationTest(PaginationTest, PardotBaseTest):
    """Test the tap handles pagination correctly."""

    @staticmethod
    def name():
        return "tap_tester_pardot_pagination_test"

    def streams_to_test(self):
        # Exclude large/child streams
        streams_to_exclude = {"visits", "list_memberships", "prospects"}
        return self.expected_stream_names().difference(streams_to_exclude)
