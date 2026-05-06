"""Test tap start date behavior."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class PardotStartDateTest(StartDateTest, PardotBaseTest):
    """Test the tap respects the start_date configuration."""

    start_date_1 = "2020-01-01T00:00:00Z"
    start_date_2 = "2025-01-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_pardot_start_date_test"

    def streams_to_test(self):
        # Exclude child streams and large streams
        streams_to_exclude = {"visits", "list_memberships", "prospects"}
        return self.expected_stream_names().difference(streams_to_exclude)
