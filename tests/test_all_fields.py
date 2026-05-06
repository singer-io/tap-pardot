"""Test that all fields can be selected and synced."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class PardotAllFieldsTest(AllFieldsTest, PardotBaseTest):
    """Test that all fields for each stream are replicated."""

    @staticmethod
    def name():
        return "tap_tester_pardot_all_fields_test"

    def streams_to_test(self):
        # Exclude large/child streams
        streams_to_exclude = {"visits", "list_memberships", "prospects"}
        return self.expected_stream_names().difference(streams_to_exclude)
