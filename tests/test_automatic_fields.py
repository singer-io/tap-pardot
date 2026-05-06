"""Test that automatic fields are always replicated regardless of selection."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import AutomaticFieldsTest


class PardotAutomaticFieldsTest(AutomaticFieldsTest, PardotBaseTest):
    """Test that automatic fields (pk, replication keys) are always replicated."""

    @staticmethod
    def name():
        return "tap_tester_pardot_automatic_fields_test"

    def streams_to_test(self):
        # Exclude large/child streams
        streams_to_exclude = {"visits", "list_memberships", "prospects"}
        return self.expected_stream_names().difference(streams_to_exclude)
