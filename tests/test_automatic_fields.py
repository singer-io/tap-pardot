"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import PardotBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class PardotAutomaticFieldsTest(MinimumSelectionTest, PardotBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_pardot_automatic_fields_test"

    def streams_to_test(self):
        # Exclude large/child streams
        streams_to_exclude = {"visits", "list_memberships", "prospects"}
        return self.expected_stream_names().difference(streams_to_exclude)
