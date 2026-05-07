"""Test tap start date behavior."""
import unittest
from base import PardotBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


@unittest.skip("All records in demo account have recent dates; cannot split by start_date.")
class PardotStartDateTest(StartDateTest, PardotBaseTest):
    """Test the tap respects the start_date configuration."""

    start_date_1 = "2020-01-01T00:00:00Z"
    start_date_2 = "2025-01-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_pardot_start_date_test"

    def streams_to_test(self):
        # Start date test requires records spanning both sides of start_date_2.
        # All list records were updated recently so both syncs return the same
        # count, causing assertGreater to fail. Skip until older data exists.
        return set()
