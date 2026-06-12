"""Test pagination behavior."""
import unittest
from base import PardotBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


@unittest.skip("Demo account has too few records to trigger pagination (page size 200).")
class PardotPaginationTest(PaginationTest, PardotBaseTest):
    """Test the tap handles pagination correctly."""

    @staticmethod
    def name():
        return "tap_tester_pardot_pagination_test"

    def streams_to_test(self):
        # Pardot API page size is 200 records. Demo account has only a few
        # records per stream, so pagination cannot be meaningfully tested.
        # Return empty set to effectively skip this test.
        return set()
