"""Mock integration tests for bookmark/state handling of excluded streams.

Verifies that bookmarks are written correctly and that resuming from a
previous state produces the expected incremental behavior.
"""
import unittest

from tap_pardot.streams import STREAM_OBJECTS

from .base import (
    EXCLUDED_STREAMS,
    PardotMockBaseTest,
    DEFAULT_RECORD_COUNT,
)


class TestBookmarkIdReplicationStreams(PardotMockBaseTest, unittest.TestCase):
    """Test bookmark behavior for IdReplicationStream streams."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def test_email_clicks_bookmark_advances(self):
        """email_clicks bookmark advances to last synced id."""
        catalog = self.select_streams(self.catalog, {'email_clicks'})
        state = {}
        messages = self.run_sync(self.client, catalog, state=state)

        state_msgs = self.get_state_messages(messages)
        self.assertGreater(len(state_msgs), 0)
        final_state = state_msgs[-1]['value']
        bookmark = final_state['bookmarks']['email_clicks']['id']
        self.assertEqual(bookmark, DEFAULT_RECORD_COUNT)

    def test_email_clicks_resumes_from_bookmark(self):
        """email_clicks skips already-synced records when resuming."""
        catalog = self.select_streams(self.catalog, {'email_clicks'})
        # Simulate previous sync that got through id=2
        state = {'bookmarks': {'email_clicks': {'id': 2}}}
        messages = self.run_sync(self.client, catalog, state=state)

        records = self.get_records_from_messages(messages, 'email_clicks')
        # Should only get record with id=3 (records after id=2)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['id'], 3)

    def test_visitor_activities_bookmark_advances(self):
        """visitor_activities bookmark advances to last synced id."""
        catalog = self.select_streams(self.catalog, {'visitor_activities'})
        state = {}
        messages = self.run_sync(self.client, catalog, state=state)

        state_msgs = self.get_state_messages(messages)
        final_state = state_msgs[-1]['value']
        bookmark = final_state['bookmarks']['visitor_activities']['id']
        self.assertEqual(bookmark, DEFAULT_RECORD_COUNT)


class TestBookmarkUpdatedAtStreams(PardotMockBaseTest, unittest.TestCase):
    """Test bookmark behavior for UpdatedAtReplicationStream streams."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def test_prospect_accounts_bookmark_advances(self):
        """prospect_accounts bookmark advances to last updated_at."""
        catalog = self.select_streams(self.catalog, {'prospect_accounts'})
        state = {}
        messages = self.run_sync(self.client, catalog, state=state)

        state_msgs = self.get_state_messages(messages)
        final_state = state_msgs[-1]['value']
        bookmark = final_state['bookmarks']['prospect_accounts']['updated_at']
        # Should be the last record's updated_at
        self.assertEqual(bookmark, '2024-06-17T10:00:00Z')

    def test_prospect_accounts_resumes_from_bookmark(self):
        """prospect_accounts skips older records when resuming."""
        catalog = self.select_streams(self.catalog, {'prospect_accounts'})
        # Only records after 2024-06-16 should come through
        state = {'bookmarks': {'prospect_accounts': {
            'updated_at': '2024-06-16T10:00:00Z'
        }}}
        messages = self.run_sync(self.client, catalog, state=state)
        records = self.get_records_from_messages(messages, 'prospect_accounts')
        # Only the last record (2024-06-17) is after the bookmark
        self.assertEqual(len(records), 1)

    def test_visitors_bookmark_advances(self):
        """visitors bookmark advances to last updated_at."""
        catalog = self.select_streams(self.catalog, {'visitors'})
        state = {}
        messages = self.run_sync(self.client, catalog, state=state)

        state_msgs = self.get_state_messages(messages)
        final_state = state_msgs[-1]['value']
        bookmark = final_state['bookmarks']['visitors']['updated_at']
        self.assertEqual(bookmark, '2024-06-17T10:00:00Z')


class TestBookmarkNoUpdatedAtSortingStreams(PardotMockBaseTest, unittest.TestCase):
    """Test bookmark behavior for NoUpdatedAtSortingStream streams."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def test_opportunities_bookmark_stores_updated_at(self):
        """opportunities stores max updated_at after sync completes."""
        catalog = self.select_streams(self.catalog, {'opportunities'})
        state = {}
        messages = self.run_sync(self.client, catalog, state=state)

        state_msgs = self.get_state_messages(messages)
        self.assertGreater(len(state_msgs), 0)
        final_state = state_msgs[-1]['value']
        bookmarks = final_state['bookmarks']['opportunities']
        # After full sync, id bookmark should be cleared, updated_at should exist
        self.assertIn('updated_at', bookmarks)
        self.assertNotIn('id', bookmarks)

    def test_opportunities_resumes_incremental(self):
        """opportunities only emits records updated after bookmark."""
        catalog = self.select_streams(self.catalog, {'opportunities'})
        # Set updated_at bookmark to filter; only newer records should emit
        state = {'bookmarks': {'opportunities': {
            'updated_at': '2024-06-16T10:00:00Z'
        }}}
        messages = self.run_sync(self.client, catalog, state=state)
        records = self.get_records_from_messages(messages, 'opportunities')
        # Only records with updated_at > bookmark should be emitted
        for rec in records:
            self.assertGreater(rec['updated_at'], '2024-06-16T10:00:00Z')


class TestBookmarkSecondSync(PardotMockBaseTest, unittest.TestCase):
    """Test that a second sync with no new data produces no records."""

    def test_second_sync_no_new_records(self):
        """After a full sync, a second sync with same data yields no records."""
        client = self._create_mock_client()
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, {'prospect_accounts'})

        # First sync
        state = {}
        messages = self.run_sync(client, catalog, state=state)
        state_msgs = self.get_state_messages(messages)
        final_state = state_msgs[-1]['value']

        # Second sync using final state from first
        messages2 = self.run_sync(client, catalog, state=final_state)
        records2 = self.get_records_from_messages(messages2, 'prospect_accounts')
        self.assertEqual(len(records2), 0)


if __name__ == '__main__':
    unittest.main()
