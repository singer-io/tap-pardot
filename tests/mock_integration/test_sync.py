"""Mock integration tests for sync pipeline of excluded streams.

Tests that the full discover -> sync pipeline works correctly for streams
that are excluded from real integration tests due to demo account data
limitations.
"""
import unittest

from tap_pardot.streams import STREAM_OBJECTS

from .base import (
    ALL_STREAMS,
    EXCLUDED_STREAMS,
    PardotMockBaseTest,
    DEFAULT_RECORD_COUNT,
)


class TestSyncExcludedStreams(PardotMockBaseTest, unittest.TestCase):
    """Verify each excluded stream produces RECORD messages through the pipeline."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def _sync_stream(self, stream_name):
        """Run sync for a single stream and return messages."""
        catalog = self.select_streams(self.catalog, {stream_name})
        messages = self.run_sync(self.client, catalog)
        return messages

    def test_email_clicks_sync(self):
        """email_clicks (IdReplicationStream) syncs records."""
        messages = self._sync_stream('email_clicks')
        records = self.get_records_from_messages(messages, 'email_clicks')
        self.assertEqual(len(records), DEFAULT_RECORD_COUNT)
        # Verify schema was written
        schemas = self.get_schema_messages(messages, 'email_clicks')
        self.assertEqual(len(schemas), 1)

    def test_visitor_activities_sync(self):
        """visitor_activities (IdReplicationStream) syncs records."""
        messages = self._sync_stream('visitor_activities')
        records = self.get_records_from_messages(messages, 'visitor_activities')
        self.assertEqual(len(records), DEFAULT_RECORD_COUNT)
        schemas = self.get_schema_messages(messages, 'visitor_activities')
        self.assertEqual(len(schemas), 1)

    def test_prospect_accounts_sync(self):
        """prospect_accounts (UpdatedAtReplicationStream, dynamic) syncs records."""
        messages = self._sync_stream('prospect_accounts')
        records = self.get_records_from_messages(messages, 'prospect_accounts')
        self.assertEqual(len(records), DEFAULT_RECORD_COUNT)
        schemas = self.get_schema_messages(messages, 'prospect_accounts')
        self.assertEqual(len(schemas), 1)

    def test_opportunities_sync(self):
        """opportunities (NoUpdatedAtSortingStream) syncs records."""
        messages = self._sync_stream('opportunities')
        records = self.get_records_from_messages(messages, 'opportunities')
        self.assertEqual(len(records), DEFAULT_RECORD_COUNT)
        schemas = self.get_schema_messages(messages, 'opportunities')
        self.assertEqual(len(schemas), 1)

    def test_visitors_sync(self):
        """visitors (UpdatedAtReplicationStream) syncs records."""
        messages = self._sync_stream('visitors')
        records = self.get_records_from_messages(messages, 'visitors')
        self.assertEqual(len(records), DEFAULT_RECORD_COUNT)
        schemas = self.get_schema_messages(messages, 'visitors')
        self.assertEqual(len(schemas), 1)

    def test_visits_sync(self):
        """visits (ChildStream + NoUpdatedAtSortingStream) syncs records."""
        # Visits requires parent Visitors data
        messages = self._sync_stream('visits')
        records = self.get_records_from_messages(messages, 'visits')
        # Visits may yield fewer records due to updated_at filtering
        self.assertGreater(len(records), 0)
        schemas = self.get_schema_messages(messages, 'visits')
        self.assertEqual(len(schemas), 1)


class TestSyncAllStreams(PardotMockBaseTest, unittest.TestCase):
    """Verify all 11 streams can sync together without errors."""

    def test_full_sync_all_streams(self):
        """All streams produce records when selected together."""
        client = self._create_mock_client()
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, ALL_STREAMS)
        messages = self.run_sync(client, catalog)

        # Every stream should have at least a SCHEMA message
        schema_streams = {msg['stream'] for msg in messages
                         if msg['type'] == 'SCHEMA'}
        self.assertEqual(schema_streams, ALL_STREAMS)

        # Every stream should produce at least one RECORD
        record_streams = {msg['stream'] for msg in messages
                         if msg['type'] == 'RECORD'}
        # Visits/list_memberships rely on parent data;
        # all other streams should have records
        for stream in ALL_STREAMS - {'visits', 'list_memberships'}:
            self.assertIn(stream, record_streams,
                          f"Stream {stream} produced no records")

    def test_state_messages_emitted(self):
        """Sync emits STATE messages for progress tracking."""
        client = self._create_mock_client()
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, {'email_clicks'})
        messages = self.run_sync(client, catalog)

        state_messages = self.get_state_messages(messages)
        self.assertGreater(len(state_messages), 0)


class TestSyncOutputSchemaConformance(PardotMockBaseTest, unittest.TestCase):
    """Verify synced records conform to the stream schema."""

    def test_records_have_key_properties(self):
        """Every RECORD message contains key_properties."""
        client = self._create_mock_client()
        catalog = self.run_discover(client)
        # Test excluded streams
        catalog = self.select_streams(catalog, EXCLUDED_STREAMS)
        messages = self.run_sync(client, catalog)

        for msg in messages:
            if msg['type'] != 'RECORD':
                continue
            stream_name = msg['stream']
            stream_cls = STREAM_OBJECTS[stream_name]
            for key_prop in stream_cls.key_properties:
                self.assertIn(key_prop, msg['record'],
                              f"Record for {stream_name} missing "
                              f"key property '{key_prop}'")

    def test_records_have_replication_keys(self):
        """Every RECORD message contains replication_keys."""
        client = self._create_mock_client()
        catalog = self.run_discover(client)
        # Only test non-child streams for replication key presence
        non_child = EXCLUDED_STREAMS - {'visits', 'list_memberships'}
        catalog = self.select_streams(catalog, non_child)
        messages = self.run_sync(client, catalog)

        for msg in messages:
            if msg['type'] != 'RECORD':
                continue
            stream_name = msg['stream']
            stream_cls = STREAM_OBJECTS[stream_name]
            for rep_key in stream_cls.replication_keys:
                self.assertIn(rep_key, msg['record'],
                              f"Record for {stream_name} missing "
                              f"replication key '{rep_key}'")


class TestParentChildStreamFlow(PardotMockBaseTest, unittest.TestCase):
    """Verify parent-child stream dependency works correctly."""

    def test_visits_uses_parent_visitor_ids(self):
        """visits stream fetches visitor IDs from parent before sync."""
        client = self._create_mock_client()
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, {'visits'})
        messages = self.run_sync(client, catalog)

        # Verify client.get was called for the parent (visitors endpoint)
        get_calls = [call for call in client.get.call_args_list
                     if call[0][0] == 'visitor']
        self.assertGreater(len(get_calls), 0,
                           "visits stream should fetch parent visitors via client.get()")

        # Verify client.post was called for visits with visitor_ids param
        post_calls = [call for call in client.post.call_args_list
                      if call[0][0] == 'visit']
        self.assertGreater(len(post_calls), 0,
                           "visits stream should call client.post('visit', ...)")
        # Check that visitor_ids param was passed
        for call in post_calls:
            self.assertIn('visitor_ids', call[1],
                          "visits post call should include 'visitor_ids' param")
            # visitor_ids should be comma-separated string of parent IDs
            self.assertIsInstance(call[1]['visitor_ids'], str)

    def test_list_memberships_uses_parent_list_ids(self):
        """list_memberships stream fetches list IDs from parent before sync."""
        client = self._create_mock_client()
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, {'list_memberships'})
        messages = self.run_sync(client, catalog)

        # Verify client.get was called for the parent (lists endpoint)
        get_calls = [call for call in client.get.call_args_list
                     if call[0][0] == 'list']
        self.assertGreater(len(get_calls), 0,
                           "list_memberships should fetch parent lists via client.get()")

        # Verify client.post was called for listMembership
        post_calls = [call for call in client.post.call_args_list
                      if call[0][0] == 'listMembership']
        self.assertGreater(len(post_calls), 0,
                           "list_memberships should call client.post('listMembership', ...)")


class TestVisitsFixPageViews(PardotMockBaseTest, unittest.TestCase):
    """Verify visits.fix_page_views() normalizes data in full pipeline."""

    def test_single_page_view_dict_normalized_to_list(self):
        """A single page_view dict is wrapped in a list during sync."""
        gen = self.get_mock_data_generator()

        # Create visit records where visitor_page_views has a single dict
        # (not a list) - this is the Pardot API quirk fix_page_views handles
        visit_records = gen.generate_records('visits', count=2)
        for i, rec in enumerate(visit_records):
            rec['id'] = i + 1
            rec['updated_at'] = f'2024-06-{16 + i:02d}T10:00:00Z'
            # Simulate Pardot returning a single page view as a dict (not list)
            rec['visitor_page_views'] = {
                'visitor_page_view': {
                    'id': 100 + i,
                    'url': f'https://example.com/page-{i}',
                    'title': f'Page {i}',
                    'created_at': '2024-06-15T10:00:00Z',
                }
            }

        # Set up visitors (parent) with matching data
        visitor_records = gen.generate_records('visitors', count=2)
        for i, rec in enumerate(visitor_records):
            rec['id'] = i + 1
            rec['updated_at'] = f'2024-06-{16 + i:02d}T10:00:00Z'

        stream_records = self._get_default_stream_records()
        stream_records['visits'] = visit_records
        stream_records['visitors'] = visitor_records

        client = self._create_mock_client(stream_records=stream_records)
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, {'visits'})
        messages = self.run_sync(client, catalog)

        records = self.get_records_from_messages(messages, 'visits')
        self.assertGreater(len(records), 0)
        for rec in records:
            page_views = rec['visitor_page_views']['visitor_page_view']
            self.assertIsInstance(page_views, list,
                                 "fix_page_views should normalize single dict to list")
            self.assertGreater(len(page_views), 0)


class TestZeroRecordStreams(PardotMockBaseTest, unittest.TestCase):
    """Verify streams with zero records emit SCHEMA but no RECORD messages."""

    def test_zero_records_emits_schema_only(self):
        """A stream with no API data emits SCHEMA message but no RECORDs."""
        stream_records = self._get_default_stream_records()
        stream_records['email_clicks'] = []

        client = self._create_mock_client(stream_records=stream_records)
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, {'email_clicks'})
        messages = self.run_sync(client, catalog)

        schemas = self.get_schema_messages(messages, 'email_clicks')
        self.assertEqual(len(schemas), 1, "Should emit SCHEMA even with no data")

        records = self.get_records_from_messages(messages, 'email_clicks')
        self.assertEqual(len(records), 0, "Should emit no RECORDs with empty data")

    def test_zero_records_multiple_streams(self):
        """Multiple streams with no data all emit SCHEMA but no RECORDs."""
        stream_records = self._get_default_stream_records()
        empty_streams = {'email_clicks', 'visitor_activities', 'opportunities'}
        for s in empty_streams:
            stream_records[s] = []

        client = self._create_mock_client(stream_records=stream_records)
        catalog = self.run_discover(client)
        catalog = self.select_streams(catalog, empty_streams)
        messages = self.run_sync(client, catalog)

        for stream in empty_streams:
            schemas = self.get_schema_messages(messages, stream)
            self.assertEqual(len(schemas), 1,
                             f"{stream} should emit SCHEMA")
            records = self.get_records_from_messages(messages, stream)
            self.assertEqual(len(records), 0,
                             f"{stream} should emit no RECORDs")


if __name__ == '__main__':
    unittest.main()
