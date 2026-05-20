"""Mock integration tests for all-fields selection of excluded streams.

Verifies that when all fields are selected, every field from the schema
appears in the output records (or is transformed to null appropriately).
"""
import unittest

from singer import metadata

from tap_pardot.streams import STREAM_OBJECTS

from .base import EXCLUDED_STREAMS, PardotMockBaseTest


class TestAllFieldsExcludedStreams(PardotMockBaseTest, unittest.TestCase):
    """Test that all schema fields are replicated for excluded streams."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def _get_expected_fields(self, stream_name):
        """Get all fields from the catalog schema for a stream."""
        for entry in self.catalog.streams:
            if entry.tap_stream_id == stream_name:
                return set(entry.schema.to_dict()['properties'].keys())
        return set()

    def _get_automatic_fields(self, stream_name):
        """Get automatic-inclusion fields for a stream."""
        for entry in self.catalog.streams:
            if entry.tap_stream_id == stream_name:
                mdata_map = metadata.to_map(entry.metadata)
                auto = set()
                for key, val in mdata_map.items():
                    if key == ():
                        continue
                    if val.get('inclusion') == 'automatic':
                        auto.add(key[1])
                return auto
        return set()

    def test_email_clicks_all_fields(self):
        """email_clicks: all schema fields present in output records."""
        stream = 'email_clicks'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_visitor_activities_all_fields(self):
        """visitor_activities: all schema fields present in output records."""
        stream = 'visitor_activities'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_prospect_accounts_all_fields(self):
        """prospect_accounts: all schema fields present in output records."""
        stream = 'prospect_accounts'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_opportunities_all_fields(self):
        """opportunities: all schema fields present in output records."""
        stream = 'opportunities'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_visitors_all_fields(self):
        """visitors: all schema fields present in output records."""
        stream = 'visitors'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_visits_all_fields(self):
        """visits: all schema fields present in output records."""
        stream = 'visits'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        if records:
            for rec in records:
                self.assertEqual(set(rec.keys()), expected_fields)


class TestDiscoveryExcludedStreams(PardotMockBaseTest, unittest.TestCase):
    """Verify discovery produces correct catalog entries for excluded streams."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def test_all_excluded_streams_discovered(self):
        """All excluded streams appear in the catalog."""
        discovered = {entry.tap_stream_id for entry in self.catalog.streams}
        for stream in EXCLUDED_STREAMS:
            self.assertIn(stream, discovered)

    def test_replication_method_correct(self):
        """Excluded streams have INCREMENTAL replication method."""
        for entry in self.catalog.streams:
            if entry.tap_stream_id in EXCLUDED_STREAMS:
                mdata_map = metadata.to_map(entry.metadata)
                self.assertEqual(
                    mdata_map[()].get('forced-replication-method'),
                    'INCREMENTAL',
                    f"Stream {entry.tap_stream_id} should be INCREMENTAL"
                )

    def test_key_properties_in_catalog(self):
        """Excluded streams have correct key_properties in catalog."""
        for entry in self.catalog.streams:
            if entry.tap_stream_id in EXCLUDED_STREAMS:
                stream_cls = STREAM_OBJECTS[entry.tap_stream_id]
                self.assertEqual(
                    entry.key_properties,
                    stream_cls.key_properties,
                    f"Key properties mismatch for {entry.tap_stream_id}"
                )

    def test_replication_keys_marked_automatic(self):
        """Replication keys have inclusion=automatic in metadata."""
        for entry in self.catalog.streams:
            if entry.tap_stream_id in EXCLUDED_STREAMS:
                stream_cls = STREAM_OBJECTS[entry.tap_stream_id]
                mdata_map = metadata.to_map(entry.metadata)
                for rep_key in stream_cls.replication_keys:
                    key = ('properties', rep_key)
                    if key in mdata_map:
                        self.assertEqual(
                            mdata_map[key].get('inclusion'),
                            'automatic',
                            f"Replication key '{rep_key}' for "
                            f"{entry.tap_stream_id} should be automatic"
                        )

    def test_prospect_accounts_dynamic_schema(self):
        """prospect_accounts calls describe() and could have dynamic fields."""
        # Verify that discover called describe for prospect_accounts
        self.client.describe.assert_called()
        # The endpoint should be 'prospectAccount'
        call_args = [call[0][0] for call in self.client.describe.call_args_list]
        self.assertIn('prospectAccount', call_args)


if __name__ == '__main__':
    unittest.main()
