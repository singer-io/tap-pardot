"""Mock integration tests for all-fields selection across all streams.

Verifies that when all fields are selected, every field from the schema
appears in the output records (or is transformed to null appropriately).
Also validates that declared replication_keys exist in actual records.
"""
import unittest

from singer import metadata

from tap_pardot.streams import STREAM_OBJECTS

from .base import ALL_STREAMS, EXCLUDED_STREAMS, NON_CHILD_STREAMS, STREAMS_WITH_DATA, PardotMockBaseTest


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


class TestAllFieldsStreamsWithData(PardotMockBaseTest, unittest.TestCase):
    """Test that all schema fields are replicated for streams with real data."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def _get_expected_fields(self, stream_name):
        """Get all fields from the catalog schema for a stream."""
        for entry in self.catalog.streams:
            if entry.tap_stream_id == stream_name:
                return set(entry.schema.to_dict()['properties'].keys())
        return set()

    def test_campaigns_all_fields(self):
        """campaigns: all schema fields present in output records."""
        stream = 'campaigns'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_lists_all_fields(self):
        """lists: all schema fields present in output records."""
        stream = 'lists'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_prospects_all_fields(self):
        """prospects: all schema fields present in output records."""
        stream = 'prospects'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_users_all_fields(self):
        """users: all schema fields present in output records."""
        stream = 'users'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        self.assertGreater(len(records), 0)
        for rec in records:
            self.assertEqual(set(rec.keys()), expected_fields)

    def test_list_memberships_all_fields(self):
        """list_memberships: all schema fields present in output records."""
        stream = 'list_memberships'
        catalog = self.select_streams(self.catalog, {stream})
        messages = self.run_sync(self.client, catalog)
        records = self.get_records_from_messages(messages, stream)

        expected_fields = self._get_expected_fields(stream)
        if records:
            for rec in records:
                self.assertEqual(set(rec.keys()), expected_fields)


class TestAutomaticFieldsAllStreams(PardotMockBaseTest, unittest.TestCase):
    """Validate automatic fields (key_properties + replication_keys) across all streams.

    This test catches the bug where a stream declares replication_keys that
    don't exist in actual records (e.g. campaigns declaring 'id' as
    replication key while the real bookmark 'last_updated' is wall-clock time).
    """

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def test_replication_keys_exist_in_records(self):
        """Every declared replication_key must exist as a field in output records."""
        catalog = self.select_streams(self.catalog, NON_CHILD_STREAMS)
        messages = self.run_sync(self.client, catalog)

        for stream_name in NON_CHILD_STREAMS:
            stream_cls = STREAM_OBJECTS[stream_name]
            records = self.get_records_from_messages(messages, stream_name)
            if not records:
                continue
            for rep_key in stream_cls.replication_keys:
                for rec in records:
                    self.assertIn(
                        rep_key, rec,
                        f"Stream '{stream_name}' declares replication_key "
                        f"'{rep_key}' but it is missing from records. "
                        f"Record fields: {list(rec.keys())}"
                    )

    def test_replication_keys_in_schema(self):
        """Every declared replication_key must exist in the stream's JSON schema."""
        for entry in self.catalog.streams:
            stream_cls = STREAM_OBJECTS[entry.tap_stream_id]
            schema_fields = set(entry.schema.to_dict()['properties'].keys())
            for rep_key in stream_cls.replication_keys:
                self.assertIn(
                    rep_key, schema_fields,
                    f"Stream '{entry.tap_stream_id}' declares replication_key "
                    f"'{rep_key}' but it's not in the schema. "
                    f"Schema fields: {sorted(schema_fields)}"
                )

    def test_key_properties_exist_in_records(self):
        """Every declared key_property must exist in output records."""
        catalog = self.select_streams(self.catalog, NON_CHILD_STREAMS)
        messages = self.run_sync(self.client, catalog)

        for stream_name in NON_CHILD_STREAMS:
            stream_cls = STREAM_OBJECTS[stream_name]
            records = self.get_records_from_messages(messages, stream_name)
            if not records:
                continue
            for key_prop in stream_cls.key_properties:
                for rec in records:
                    self.assertIn(
                        key_prop, rec,
                        f"Stream '{stream_name}' declares key_property "
                        f"'{key_prop}' but it is missing from records."
                    )

    def test_automatic_fields_marked_in_metadata(self):
        """Key properties and replication keys are marked inclusion=automatic."""
        for entry in self.catalog.streams:
            stream_cls = STREAM_OBJECTS[entry.tap_stream_id]
            mdata_map = metadata.to_map(entry.metadata)

            # Key properties must be automatic
            for key_prop in stream_cls.key_properties:
                key = ('properties', key_prop)
                self.assertEqual(
                    mdata_map.get(key, {}).get('inclusion'), 'automatic',
                    f"Stream '{entry.tap_stream_id}': key_property "
                    f"'{key_prop}' should have inclusion=automatic"
                )

            # Replication keys must be automatic
            for rep_key in stream_cls.replication_keys:
                key = ('properties', rep_key)
                if key in mdata_map:
                    self.assertEqual(
                        mdata_map[key].get('inclusion'), 'automatic',
                        f"Stream '{entry.tap_stream_id}': replication_key "
                        f"'{rep_key}' should have inclusion=automatic"
                    )

    def test_schema_bookmark_properties_match_replication_keys(self):
        """SCHEMA message bookmark_properties matches stream's replication_keys."""
        catalog = self.select_streams(self.catalog, NON_CHILD_STREAMS)
        messages = self.run_sync(self.client, catalog)

        schema_msgs = [m for m in messages if m['type'] == 'SCHEMA']
        for msg in schema_msgs:
            stream_name = msg['stream']
            stream_cls = STREAM_OBJECTS[stream_name]
            expected_bookmark_props = stream_cls.replication_keys
            actual_bookmark_props = msg.get('bookmark_properties', [])
            self.assertEqual(
                actual_bookmark_props, expected_bookmark_props,
                f"Stream '{stream_name}': SCHEMA bookmark_properties "
                f"{actual_bookmark_props} != replication_keys {expected_bookmark_props}"
            )


class TestDiscoveryAllStreams(PardotMockBaseTest, unittest.TestCase):
    """Verify discovery produces correct catalog entries for all streams."""

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self.run_discover(self.client)

    def test_all_streams_discovered(self):
        """All streams appear in the catalog."""
        discovered = {entry.tap_stream_id for entry in self.catalog.streams}
        for stream in ALL_STREAMS:
            self.assertIn(stream, discovered)

    def test_replication_method_correct(self):
        """All streams have INCREMENTAL replication method."""
        for entry in self.catalog.streams:
            mdata_map = metadata.to_map(entry.metadata)
            self.assertEqual(
                mdata_map[()].get('forced-replication-method'),
                'INCREMENTAL',
                f"Stream {entry.tap_stream_id} should be INCREMENTAL"
            )

    def test_key_properties_in_catalog(self):
        """All streams have correct key_properties in catalog."""
        for entry in self.catalog.streams:
            stream_cls = STREAM_OBJECTS[entry.tap_stream_id]
            self.assertEqual(
                entry.key_properties,
                stream_cls.key_properties,
                f"Key properties mismatch for {entry.tap_stream_id}"
            )

    def test_replication_keys_marked_automatic(self):
        """Replication keys have inclusion=automatic in metadata."""
        for entry in self.catalog.streams:
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
