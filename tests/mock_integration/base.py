"""Base test class for mock integration tests of tap-pardot.

Provides a mock Pardot API client and helper methods to run the real
discover() and sync() pipeline against generated data.
"""
import io
import json
import os
import sys
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from singer import Catalog, metadata

from tap_pardot.discover import discover
from tap_pardot.sync import sync
from tap_pardot.streams import STREAM_OBJECTS

from .mock_data_generator import MockDataGenerator

SCHEMAS_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    '..', '..', 'tap_pardot', 'schemas'
)

# Number of mock records to generate per stream
DEFAULT_RECORD_COUNT = 3

# Streams that use client.post() (ChildStreams)
CHILD_STREAMS = {'visits', 'list_memberships'}

# Streams excluded from real integration tests (the focus of these mock tests)
EXCLUDED_STREAMS = {
    'email_clicks',
    'visitor_activities',
    'prospect_accounts',
    'opportunities',
    'visitors',
    'visits',
}

# All streams
ALL_STREAMS = set(STREAM_OBJECTS.keys())


class PardotMockBaseTest:
    """Base class providing mock client and pipeline helpers."""

    start_date = '2024-01-01T00:00:00Z'

    def get_config(self):
        return {
            'start_date': self.start_date,
            'email': 'test@example.com',
            'password': 'fake',
            'user_key': 'fake-user-key',
        }

    def get_mock_data_generator(self):
        return MockDataGenerator(SCHEMAS_DIR)

    def _build_api_response(self, stream_name, records):
        """Build a mock Pardot API response dict wrapping *records*."""
        stream_cls = STREAM_OBJECTS[stream_name]
        data_key = stream_cls.data_key
        return {
            'result': {
                'total_results': len(records),
                data_key: records if len(records) > 1 else records[0],
            }
        }

    def _empty_response(self):
        return {'result': None}

    def _create_mock_client(self, stream_records=None):
        """Create a mock Client that returns generated data.

        Args:
            stream_records: dict mapping stream_name -> list of record dicts.
                           If None, generates DEFAULT_RECORD_COUNT records per stream.
        """
        gen = self.get_mock_data_generator()

        if stream_records is None:
            stream_records = {}
            for stream_name in ALL_STREAMS:
                stream_cls = STREAM_OBJECTS[stream_name]
                records = gen.generate_records(stream_name, count=DEFAULT_RECORD_COUNT)
                # Ensure id fields are unique integers
                for i, rec in enumerate(records):
                    rec['id'] = i + 1
                # Set replication key values for proper ordering
                if 'updated_at' in stream_cls.replication_keys or (
                    hasattr(stream_cls, 'last_updated_at')
                ):
                    for i, rec in enumerate(records):
                        rec['updated_at'] = f'2024-06-{15 + i:02d}T10:00:00Z'
                stream_records[stream_name] = records

        self._stream_records = stream_records

        # Build endpoint -> stream_name mapping
        endpoint_map = {}
        for name, cls in STREAM_OBJECTS.items():
            endpoint_map[cls.endpoint] = name

        # Track call counts to handle pagination (return empty on 2nd call)
        call_counts = {}

        def mock_get(endpoint, **params):
            stream_name = endpoint_map.get(endpoint, endpoint)
            key = (stream_name, 'get')
            call_counts[key] = call_counts.get(key, 0) + 1

            records = stream_records.get(stream_name, [])

            # Support id_greater_than pagination
            id_gt = params.get('id_greater_than', None)
            if id_gt is not None and id_gt != 0:
                records = [r for r in records if r['id'] > id_gt]

            # Support updated_after filtering
            updated_after = params.get('updated_after')
            if updated_after and stream_name not in CHILD_STREAMS:
                records = [r for r in records
                           if r.get('updated_at', '9999') > updated_after]

            if not records:
                return self._empty_response()
            return self._build_api_response(stream_name, records)

        def mock_post(endpoint, **params):
            stream_name = endpoint_map.get(endpoint, endpoint)
            key = (stream_name, 'post')
            call_counts[key] = call_counts.get(key, 0) + 1

            records = stream_records.get(stream_name, [])

            # Handle offset-based pagination for visits
            offset = params.get('offset', 0)
            if offset and offset >= len(records):
                return self._empty_response()

            # Handle id_greater_than for list_memberships
            id_gt = params.get('id_greater_than')
            if id_gt is not None and id_gt != 0:
                records = [r for r in records if r['id'] > id_gt]

            # Filter by updated_after for list_memberships
            updated_after = params.get('updated_after')
            if updated_after:
                records = [r for r in records
                           if r.get('updated_at', '9999') > updated_after]

            if not records:
                return self._empty_response()
            return self._build_api_response(stream_name, records)

        def mock_describe(endpoint):
            """Return empty describe result (no dynamic fields in mock)."""
            return {'result': {'field': []}}

        client = MagicMock()
        client.get = MagicMock(side_effect=mock_get)
        client.post = MagicMock(side_effect=mock_post)
        client.describe = MagicMock(side_effect=mock_describe)
        return client

    def run_discover(self, client):
        """Run real discover() and return the Catalog."""
        return discover(client)

    def select_streams(self, catalog, stream_names):
        """Mark specified streams as selected in the catalog."""
        for entry in catalog.streams:
            if entry.tap_stream_id in stream_names:
                mdata = metadata.to_map(entry.metadata)
                mdata[()]['selected'] = True
                # Select all fields
                for key in mdata:
                    if key != ():
                        mdata[key]['selected'] = True
                entry.metadata = metadata.to_list(mdata)
        return catalog

    def run_sync(self, client, catalog, state=None, config=None):
        """Run real sync() and capture Singer messages from stdout."""
        if config is None:
            config = self.get_config()
        if state is None:
            state = {}

        output = io.StringIO()
        with redirect_stdout(output):
            sync(client, config, state, catalog)

        return self._parse_singer_messages(output.getvalue())

    @staticmethod
    def _parse_singer_messages(raw_output):
        """Parse Singer messages from captured stdout."""
        messages = []
        for line in raw_output.strip().split('\n'):
            if line.strip():
                try:
                    messages.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return messages

    def get_records_from_messages(self, messages, stream_name):
        """Extract RECORD messages for a given stream."""
        return [
            msg['record'] for msg in messages
            if msg.get('type') == 'RECORD'
            and msg.get('stream') == stream_name
        ]

    def get_schema_messages(self, messages, stream_name=None):
        """Extract SCHEMA messages, optionally filtered by stream."""
        schemas = [msg for msg in messages if msg.get('type') == 'SCHEMA']
        if stream_name:
            schemas = [s for s in schemas if s.get('stream') == stream_name]
        return schemas

    def get_state_messages(self, messages):
        """Extract STATE messages."""
        return [msg for msg in messages if msg.get('type') == 'STATE']
