import unittest
from unittest.mock import MagicMock, patch

from tap_pardot.sync import sync


class TestSync(unittest.TestCase):
    """Test sync function."""

    @patch("tap_pardot.sync.singer.write_record")
    @patch("tap_pardot.sync.singer.write_schema")
    @patch("tap_pardot.sync.Transformer")
    def test_sync_calls_write_schema(self, mock_transformer_cls, mock_write_schema, mock_write_record):
        """Test sync writes schema for selected streams."""
        mock_transformer = MagicMock()
        mock_transformer_cls.return_value.__enter__ = MagicMock(return_value=mock_transformer)
        mock_transformer_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_transformer.transform.return_value = {"id": 1}

        mock_catalog = MagicMock()
        mock_stream = MagicMock()
        mock_stream.tap_stream_id = "prospects"
        mock_stream.schema.to_dict.return_value = {"type": "object", "properties": {"id": {"type": ["integer"]}}}
        mock_stream.metadata = []
        mock_catalog.get_selected_streams.return_value = [mock_stream]

        client = MagicMock()
        config = {"start_date": "2020-01-01T00:00:00Z"}
        state = {}

        with patch("tap_pardot.sync.STREAM_OBJECTS") as mock_stream_objects:
            mock_stream_instance = MagicMock()
            mock_stream_instance.key_properties = ["id"]
            mock_stream_instance.replication_keys = ["updated_at"]
            mock_stream_instance.sync.return_value = iter([{"id": 1, "updated_at": "2020-01-02T00:00:00Z"}])
            mock_stream_objects.get.return_value = MagicMock(return_value=mock_stream_instance)

            sync(client, config, state, mock_catalog)

        mock_write_schema.assert_called_once_with(
            "prospects",
            {"type": "object", "properties": {"id": {"type": ["integer"]}}},
            ["id"],
            ["updated_at"],
        )

    @patch("tap_pardot.sync.singer.write_record")
    @patch("tap_pardot.sync.singer.write_schema")
    @patch("tap_pardot.sync.Transformer")
    def test_sync_writes_records(self, mock_transformer_cls, mock_write_schema, mock_write_record):
        """Test sync writes records for selected streams."""
        mock_transformer = MagicMock()
        mock_transformer_cls.return_value.__enter__ = MagicMock(return_value=mock_transformer)
        mock_transformer_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_transformer.transform.side_effect = lambda rec, schema, mdata: rec

        mock_catalog = MagicMock()
        mock_stream = MagicMock()
        mock_stream.tap_stream_id = "campaigns"
        mock_stream.schema.to_dict.return_value = {"type": "object", "properties": {"id": {"type": ["integer"]}}}
        mock_stream.metadata = []
        mock_catalog.get_selected_streams.return_value = [mock_stream]

        client = MagicMock()
        config = {"start_date": "2020-01-01T00:00:00Z"}
        state = {}

        with patch("tap_pardot.sync.STREAM_OBJECTS") as mock_stream_objects:
            records = [{"id": 1}, {"id": 2}, {"id": 3}]
            mock_stream_instance = MagicMock()
            mock_stream_instance.key_properties = ["id"]
            mock_stream_instance.replication_keys = ["id"]
            mock_stream_instance.sync.return_value = iter(records)
            mock_stream_objects.get.return_value = MagicMock(return_value=mock_stream_instance)

            sync(client, config, state, mock_catalog)

        self.assertEqual(mock_write_record.call_count, 3)
        # Verify correct stream_id and record data passed to write_record
        written_stream_ids = [call[0][0] for call in mock_write_record.call_args_list]
        self.assertTrue(all(sid == "campaigns" for sid in written_stream_ids))
        written_records = [call[0][1] for call in mock_write_record.call_args_list]
        self.assertEqual(written_records, [{"id": 1}, {"id": 2}, {"id": 3}])

    @patch("tap_pardot.sync.singer.write_record")
    @patch("tap_pardot.sync.singer.write_schema")
    @patch("tap_pardot.sync.Transformer")
    def test_sync_multiple_streams(self, mock_transformer_cls, mock_write_schema, mock_write_record):
        """Test sync processes multiple selected streams."""
        mock_transformer = MagicMock()
        mock_transformer_cls.return_value.__enter__ = MagicMock(return_value=mock_transformer)
        mock_transformer_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_transformer.transform.side_effect = lambda rec, schema, mdata: rec

        mock_catalog = MagicMock()
        stream1 = MagicMock()
        stream1.tap_stream_id = "prospects"
        stream1.schema.to_dict.return_value = {"type": "object", "properties": {}}
        stream1.metadata = []
        stream2 = MagicMock()
        stream2.tap_stream_id = "campaigns"
        stream2.schema.to_dict.return_value = {"type": "object", "properties": {}}
        stream2.metadata = []
        mock_catalog.get_selected_streams.return_value = [stream1, stream2]

        client = MagicMock()
        config = {"start_date": "2020-01-01T00:00:00Z"}
        state = {}

        with patch("tap_pardot.sync.STREAM_OBJECTS") as mock_stream_objects:
            mock_instance1 = MagicMock()
            mock_instance1.key_properties = ["id"]
            mock_instance1.replication_keys = ["updated_at"]
            mock_instance1.sync.return_value = iter([{"id": 1}])
            mock_instance2 = MagicMock()
            mock_instance2.key_properties = ["id"]
            mock_instance2.replication_keys = ["updated_at"]
            mock_instance2.sync.return_value = iter([{"id": 2}])
            factory = MagicMock(side_effect=[mock_instance1, mock_instance2])
            mock_stream_objects.get.return_value = factory

            sync(client, config, state, mock_catalog)

        self.assertEqual(mock_write_schema.call_count, 2)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch("tap_pardot.sync.singer.write_record")
    @patch("tap_pardot.sync.singer.write_schema")
    @patch("tap_pardot.sync.Transformer")
    def test_sync_no_selected_streams(self, mock_transformer_cls, mock_write_schema, mock_write_record):
        """Test sync does nothing when no streams are selected."""
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = []

        client = MagicMock()
        config = {"start_date": "2020-01-01T00:00:00Z"}
        state = {}

        sync(client, config, state, mock_catalog)

        mock_write_schema.assert_not_called()
        mock_write_record.assert_not_called()


if __name__ == "__main__":
    unittest.main()
