import os
import unittest
from unittest.mock import MagicMock, patch, mock_open

from tap_pardot.discover import discover, _load_schemas, _get_abs_path, _parse_schema_description


class TestDiscover(unittest.TestCase):
    """Test discover function."""

    @patch("tap_pardot.discover._load_schemas")
    def test_discover_returns_catalog(self, mock_load_schemas):
        """Test discover returns a valid Catalog object."""
        mock_load_schemas.return_value = {
            "prospects": {
                "type": "object",
                "properties": {
                    "id": {"type": ["integer"]},
                    "email": {"type": ["null", "string"]},
                },
            }
        }

        client = MagicMock()
        catalog = discover(client)

        self.assertIsNotNone(catalog)
        self.assertEqual(len(catalog.streams), 1)
        self.assertEqual(catalog.streams[0].stream, "prospects")

    @patch("tap_pardot.discover._load_schemas")
    def test_discover_multiple_streams(self, mock_load_schemas):
        """Test discover returns catalog with multiple streams."""
        mock_load_schemas.return_value = {
            "prospects": {
                "type": "object",
                "properties": {
                    "id": {"type": ["integer"]},
                },
            },
            "campaigns": {
                "type": "object",
                "properties": {
                    "id": {"type": ["integer"]},
                    "name": {"type": ["null", "string"]},
                },
            },
        }

        client = MagicMock()
        catalog = discover(client)

        self.assertEqual(len(catalog.streams), 2)
        stream_names = {s.stream for s in catalog.streams}
        self.assertIn("prospects", stream_names)
        self.assertIn("campaigns", stream_names)

    @patch("tap_pardot.discover._load_schemas")
    def test_discover_sets_metadata(self, mock_load_schemas):
        """Test discover sets correct metadata on catalog entries."""
        mock_load_schemas.return_value = {
            "email_clicks": {
                "type": "object",
                "properties": {
                    "id": {"type": ["integer"]},
                },
            },
        }

        client = MagicMock()
        catalog = discover(client)

        stream = catalog.streams[0]
        self.assertEqual(stream.key_properties, ["id"])


class TestLoadSchemas(unittest.TestCase):
    """Test _load_schemas function."""

    @patch("tap_pardot.discover.os.listdir")
    @patch("builtins.open", new_callable=mock_open, read_data='{"type": "object", "properties": {"id": {"type": ["integer"]}}}')
    def test_load_schemas_non_dynamic(self, mock_file, mock_listdir):
        """Test loading schemas for non-dynamic streams."""
        mock_listdir.return_value = ["email_clicks.json"]

        client = MagicMock()
        schemas = _load_schemas(client)

        self.assertIn("email_clicks", schemas)
        self.assertEqual(schemas["email_clicks"]["type"], "object")

    @patch("tap_pardot.discover.os.listdir")
    @patch("builtins.open", new_callable=mock_open, read_data='{"type": "object", "properties": {"id": {"type": ["integer"]}}}')
    def test_load_schemas_dynamic_calls_describe(self, mock_file, mock_listdir):
        """Test loading schemas for dynamic streams calls describe."""
        mock_listdir.return_value = ["prospect_accounts.json"]

        client = MagicMock()
        client.describe.return_value = {
            "result": {
                "field": [
                    {"@attributes": {"id": "custom_field_1"}},
                ]
            }
        }

        schemas = _load_schemas(client)

        self.assertIn("prospect_accounts", schemas)
        client.describe.assert_called_once()
        # Verify dynamic fields are merged
        self.assertIn("custom_field_1", schemas["prospect_accounts"]["properties"])


class TestParseSchemaDescription(unittest.TestCase):
    """Test _parse_schema_description function."""

    def test_parse_single_field(self):
        """Test parsing a single field description."""
        description = {
            "result": {
                "field": [
                    {"@attributes": {"id": "custom_field_1"}},
                ]
            }
        }

        result = _parse_schema_description(description)

        self.assertIn("custom_field_1", result)
        self.assertEqual(result["custom_field_1"]["type"], ["null", "string", "object"])

    def test_parse_multiple_fields(self):
        """Test parsing multiple field descriptions."""
        description = {
            "result": {
                "field": [
                    {"@attributes": {"id": "field_a"}},
                    {"@attributes": {"id": "field_b"}},
                    {"@attributes": {"id": "field_c"}},
                ]
            }
        }

        result = _parse_schema_description(description)

        self.assertEqual(len(result), 3)
        self.assertIn("field_a", result)
        self.assertIn("field_b", result)
        self.assertIn("field_c", result)


class TestGetAbsPath(unittest.TestCase):
    """Test _get_abs_path function."""

    def test_returns_absolute_path(self):
        """Test _get_abs_path returns an absolute path."""
        result = _get_abs_path("schemas")
        self.assertTrue(os.path.isabs(result))
        self.assertTrue(result.endswith("schemas"))


if __name__ == "__main__":
    unittest.main()
