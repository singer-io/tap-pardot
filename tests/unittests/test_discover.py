import os
import unittest
from unittest.mock import MagicMock, patch, mock_open

from tap_pardot.client import PardotForbiddenError
from tap_pardot.discover import discover, _load_schemas, _get_abs_path, _parse_schema_description, _apply_access_checks, _prune_inaccessible_children


class TestDiscover(unittest.TestCase):
    """Test discover function."""

    @patch("tap_pardot.discover._apply_access_checks")
    @patch("tap_pardot.discover._load_schemas")
    def test_discover_returns_catalog(self, mock_load_schemas, mock_access_checks):
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

    @patch("tap_pardot.discover._apply_access_checks")
    @patch("tap_pardot.discover._load_schemas")
    def test_discover_multiple_streams(self, mock_load_schemas, mock_access_checks):
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

    @patch("tap_pardot.discover._apply_access_checks")
    @patch("tap_pardot.discover._load_schemas")
    def test_discover_sets_metadata(self, mock_load_schemas, mock_access_checks):
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
        client.describe.assert_called_once_with("prospectAccount")
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


class TestApplyAccessChecks(unittest.TestCase):
    """Test _apply_access_checks function."""

    @patch("tap_pardot.discover.STREAM_OBJECTS")
    def test_all_streams_accessible(self, mock_stream_objects):
        """Test that all streams remain when all are accessible."""
        client = MagicMock()
        client.get.return_value = {"result": None}

        mock_prospects_cls = MagicMock()
        mock_prospects_instance = MagicMock()
        mock_prospects_instance.check_access.return_value = True
        mock_prospects_cls.return_value = mock_prospects_instance
        mock_prospects_cls.parent_class = None

        mock_stream_objects.get.return_value = mock_prospects_cls
        mock_stream_objects.items.return_value = [("prospects", mock_prospects_cls)]
        mock_stream_objects.__iter__ = lambda self: iter(["prospects"])

        schemas = {"prospects": {"type": "object", "properties": {}}}
        _apply_access_checks(client, schemas)

        self.assertIn("prospects", schemas)

    @patch("tap_pardot.discover.STREAM_OBJECTS")
    def test_inaccessible_stream_excluded(self, mock_stream_objects):
        """Test that inaccessible streams are excluded from schemas."""
        client = MagicMock()

        mock_prospects_cls = MagicMock()
        mock_prospects_instance = MagicMock()
        mock_prospects_instance.check_access.return_value = False
        mock_prospects_cls.return_value = mock_prospects_instance
        mock_prospects_cls.parent_class = None

        mock_campaigns_cls = MagicMock()
        mock_campaigns_instance = MagicMock()
        mock_campaigns_instance.check_access.return_value = True
        mock_campaigns_cls.return_value = mock_campaigns_instance
        mock_campaigns_cls.parent_class = None

        mock_stream_objects.get.side_effect = lambda k: {"prospects": mock_prospects_cls, "campaigns": mock_campaigns_cls}.get(k)
        mock_stream_objects.items.return_value = [("prospects", mock_prospects_cls), ("campaigns", mock_campaigns_cls)]
        mock_stream_objects.__getitem__ = lambda self, key: {"prospects": mock_prospects_cls, "campaigns": mock_campaigns_cls}[key]

        schemas = {
            "prospects": {"type": "object", "properties": {}},
            "campaigns": {"type": "object", "properties": {}},
        }
        _apply_access_checks(client, schemas)

        self.assertNotIn("prospects", schemas)
        self.assertIn("campaigns", schemas)

    @patch("tap_pardot.discover.STREAM_OBJECTS")
    def test_all_streams_inaccessible_raises(self, mock_stream_objects):
        """Test that PardotForbiddenError is raised when no streams are accessible."""
        client = MagicMock()

        mock_prospects_cls = MagicMock()
        mock_prospects_instance = MagicMock()
        mock_prospects_instance.check_access.return_value = False
        mock_prospects_cls.return_value = mock_prospects_instance
        mock_prospects_cls.parent_class = None
        mock_prospects_cls.stream_name = "prospects"

        mock_stream_objects.get.return_value = mock_prospects_cls
        mock_stream_objects.items.return_value = [("prospects", mock_prospects_cls)]
        mock_stream_objects.__getitem__ = lambda self, key: {"prospects": mock_prospects_cls}[key]

        schemas = {"prospects": {"type": "object", "properties": {}}}

        with self.assertRaises(PardotForbiddenError):
            _apply_access_checks(client, schemas)


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Test _prune_inaccessible_children function."""

    @patch("tap_pardot.discover.STREAM_OBJECTS")
    def test_child_excluded_when_parent_missing(self, mock_stream_objects):
        """Test child streams are removed when their parent is not in schemas."""
        from tap_pardot.streams import Visitors

        mock_visits_cls = MagicMock()
        mock_visits_cls.parent_class = Visitors
        mock_visits_cls.parent_class.stream_name = "visitors"

        mock_stream_objects.items.return_value = [("visits", mock_visits_cls)]

        schemas = {"visits": {"type": "object", "properties": {}}}
        _prune_inaccessible_children(schemas)

        self.assertNotIn("visits", schemas)

    @patch("tap_pardot.discover.STREAM_OBJECTS")
    def test_child_kept_when_parent_present(self, mock_stream_objects):
        """Test child streams remain when their parent is in schemas."""
        from tap_pardot.streams import Visitors

        mock_visits_cls = MagicMock()
        mock_visits_cls.parent_class = Visitors
        mock_visits_cls.parent_class.stream_name = "visitors"

        mock_stream_objects.items.return_value = [("visits", mock_visits_cls)]

        schemas = {
            "visitors": {"type": "object", "properties": {}},
            "visits": {"type": "object", "properties": {}},
        }
        _prune_inaccessible_children(schemas)

        self.assertIn("visits", schemas)


class TestStreamCheckAccess(unittest.TestCase):
    """Test Stream.check_access() method."""

    def test_check_access_returns_true_on_success(self):
        """Test check_access returns True when API call succeeds."""
        from tap_pardot.streams import Prospects

        client = MagicMock()
        client.get.return_value = {"result": None}

        stream = Prospects(client=client, config={"start_date": "2020-01-01T00:00:00Z"}, state={}, emit=False)
        self.assertTrue(stream.check_access())

    def test_check_access_returns_false_on_403(self):
        """Test check_access returns False when PardotForbiddenError is raised."""
        from tap_pardot.streams import Prospects

        client = MagicMock()
        client.get.side_effect = PardotForbiddenError("403 Forbidden")

        stream = Prospects(client=client, config={"start_date": "2020-01-01T00:00:00Z"}, state={}, emit=False)
        self.assertFalse(stream.check_access())

    def test_check_access_child_stream_always_true(self):
        """Test check_access always returns True for child streams without making API calls."""
        from tap_pardot.streams import Visits

        client = MagicMock()
        client.get.side_effect = PardotForbiddenError("403 Forbidden")
        stream = Visits(client=client, config={"start_date": "2020-01-01T00:00:00Z"}, state={}, emit=False)
        # Child streams always return True — access governed by parent
        self.assertTrue(stream.check_access())
        client.get.assert_not_called()

    def test_check_access_child_stream_does_not_call_api(self):
        """Test check_access for child streams skips API call entirely."""
        from tap_pardot.streams import ListMemberships

        client = MagicMock()
        stream = ListMemberships(client=client, config={"start_date": "2020-01-01T00:00:00Z"}, state={}, emit=False)
        self.assertTrue(stream.check_access())
        client.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
