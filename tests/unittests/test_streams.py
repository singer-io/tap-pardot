import unittest
from unittest.mock import MagicMock, patch

from tap_pardot.streams import (
    EmailClicks,
    VisitorActivities,
    ProspectAccounts,
    Prospects,
    Opportunities,
    Users,
    Visitors,
    Visits,
    Lists,
    ListMemberships,
    Campaigns,
    STREAM_OBJECTS,
)


class TestStreamObjects(unittest.TestCase):
    """Test STREAM_OBJECTS dictionary is correctly populated."""

    def test_all_streams_present(self):
        """Test all expected streams are present in STREAM_OBJECTS."""
        expected_streams = [
            "email_clicks",
            "visitor_activities",
            "prospect_accounts",
            "prospects",
            "opportunities",
            "users",
            "visitors",
            "visits",
            "lists",
            "list_memberships",
            "campaigns",
        ]
        for stream_name in expected_streams:
            self.assertIn(stream_name, STREAM_OBJECTS)

    def test_stream_classes_map_correctly(self):
        """Test streams map to correct classes."""
        self.assertEqual(STREAM_OBJECTS["email_clicks"], EmailClicks)
        self.assertEqual(STREAM_OBJECTS["visitor_activities"], VisitorActivities)
        self.assertEqual(STREAM_OBJECTS["prospect_accounts"], ProspectAccounts)
        self.assertEqual(STREAM_OBJECTS["prospects"], Prospects)
        self.assertEqual(STREAM_OBJECTS["opportunities"], Opportunities)
        self.assertEqual(STREAM_OBJECTS["users"], Users)
        self.assertEqual(STREAM_OBJECTS["visitors"], Visitors)
        self.assertEqual(STREAM_OBJECTS["visits"], Visits)
        self.assertEqual(STREAM_OBJECTS["lists"], Lists)
        self.assertEqual(STREAM_OBJECTS["list_memberships"], ListMemberships)
        self.assertEqual(STREAM_OBJECTS["campaigns"], Campaigns)


class TestStreamBase(unittest.TestCase):
    """Test base Stream class."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_get_default_start(self):
        """Test get_default_start returns config start_date."""
        stream = Prospects(self.client, self.config, self.state)
        self.assertEqual(stream.get_default_start(), "2020-01-01T00:00:00Z")

    @patch("singer.write_state")
    def test_get_bookmark_no_state(self, mock_write_state):
        """Test get_bookmark returns default start when no bookmark exists."""
        stream = Prospects(self.client, self.config, self.state)
        self.assertEqual(stream.get_bookmark(), "2020-01-01T00:00:00Z")

    @patch("singer.write_state")
    def test_get_bookmark_with_state(self, mock_write_state):
        """Test get_bookmark returns existing bookmark value."""
        self.state = {
            "bookmarks": {"prospects": {"updated_at": "2021-06-15T00:00:00Z"}}
        }
        stream = Prospects(self.client, self.config, self.state)
        self.assertEqual(stream.get_bookmark(), "2021-06-15T00:00:00Z")

    @patch("singer.write_state")
    def test_update_bookmark(self, mock_write_state):
        """Test update_bookmark writes bookmark to state."""
        stream = Prospects(self.client, self.config, self.state)
        stream.update_bookmark("2021-07-01T00:00:00Z")

        self.assertEqual(
            self.state["bookmarks"]["prospects"]["updated_at"],
            "2021-07-01T00:00:00Z",
        )
        mock_write_state.assert_called_once_with(self.state)

    @patch("singer.write_state")
    def test_update_bookmark_no_emit(self, mock_write_state):
        """Test update_bookmark doesn't emit state when emit=False."""
        stream = Prospects(self.client, self.config, self.state, emit=False)
        stream.update_bookmark("2021-07-01T00:00:00Z")

        mock_write_state.assert_not_called()

    def test_get_records_empty_result(self):
        """Test get_records returns empty list when result is None."""
        self.client.get.return_value = {"result": None}
        stream = Prospects(self.client, self.config, self.state)
        records = stream.get_records()
        self.assertEqual(records, [])

    def test_get_records_zero_total(self):
        """Test get_records returns empty list when total_results is 0."""
        self.client.get.return_value = {"result": {"total_results": 0}}
        stream = Prospects(self.client, self.config, self.state)
        records = stream.get_records()
        self.assertEqual(records, [])

    def test_get_records_single_dict_result(self):
        """Test get_records wraps dict result in a list."""
        self.client.get.return_value = {
            "result": {"total_results": 1, "prospect": {"id": 1, "email": "test@example.com"}}
        }
        stream = Prospects(self.client, self.config, self.state)
        records = stream.get_records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 1)

    def test_get_records_list_result(self):
        """Test get_records returns list of records."""
        self.client.get.return_value = {
            "result": {
                "total_results": 2,
                "prospect": [{"id": 1}, {"id": 2}],
            }
        }
        stream = Prospects(self.client, self.config, self.state)
        records = stream.get_records()
        self.assertEqual(len(records), 2)

    def test_check_order_valid(self):
        """Test check_order passes with ascending values."""
        stream = Prospects(self.client, self.config, self.state)
        stream.check_order("2021-01-01T00:00:00Z")
        stream.check_order("2021-01-02T00:00:00Z")
        stream.check_order("2021-01-03T00:00:00Z")

    def test_check_order_invalid_raises_exception(self):
        """Test check_order raises on out-of-order values."""
        stream = Prospects(self.client, self.config, self.state)
        stream.check_order("2021-01-03T00:00:00Z")
        with self.assertRaises(Exception) as ctx:
            stream.check_order("2021-01-01T00:00:00Z")
        self.assertIn("out of order", str(ctx.exception).lower())


class TestIdReplicationStream(unittest.TestCase):
    """Test IdReplicationStream class."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_default_start_is_zero(self):
        """Test IdReplicationStream default start is 0."""
        stream = EmailClicks(self.client, self.config, self.state)
        self.assertEqual(stream.get_default_start(), 0)

    def test_replication_keys(self):
        """Test IdReplicationStream uses id as replication key."""
        stream = EmailClicks(self.client, self.config, self.state)
        self.assertEqual(stream.replication_keys, ["id"])

    def test_replication_method(self):
        """Test IdReplicationStream uses INCREMENTAL method."""
        stream = EmailClicks(self.client, self.config, self.state)
        self.assertEqual(stream.replication_method, "INCREMENTAL")

    def test_get_params(self):
        """Test IdReplicationStream get_params returns correct parameters."""
        stream = EmailClicks(self.client, self.config, self.state)
        params = stream.get_params()

        self.assertEqual(params["created_after"], "2020-01-01T00:00:00Z")
        self.assertEqual(params["id_greater_than"], 0)
        self.assertEqual(params["sort_by"], "id")
        self.assertEqual(params["sort_order"], "ascending")

    def test_get_params_with_bookmark(self):
        """Test get_params uses bookmark when available."""
        self.state = {"bookmarks": {"email_clicks": {"id": 100}}}
        stream = EmailClicks(self.client, self.config, self.state)
        params = stream.get_params()

        self.assertEqual(params["id_greater_than"], 100)


class TestUpdatedAtReplicationStream(unittest.TestCase):
    """Test UpdatedAtReplicationStream class."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_replication_keys(self):
        """Test uses updated_at as replication key."""
        stream = Prospects(self.client, self.config, self.state)
        self.assertEqual(stream.replication_keys, ["updated_at"])

    def test_get_params(self):
        """Test get_params returns correct parameters."""
        stream = Prospects(self.client, self.config, self.state)
        params = stream.get_params()

        self.assertEqual(params["updated_after"], "2020-01-01T00:00:00Z")
        self.assertEqual(params["sort_by"], "updated_at")
        self.assertEqual(params["sort_order"], "ascending")

    def test_get_params_with_bookmark(self):
        """Test get_params uses existing bookmark."""
        self.state = {"bookmarks": {"prospects": {"updated_at": "2021-06-01T00:00:00Z"}}}
        stream = Prospects(self.client, self.config, self.state)
        params = stream.get_params()

        self.assertEqual(params["updated_after"], "2021-06-01T00:00:00Z")

    @patch("singer.write_state")
    def test_sync_page_yields_records(self, mock_write_state):
        """Test sync_page yields records and updates bookmark."""
        self.client.get.return_value = {
            "result": {
                "total_results": 2,
                "prospect": [
                    {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
                    {"id": 2, "updated_at": "2021-01-02T00:00:00Z"},
                ],
            }
        }
        stream = Prospects(self.client, self.config, self.state)
        records = list(stream.sync_page())

        self.assertEqual(len(records), 2)
        self.assertEqual(
            self.state["bookmarks"]["prospects"]["updated_at"],
            "2021-01-02T00:00:00Z",
        )


class TestNoUpdatedAtSortingStream(unittest.TestCase):
    """Test NoUpdatedAtSortingStream class."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_replication_keys(self):
        """Test uses both id and updated_at as replication keys."""
        stream = Opportunities(self.client, self.config, self.state)
        self.assertEqual(stream.replication_keys, ["id", "updated_at"])

    def test_get_params(self):
        """Test get_params returns correct parameters."""
        stream = Opportunities(self.client, self.config, self.state)
        params = stream.get_params()

        self.assertEqual(params["created_after"], "2020-01-01T00:00:00Z")
        self.assertEqual(params["id_greater_than"], 0)
        self.assertEqual(params["sort_by"], "id")
        self.assertEqual(params["sort_order"], "ascending")

    @patch("singer.write_state")
    def test_post_sync_clears_id_bookmark(self, mock_write_state):
        """Test post_sync clears id bookmark and updates updated_at."""
        stream = Opportunities(self.client, self.config, self.state)
        stream.max_updated_at = "2021-06-01T00:00:00Z"
        stream.post_sync()

        self.assertEqual(
            self.state["bookmarks"]["opportunities"]["updated_at"],
            "2021-06-01T00:00:00Z",
        )

    @patch("singer.write_state")
    def test_sync_page_skips_old_records(self, mock_write_state):
        """Test sync_page skips records older than last_updated_at."""
        self.state = {
            "bookmarks": {"opportunities": {"updated_at": "2021-06-01T00:00:00Z"}}
        }
        self.client.get.return_value = {
            "result": {
                "total_results": 3,
                "opportunity": [
                    {"id": 1, "updated_at": "2021-05-01T00:00:00Z"},
                    {"id": 2, "updated_at": "2021-06-01T00:00:00Z"},
                    {"id": 3, "updated_at": "2021-07-01T00:00:00Z"},
                ],
            }
        }
        stream = Opportunities(self.client, self.config, self.state)
        records = list(stream.sync_page())

        # Only record with updated_at > last_updated_at should be yielded
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 3)


class TestUpdatedAtSortByIdReplicationStream(unittest.TestCase):
    """Test UpdatedAtSortByIdReplicationStream class."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_replication_keys(self):
        """Test uses id as replication key."""
        stream = Campaigns(self.client, self.config, self.state)
        self.assertEqual(stream.replication_keys, ["id"])

    @patch("singer.write_state")
    @patch("singer.utils.now")
    @patch("singer.utils.strftime")
    def test_pre_sync_sets_start_time(self, mock_strftime, mock_now, mock_write_state):
        """Test pre_sync stores sync_start_time in bookmark."""
        mock_strftime.return_value = "2021-07-01T00:00:00Z"
        stream = Campaigns(self.client, self.config, self.state)
        stream.pre_sync()

        self.assertEqual(stream.start_time, "2021-07-01T00:00:00Z")
        self.assertEqual(
            self.state["bookmarks"]["campaigns"]["sync_start_time"],
            "2021-07-01T00:00:00Z",
        )

    @patch("singer.write_state")
    @patch("singer.utils.now")
    @patch("singer.utils.strftime")
    def test_pre_sync_resumes_existing_start_time(self, mock_strftime, mock_now, mock_write_state):
        """Test pre_sync uses existing sync_start_time."""
        self.state = {
            "bookmarks": {"campaigns": {"sync_start_time": "2021-06-15T00:00:00Z"}}
        }
        stream = Campaigns(self.client, self.config, self.state)
        stream.pre_sync()

        self.assertEqual(stream.start_time, "2021-06-15T00:00:00Z")
        # Should not create a new timestamp
        mock_strftime.assert_not_called()

    @patch("singer.write_state")
    @patch("singer.utils.now")
    @patch("singer.utils.strftime")
    def test_post_sync_clears_bookmarks(self, mock_strftime, mock_now, mock_write_state):
        """Test post_sync clears sync_start_time and id, sets last_updated."""
        mock_strftime.return_value = "2021-07-01T00:00:00Z"
        stream = Campaigns(self.client, self.config, self.state)
        stream.pre_sync()
        stream.post_sync()

        self.assertNotIn("sync_start_time", self.state["bookmarks"].get("campaigns", {}))
        self.assertNotIn("id", self.state["bookmarks"].get("campaigns", {}))
        self.assertEqual(
            self.state["bookmarks"]["campaigns"]["last_updated"],
            "2021-07-01T00:00:00Z",
        )

    @patch("singer.write_state")
    def test_get_params(self, mock_write_state):
        """Test get_params returns correct parameters."""
        stream = Campaigns(self.client, self.config, self.state)
        params = stream.get_params()

        self.assertEqual(params["id_greater_than"], 0)
        self.assertEqual(params["sort_by"], "id")
        self.assertEqual(params["sort_order"], "ascending")

    @patch("singer.write_state")
    def test_sync_page_yields_records_and_updates_id(self, mock_write_state):
        """Test sync_page yields records and updates id bookmark."""
        self.client.get.return_value = {
            "result": {
                "total_results": 2,
                "campaign": [{"id": 10}, {"id": 20}],
            }
        }
        stream = Campaigns(self.client, self.config, self.state)
        records = list(stream.sync_page())

        self.assertEqual(len(records), 2)
        self.assertEqual(self.state["bookmarks"]["campaigns"]["id"], 20)


class TestVisitorsStream(unittest.TestCase):
    """Test Visitors stream specifics."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_get_params_includes_only_identified(self):
        """Test Visitors get_params includes only_identified=false."""
        stream = Visitors(self.client, self.config, self.state)
        params = stream.get_params()

        self.assertEqual(params["only_identified"], "false")
        self.assertEqual(params["sort_by"], "updated_at")
        self.assertEqual(params["sort_order"], "ascending")


class TestStreamProperties(unittest.TestCase):
    """Test stream class properties are correctly defined."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_email_clicks_properties(self):
        """Test EmailClicks stream properties."""
        self.assertEqual(EmailClicks.stream_name, "email_clicks")
        self.assertEqual(EmailClicks.data_key, "emailClick")
        self.assertEqual(EmailClicks.endpoint, "emailClick")
        self.assertFalse(EmailClicks.is_dynamic)

    def test_visitor_activities_properties(self):
        """Test VisitorActivities stream properties."""
        self.assertEqual(VisitorActivities.stream_name, "visitor_activities")
        self.assertEqual(VisitorActivities.data_key, "visitor_activity")
        self.assertEqual(VisitorActivities.endpoint, "visitorActivity")
        self.assertFalse(VisitorActivities.is_dynamic)

    def test_prospect_accounts_properties(self):
        """Test ProspectAccounts stream properties."""
        self.assertEqual(ProspectAccounts.stream_name, "prospect_accounts")
        self.assertEqual(ProspectAccounts.data_key, "prospectAccount")
        self.assertEqual(ProspectAccounts.endpoint, "prospectAccount")
        self.assertTrue(ProspectAccounts.is_dynamic)

    def test_prospects_properties(self):
        """Test Prospects stream properties."""
        self.assertEqual(Prospects.stream_name, "prospects")
        self.assertEqual(Prospects.data_key, "prospect")
        self.assertEqual(Prospects.endpoint, "prospect")
        self.assertFalse(Prospects.is_dynamic)

    def test_opportunities_properties(self):
        """Test Opportunities stream properties."""
        self.assertEqual(Opportunities.stream_name, "opportunities")
        self.assertEqual(Opportunities.data_key, "opportunity")
        self.assertEqual(Opportunities.endpoint, "opportunity")
        self.assertFalse(Opportunities.is_dynamic)

    def test_users_properties(self):
        """Test Users stream properties."""
        self.assertEqual(Users.stream_name, "users")
        self.assertEqual(Users.data_key, "user")
        self.assertEqual(Users.endpoint, "user")
        self.assertFalse(Users.is_dynamic)

    def test_visitors_properties(self):
        """Test Visitors stream properties."""
        self.assertEqual(Visitors.stream_name, "visitors")
        self.assertEqual(Visitors.data_key, "visitor")
        self.assertEqual(Visitors.endpoint, "visitor")
        self.assertFalse(Visitors.is_dynamic)

    def test_visits_properties(self):
        """Test Visits stream properties."""
        self.assertEqual(Visits.stream_name, "visits")
        self.assertEqual(Visits.data_key, "visit")
        self.assertEqual(Visits.endpoint, "visit")
        self.assertFalse(Visits.is_dynamic)
        self.assertEqual(Visits.parent_class, Visitors)
        self.assertEqual(Visits.parent_id_param, "visitor_ids")

    def test_lists_properties(self):
        """Test Lists stream properties."""
        self.assertEqual(Lists.stream_name, "lists")
        self.assertEqual(Lists.data_key, "list")
        self.assertEqual(Lists.endpoint, "list")
        self.assertFalse(Lists.is_dynamic)

    def test_list_memberships_properties(self):
        """Test ListMemberships stream properties."""
        self.assertEqual(ListMemberships.stream_name, "list_memberships")
        self.assertEqual(ListMemberships.data_key, "list_membership")
        self.assertEqual(ListMemberships.endpoint, "listMembership")
        self.assertFalse(ListMemberships.is_dynamic)
        self.assertEqual(ListMemberships.parent_class, Lists)
        self.assertEqual(ListMemberships.parent_id_param, "list_id")

    def test_campaigns_properties(self):
        """Test Campaigns stream properties."""
        self.assertEqual(Campaigns.stream_name, "campaigns")
        self.assertEqual(Campaigns.data_key, "campaign")
        self.assertEqual(Campaigns.endpoint, "campaign")
        self.assertFalse(Campaigns.is_dynamic)


class TestVisitsStream(unittest.TestCase):
    """Test Visits stream specifics."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    def test_fix_page_views_dict_to_list(self):
        """Test fix_page_views converts single dict to list."""
        stream = Visits(self.client, self.config, self.state)
        record = {
            "visitor_page_views": {
                "visitor_page_view": {"url": "http://example.com", "id": 1}
            }
        }
        stream.fix_page_views(record)

        self.assertIsInstance(
            record["visitor_page_views"]["visitor_page_view"], list
        )
        self.assertEqual(len(record["visitor_page_views"]["visitor_page_view"]), 1)

    def test_fix_page_views_list_unchanged(self):
        """Test fix_page_views doesn't change existing list."""
        stream = Visits(self.client, self.config, self.state)
        record = {
            "visitor_page_views": {
                "visitor_page_view": [
                    {"url": "http://example.com", "id": 1},
                    {"url": "http://example2.com", "id": 2},
                ]
            }
        }
        stream.fix_page_views(record)

        self.assertEqual(len(record["visitor_page_views"]["visitor_page_view"]), 2)


class TestComplexBookmarkStream(unittest.TestCase):
    """Test ComplexBookmarkStream class."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    @patch("singer.write_state")
    def test_get_default_start_updated_at(self, mock_write_state):
        """Test get_default_start returns config start_date for updated_at."""
        stream = Opportunities(self.client, self.config, self.state)
        self.assertEqual(
            stream.get_default_start("updated_at"), "2020-01-01T00:00:00Z"
        )

    @patch("singer.write_state")
    def test_get_default_start_id(self, mock_write_state):
        """Test get_default_start returns 0 for id."""
        stream = Opportunities(self.client, self.config, self.state)
        self.assertEqual(stream.get_default_start("id"), 0)

    @patch("singer.write_state")
    def test_get_default_start_offset(self, mock_write_state):
        """Test get_default_start returns 0 for offset."""
        stream = Opportunities(self.client, self.config, self.state)
        self.assertEqual(stream.get_default_start("offset"), 0)

    @patch("singer.write_state")
    def test_clear_bookmark(self, mock_write_state):
        """Test clear_bookmark removes bookmark key."""
        self.state = {"bookmarks": {"opportunities": {"id": 50, "updated_at": "2021-01-01T00:00:00Z"}}}
        stream = Opportunities(self.client, self.config, self.state)
        stream.clear_bookmark("id")

        self.assertNotIn("id", self.state["bookmarks"]["opportunities"])

    @patch("singer.write_state")
    def test_update_bookmark_complex(self, mock_write_state):
        """Test update_bookmark for complex stream sets correct key."""
        stream = Opportunities(self.client, self.config, self.state)
        stream.update_bookmark("id", 42)

        self.assertEqual(self.state["bookmarks"]["opportunities"]["id"], 42)


class TestFullSync(unittest.TestCase):
    """Test full sync workflow for a stream."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = MagicMock()
        self.config = {"start_date": "2020-01-01T00:00:00Z"}
        self.state = {"bookmarks": {}}

    @patch("singer.write_state")
    def test_sync_stops_on_empty_result(self, mock_write_state):
        """Test sync stops when no more records are returned."""
        self.client.get.return_value = {"result": None}
        stream = Prospects(self.client, self.config, self.state)
        records = list(stream.sync())

        self.assertEqual(len(records), 0)

    @patch("singer.write_state")
    def test_sync_iterates_pages(self, mock_write_state):
        """Test sync iterates through pages until empty."""
        # First call returns records, second call returns none
        self.client.get.side_effect = [
            {
                "result": {
                    "total_results": 2,
                    "prospect": [
                        {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
                        {"id": 2, "updated_at": "2021-01-02T00:00:00Z"},
                    ],
                }
            },
            {"result": None},
        ]
        stream = Prospects(self.client, self.config, self.state)
        records = list(stream.sync())

        self.assertEqual(len(records), 2)


if __name__ == "__main__":
    unittest.main()
