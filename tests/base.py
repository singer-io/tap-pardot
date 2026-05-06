import os

from tap_tester import menagerie, runner
from tap_tester.logger import LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class PardotBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. Shared methods used in tap-tester tests.
    """

    start_date = "2020-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-pardot"

    @staticmethod
    def get_type():
        """The expected url route ending."""
        return "platform.pardot"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "campaigns": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"id"},
                cls.OBEYS_START_DATE: True,
            },
            "email_clicks": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"id"},
                cls.OBEYS_START_DATE: True,
            },
            "list_memberships": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"id", "updated_at", "list_id"},
                cls.OBEYS_START_DATE: True,
            },
            "lists": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
            },
            "opportunities": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"id", "updated_at"},
                cls.OBEYS_START_DATE: True,
            },
            "prospect_accounts": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
            },
            "prospects": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
            },
            "users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"id", "updated_at"},
                cls.OBEYS_START_DATE: True,
            },
            "visitor_activities": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"id"},
                cls.OBEYS_START_DATE: True,
            },
            "visitors": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
            },
            "visits": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"id", "updated_at"},
                cls.OBEYS_START_DATE: True,
            },
        }

    @classmethod
    def expected_stream_names(cls):
        """Return all expected stream names."""
        return set(cls.expected_metadata().keys())

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "refresh_token": "TAP_PARDOT_REFRESH_TOKEN",
            "client_id": "TAP_PARDOT_CLIENT_ID",
            "client_secret": "TAP_PARDOT_CLIENT_SECRET",
        }
        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return {
            "start_date": self.start_date,
            "pardot_business_unit_id": os.getenv("TAP_PARDOT_BUSINESS_UNIT_ID"),
        }
